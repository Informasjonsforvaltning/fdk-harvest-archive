package no.fdk.harvestarchive.archive

import no.fdk.harvestarchive.metrics.ArchiveMetrics
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.time.measureTimedValue

/**
 * Periodically checks archive directories and creates zip files when their total
 * size exceeds the configured threshold.
 */
@Component
class ArchiveZipper(
    private val archiveDirectories: ArchiveDirectories,
    @param:Value("\${app.archive.zip-threshold-bytes}") private val zipThresholdBytes: Long,
    @param:Value("\${app.archive.zip-threshold-file-count}") private val zipThresholdFileCount: Int,
    @param:Value("\${app.archive.zip-max-file-count}") private val zipMaxFileCount: Int,
    private val archiveMetrics: ArchiveMetrics,
) {
    @Scheduled(fixedDelayString = "\${app.archive.zip-check-interval-ms}")
    fun checkAndZipAll() {
        archiveDirectories.forEach { archiveType, dir ->
            try {
                val dirPath = Paths.get(dir)
                if (Files.exists(dirPath)) {
                    zipIfOverThreshold(archiveType, dirPath)
                }
            } catch (e: Exception) {
                LOGGER.error("Failed to check archive dir {} for {}", dir, archiveType, e)
            }
        }
    }

    fun zipIfOverThreshold(
        archiveType: ArchiveType,
        dirPath: Path,
        thresholdBytes: Long = zipThresholdBytes,
        thresholdFileCount: Int = zipThresholdFileCount,
        maxFileCount: Int = zipMaxFileCount,
    ) {
        val files = listRegularFiles(dirPath)

        val totalSize = files.sumOf { Files.size(it) }
        val fileCount = files.size.toLong()

        archiveMetrics.updateDirectorySnapshot(archiveType, totalSize, fileCount)

        if (totalSize < thresholdBytes && fileCount < thresholdFileCount) return

        val parent = dirPath.parent ?: return
        val filesToArchive = files.take(maxFileCount)
        if (filesToArchive.isEmpty()) return

        val timed =
            measureTimedValue {
                try {
                    val zipFileName = "${dirPath.fileName}-${System.currentTimeMillis()}.zip"
                    val zipPath = parent.resolve(zipFileName)
                    val tmpPath = parent.resolve("$zipFileName.tmp")

                    try {
                        ZipOutputStream(Files.newOutputStream(tmpPath)).use { zipOut ->
                            filesToArchive.forEach { file ->
                                val entryName = dirPath.relativize(file).toString()
                                zipOut.putNextEntry(ZipEntry(entryName))
                                Files.newInputStream(file).use { input ->
                                    input.copyTo(zipOut)
                                }
                                zipOut.closeEntry()
                            }
                        }
                        Files.move(tmpPath, zipPath, StandardCopyOption.ATOMIC_MOVE)
                    } catch (e: Exception) {
                        Files.deleteIfExists(tmpPath)
                        throw e
                    }

                    filesToArchive.forEach { file ->
                        try {
                            Files.deleteIfExists(file)
                        } catch (ex: Exception) {
                            LOGGER.warn("Failed to delete archived file {}", file, ex)
                        }
                    }

                    ZipAttempt.Success(zipPath)
                } catch (e: Exception) {
                    LOGGER.error("Failed to create zip for directory {}", dirPath, e)
                    ZipAttempt.Failure
                }
            }
        try {
            when (val result = timed.value) {
                is ZipAttempt.Success -> {
                    val zipBytes = Files.size(result.zipPath)
                    LOGGER.debug(
                        "Created zip archive {} for directory {} ({} bytes, {} files).",
                        result.zipPath.fileName,
                        dirPath,
                        zipBytes,
                        filesToArchive.size,
                    )
                    archiveMetrics.recordZip(
                        archiveType,
                        filesToArchive.size,
                        zipBytes,
                        timed.duration,
                    )
                }

                ZipAttempt.Failure -> archiveMetrics.recordZipError(archiveType, timed.duration)
            }
        } finally {
            refreshDirectorySnapshot(archiveType, dirPath)
        }
    }

    private sealed class ZipAttempt {
        data class Success(val zipPath: Path) : ZipAttempt()

        data object Failure : ZipAttempt()
    }

    private fun refreshDirectorySnapshot(archiveType: ArchiveType, dirPath: Path) {
        val remaining = if (Files.exists(dirPath)) listRegularFiles(dirPath) else emptyList()
        archiveMetrics.updateDirectorySnapshot(
            archiveType,
            remaining.sumOf { Files.size(it) },
            remaining.size.toLong(),
        )
    }

    private fun listRegularFiles(dirPath: Path): List<Path> = Files.walk(dirPath).use { paths ->
        paths.filter { Files.isRegularFile(it) }.toList()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ArchiveZipper::class.java)
    }
}
