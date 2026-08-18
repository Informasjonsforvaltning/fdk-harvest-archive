package no.fdk.harvestarchive.archive

import no.fdk.harvestarchive.metrics.ArchiveMetrics
import no.fdk.harvestarchive.metrics.ArchiveMetrics.ZipStatus
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.time.measureTimedValue

/**
 * Periodically checks archive directories and creates zip files when their total
 * size exceeds the configured threshold.
 */
@Component
class ArchiveZipper(
    @param:Value("\${app.archive.dataset-dir}") private val datasetDir: String,
    @param:Value("\${app.archive.concept-dir}") private val conceptDir: String,
    @param:Value("\${app.archive.data-service-dir}") private val dataServiceDir: String,
    @param:Value("\${app.archive.information-model-dir}") private val informationModelDir: String,
    @param:Value("\${app.archive.event-dir}") private val eventDir: String,
    @param:Value("\${app.archive.service-dir}") private val serviceDir: String,
    @param:Value("\${app.archive.zip-threshold-bytes}") private val zipThresholdBytes: Long,
    @param:Value("\${app.archive.zip-max-file-count}") private val zipMaxFileCount: Int,
) {
    private val archiveTypeToDir: Map<ArchiveType, String> =
        mapOf(
            ArchiveType.DATASET to datasetDir,
            ArchiveType.CONCEPT to conceptDir,
            ArchiveType.DATA_SERVICE to dataServiceDir,
            ArchiveType.INFORMATION_MODEL to informationModelDir,
            ArchiveType.EVENT to eventDir,
            ArchiveType.SERVICE to serviceDir,
        )

    @Scheduled(fixedDelayString = "\${app.archive.zip-check-interval-ms}")
    fun checkAndZipAll() {
        archiveTypeToDir.forEach { (archiveType, dir) ->
            val dirPath = Paths.get(dir)
            if (Files.exists(dirPath)) {
                zipIfOverThreshold(archiveType, dirPath)
            }
        }
    }

    fun zipIfOverThreshold(
        archiveType: ArchiveType,
        dirPath: Path,
        thresholdBytes: Long = zipThresholdBytes,
        maxFileCount: Int = zipMaxFileCount,
    ) {
        val files =
            Files
                .walk(dirPath)
                .filter { Files.isRegularFile(it) }
                .toList()

        val totalSize = files.sumOf { Files.size(it) }
        val fileCount = files.size.toLong()

        ArchiveMetrics.updateDirectorySnapshot(archiveType, totalSize, fileCount)

        if (totalSize < thresholdBytes) return

        val parent = dirPath.parent ?: return
        val filesToArchive = files.take(maxFileCount)
        if (filesToArchive.isEmpty()) return

        try {
            val timed =
                measureTimedValue {
                    val zipFileName = "${dirPath.fileName}-${System.currentTimeMillis()}.zip"
                    val zipPath = parent.resolve(zipFileName)

                    ZipOutputStream(Files.newOutputStream(zipPath)).use { zipOut ->
                        filesToArchive.forEach { file ->
                            val entryName = dirPath.relativize(file).toString()
                            zipOut.putNextEntry(ZipEntry(entryName))
                            Files.newInputStream(file).use { input ->
                                input.copyTo(zipOut)
                            }
                            zipOut.closeEntry()
                        }
                    }

                    filesToArchive.forEach { file ->
                        try {
                            Files.deleteIfExists(file)
                        } catch (ex: Exception) {
                            LOGGER.warn("Failed to delete archived file {}", file, ex)
                        }
                    }

                    val zipBytes = Files.size(zipPath)

                    LOGGER.debug(
                        "Created zip archive {} for directory {} ({} bytes, {} files).",
                        zipPath.fileName,
                        dirPath,
                        zipBytes,
                        filesToArchive.size,
                    )

                    zipBytes
                }
            ArchiveMetrics.recordZip(
                archiveType,
                ZipStatus.SUCCESS,
                filesToArchive.size,
                timed.value,
                timed.duration,
            )
        } catch (e: Exception) {
            ArchiveMetrics.recordZip(
                archiveType,
                ZipStatus.ERROR,
                0,
                0,
                kotlin.time.Duration.ZERO,
            )
            LOGGER.error("Failed to create zip for directory {}", dirPath, e)
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ArchiveZipper::class.java)
    }
}
