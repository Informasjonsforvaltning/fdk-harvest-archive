package no.fdk.harvestarchive.archive

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.fdk.concept.ConceptEvent
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataset.DatasetEvent
import no.fdk.event.EventEvent
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.service.ServiceEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

/**
 * Persists harvest events as JSON files under type-specific directories.
 * Each event is written as `{timestamp}_{fdkId}.json` with type, harvestRunId, uri, fdkId, graph, and timestamp.
 */
@Service
class EventArchiveService(
    @param:Value($$"${app.archive.dataset-dir}") private val datasetDir: String,
    @param:Value($$"${app.archive.concept-dir}") private val conceptDir: String,
    @param:Value($$"${app.archive.data-service-dir}") private val dataServiceDir: String,
    @param:Value($$"${app.archive.information-model-dir}") private val informationModelDir: String,
    @param:Value($$"${app.archive.event-dir}") private val eventDir: String,
    @param:Value($$"${app.archive.service-dir}") private val serviceDir: String,
) {
    private val objectMapper = jacksonObjectMapper()
    private val zipThresholdBytes: Long = ZIP_THRESHOLD_BYTES

    private val archiveTypeToDir: Map<ArchiveType, String> =
        mapOf(
            ArchiveType.DATASET to datasetDir,
            ArchiveType.CONCEPT to conceptDir,
            ArchiveType.DATA_SERVICE to dataServiceDir,
            ArchiveType.INFORMATION_MODEL to informationModelDir,
            ArchiveType.EVENT to eventDir,
            ArchiveType.SERVICE to serviceDir,
        )

    /**
     * Saves a generic (map) payload to the directory for the given topic, only if the event type is HARVESTED or REMOVED for that topic.
     */
    fun saveGenericForTopic(topic: String, payload: Map<String, Any?>) {
        val archiveType = ArchiveType.fromTopic(topic) ?: return
        val eventType = payload["type"]?.toString() ?: return
        if (!archiveType.allowsEventType(eventType)) {
            LOGGER.debug("Skipping generic event with type {} for topic {}", eventType, topic)
            return
        }
        val filename = "${payload["timestamp"]}_${payload["fdkId"]}.json"
        saveAsFile(archiveTypeToDir.getValue(archiveType), filename, payload)
        LOGGER.debug("Generic event saved to {}", filename)
    }

    fun saveDataset(event: DatasetEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload =
            mapOf(
                "type" to event.type.name,
                "harvestRunId" to event.harvestRunId?.toString(),
                "uri" to event.uri?.toString(),
                "fdkId" to event.fdkId.toString(),
                "graph" to event.graph.toString(),
                "catalogGraph" to event.catalogGraph?.toString(),
                "timestamp" to event.timestamp,
            )
        saveAsFile(archiveTypeToDir.getValue(ArchiveType.DATASET), filename, payload)
        LOGGER.debug("Dataset event saved to {}", filename)
    }

    fun saveConcept(event: ConceptEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload =
            mapOf(
                "type" to event.type.name,
                "harvestRunId" to event.harvestRunId?.toString(),
                "uri" to event.uri?.toString(),
                "fdkId" to event.fdkId.toString(),
                "graph" to event.graph.toString(),
                "catalogGraph" to event.catalogGraph?.toString(),
                "timestamp" to event.timestamp,
            )
        saveAsFile(archiveTypeToDir.getValue(ArchiveType.CONCEPT), filename, payload)
        LOGGER.debug("Concept event saved to {}", filename)
    }

    fun saveDataService(event: DataServiceEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload =
            mapOf(
                "type" to event.type.name,
                "harvestRunId" to event.harvestRunId?.toString(),
                "uri" to event.uri?.toString(),
                "fdkId" to event.fdkId.toString(),
                "graph" to event.graph.toString(),
                "catalogGraph" to event.catalogGraph?.toString(),
                "timestamp" to event.timestamp,
            )
        saveAsFile(archiveTypeToDir.getValue(ArchiveType.DATA_SERVICE), filename, payload)
        LOGGER.debug("DataService event saved to {}", filename)
    }

    fun saveInformationModel(event: InformationModelEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload =
            mapOf(
                "type" to event.type.name,
                "harvestRunId" to event.harvestRunId?.toString(),
                "uri" to event.uri?.toString(),
                "fdkId" to event.fdkId.toString(),
                "graph" to event.graph.toString(),
                "catalogGraph" to event.catalogGraph?.toString(),
                "timestamp" to event.timestamp,
            )
        saveAsFile(archiveTypeToDir.getValue(ArchiveType.INFORMATION_MODEL), filename, payload)
        LOGGER.debug("InformationModel event saved to {}", filename)
    }

    fun saveEvent(event: EventEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload =
            mapOf(
                "type" to event.type.name,
                "harvestRunId" to event.harvestRunId?.toString(),
                "uri" to event.uri?.toString(),
                "fdkId" to event.fdkId.toString(),
                "graph" to event.graph.toString(),
                "catalogGraph" to event.catalogGraph?.toString(),
                "timestamp" to event.timestamp,
            )
        saveAsFile(archiveTypeToDir.getValue(ArchiveType.EVENT), filename, payload)
        LOGGER.debug("Event event saved to {}", filename)
    }

    fun saveService(event: ServiceEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload =
            mapOf(
                "type" to event.type.name,
                "harvestRunId" to event.harvestRunId?.toString(),
                "uri" to event.uri?.toString(),
                "fdkId" to event.fdkId.toString(),
                "graph" to event.graph.toString(),
                "catalogGraph" to event.catalogGraph?.toString(),
                "timestamp" to event.timestamp,
            )
        saveAsFile(archiveTypeToDir.getValue(ArchiveType.SERVICE), filename, payload)
        LOGGER.debug("Service event saved to {}", filename)
    }

    private fun saveAsFile(dir: String, filename: String, payload: Map<String, Any?>) {
        val dirPath = Paths.get(dir)
        Files.createDirectories(dirPath)
        val path = dirPath.resolve(filename)
        objectMapper.writeValue(path.toFile(), payload)
    }

    /**
     * Periodically checks each archive directory size and creates a zip (and deletes source files) when over threshold.
     */
    @Scheduled(fixedDelayString = $$"${app.archive.zip-check-interval-ms}")
    fun checkArchiveDirsAndZipIfOverThreshold() {
        archiveTypeToDir.values
            .map { Paths.get(it) }
            .filter { Files.exists(it) }
            .forEach { createZipIfLargerThanThreshold(it) }
    }

    private fun createZipIfLargerThanThreshold(
        dirPath: Path,
        thresholdBytes: Long = zipThresholdBytes,
        maxFileCount: Int = ZIP_MAX_FILE_COUNT,
    ) {
        val totalSize =
            Files
                .walk(dirPath)
                .filter { Files.isRegularFile(it) }
                .mapToLong { Files.size(it) }
                .sum()

        if (totalSize < thresholdBytes) return

        val parent = dirPath.parent ?: return
        val zipFileName = "${dirPath.fileName}-${System.currentTimeMillis()}.zip"
        val zipPath = parent.resolve(zipFileName)

        val filesToArchive =
            Files
                .walk(dirPath)
                .filter { Files.isRegularFile(it) }
                .toList()
                .take(maxFileCount)

        if (filesToArchive.isEmpty()) return

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

        // Delete files after successful zipping to avoid duplicate storage.
        filesToArchive.forEach { file ->
            try {
                Files.deleteIfExists(file)
            } catch (ex: Exception) {
                LOGGER.warn("Failed to delete archived file {}", file, ex)
            }
        }

        LOGGER.debug(
            "Created zip archive {} for directory {} (size {} bytes). Archived and deleted {} files.",
            zipPath.fileName,
            dirPath,
            totalSize,
            filesToArchive.size,
        )
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(EventArchiveService::class.java)
        private const val ZIP_THRESHOLD_BYTES: Long = 10L * 1024 * 1024 * 1024 // 10 GiB
        private const val ZIP_MAX_FILE_COUNT: Int = 20000 // 20 000 files
    }
}
