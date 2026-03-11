package no.fdk.fdk_harvest_archive.archive

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.dataset.DatasetEvent
import no.fdk.dataset.DatasetEventType
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
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

    private val topicToDir: Map<String, String> = mapOf(
        "dataset-events" to datasetDir,
        "concept-events" to conceptDir,
        "data-service-events" to dataServiceDir,
        "information-model-events" to informationModelDir,
        "event-events" to eventDir,
        "service-events" to serviceDir,
    )

    private val topicToAllowedTypes: Map<String, Set<String>> = mapOf(
        "dataset-events" to setOf(DatasetEventType.DATASET_HARVESTED.name, DatasetEventType.DATASET_REMOVED.name),
        "concept-events" to setOf(ConceptEventType.CONCEPT_HARVESTED.name, ConceptEventType.CONCEPT_REMOVED.name),
        "data-service-events" to setOf(DataServiceEventType.DATA_SERVICE_HARVESTED.name, DataServiceEventType.DATA_SERVICE_REMOVED.name),
        "information-model-events" to setOf(InformationModelEventType.INFORMATION_MODEL_HARVESTED.name, InformationModelEventType.INFORMATION_MODEL_REMOVED.name),
        "event-events" to setOf(EventEventType.EVENT_HARVESTED.name, EventEventType.EVENT_REMOVED.name),
        "service-events" to setOf(ServiceEventType.SERVICE_HARVESTED.name, ServiceEventType.SERVICE_REMOVED.name),
    )

    /**
     * Saves a generic (map) payload to the directory for the given topic, only if the event type is HARVESTED or REMOVED for that topic.
     */
    fun saveGenericForTopic(topic: String, payload: Map<String, Any?>) {
        val dir = topicToDir[topic] ?: return
        val allowedTypes = topicToAllowedTypes[topic] ?: return
        val type = payload["type"]?.toString() ?: return
        if (type !in allowedTypes) {
            LOGGER.debug("Skipping generic event with type {} for topic {}", type, topic)
            return
        }
        val filename = "${payload["timestamp"]}_${payload["fdkId"]}.json"
        saveAsFile(dir, filename, payload)
        LOGGER.debug("Generic event saved to {}", filename)
    }

    fun saveDataset(event: DatasetEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(datasetDir, filename, payload)
        LOGGER.debug("Dataset event saved to {}", filename)
    }

    fun saveConcept(event: ConceptEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(conceptDir, filename, payload)
        LOGGER.debug("Concept event saved to {}", filename)
    }

    fun saveDataService(event: DataServiceEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(dataServiceDir, filename, payload)
        LOGGER.debug("DataService event saved to {}", filename)
    }

    fun saveInformationModel(event: InformationModelEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(informationModelDir, filename, payload)
        LOGGER.debug("InformationModel event saved to {}", filename)
    }

    fun saveEvent(event: EventEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(eventDir, filename, payload)
        LOGGER.debug("Event event saved to {}", filename)
    }

    fun saveService(event: ServiceEvent) {
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(serviceDir, filename, payload)
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
        listOf(datasetDir, conceptDir, dataServiceDir, informationModelDir, eventDir, serviceDir)
            .map { Paths.get(it) }
            .filter { Files.exists(it) }
            .forEach { createZipIfLargerThanThreshold(it) }
    }

    private fun createZipIfLargerThanThreshold(dirPath: Path, thresholdBytes: Long = zipThresholdBytes, maxFileCount: Int = ZIP_MAX_FILE_COUNT) {
        val totalSize = Files.walk(dirPath)
            .filter { Files.isRegularFile(it) }
            .mapToLong { Files.size(it) }
            .sum()

        if (totalSize < thresholdBytes) return

        val parent = dirPath.parent ?: return
        val zipFileName = "${dirPath.fileName}-${System.currentTimeMillis()}.zip"
        val zipPath = parent.resolve(zipFileName)

        val filesToArchive = Files.walk(dirPath)
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

        LOGGER.debug("Created zip archive {} for directory {} (size {} bytes). Archived and deleted {} files.", zipPath.fileName, dirPath, totalSize, filesToArchive.size)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(EventArchiveService::class.java)
        private const val ZIP_THRESHOLD_BYTES: Long = 10L * 1024 * 1024 * 1024 // 10 GiB
        private const val ZIP_MAX_FILE_COUNT: Int = 20000 // 20 000 files
    }
}
