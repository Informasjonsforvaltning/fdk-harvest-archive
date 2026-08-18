package no.fdk.harvestarchive.archive

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.fdk.concept.ConceptEvent
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataset.DatasetEvent
import no.fdk.event.EventEvent
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.service.ServiceEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.time.Duration
import kotlin.time.measureTimedValue

/**
 * Persists harvest events as JSON files under type-specific directories.
 * Each event is written as `{timestamp}_{fdkId}.json` with type, harvestRunId, uri, fdkId, graph, and timestamp.
 */
@Service
class EventArchiveService(private val archiveDirectories: ArchiveDirectories, private val archiveMetrics: ArchiveMetrics) {
    private val objectMapper = jacksonObjectMapper()

    /**
     * Saves a generic (map) payload to the directory for the given topic, only if the event type is HARVESTED or REMOVED for that topic.
     */
    fun saveGenericForTopic(topic: String, payload: Map<String, Any?>): ArchiveWrite {
        val archiveType = ArchiveType.fromTopic(topic)
        if (archiveType == null) {
            return ArchiveWrite.Skipped(null)
        }
        val eventType = payload["type"]?.toString()
        if (eventType == null) {
            return ArchiveWrite.Skipped(archiveType)
        }
        if (!archiveType.allowsEventType(eventType)) {
            LOGGER.debug("Skipping generic event with type {} for topic {}", eventType, topic)
            return ArchiveWrite.Skipped(archiveType)
        }
        savePayload(archiveType, payload)
        return ArchiveWrite.Saved(archiveType)
    }

    fun saveDataset(event: DatasetEvent) {
        saveTyped(
            ArchiveType.DATASET,
            type = event.type.name,
            harvestRunId = event.harvestRunId,
            uri = event.uri,
            fdkId = event.fdkId,
            graph = event.graph,
            catalogGraph = event.catalogGraph,
            timestamp = event.timestamp,
        )
    }

    fun saveConcept(event: ConceptEvent) {
        saveTyped(
            ArchiveType.CONCEPT,
            type = event.type.name,
            harvestRunId = event.harvestRunId,
            uri = event.uri,
            fdkId = event.fdkId,
            graph = event.graph,
            catalogGraph = event.catalogGraph,
            timestamp = event.timestamp,
        )
    }

    fun saveDataService(event: DataServiceEvent) {
        saveTyped(
            ArchiveType.DATA_SERVICE,
            type = event.type.name,
            harvestRunId = event.harvestRunId,
            uri = event.uri,
            fdkId = event.fdkId,
            graph = event.graph,
            catalogGraph = event.catalogGraph,
            timestamp = event.timestamp,
        )
    }

    fun saveInformationModel(event: InformationModelEvent) {
        saveTyped(
            ArchiveType.INFORMATION_MODEL,
            type = event.type.name,
            harvestRunId = event.harvestRunId,
            uri = event.uri,
            fdkId = event.fdkId,
            graph = event.graph,
            catalogGraph = event.catalogGraph,
            timestamp = event.timestamp,
        )
    }

    fun saveEvent(event: EventEvent) {
        saveTyped(
            ArchiveType.EVENT,
            type = event.type.name,
            harvestRunId = event.harvestRunId,
            uri = event.uri,
            fdkId = event.fdkId,
            graph = event.graph,
            catalogGraph = event.catalogGraph,
            timestamp = event.timestamp,
        )
    }

    fun saveService(event: ServiceEvent) {
        saveTyped(
            ArchiveType.SERVICE,
            type = event.type.name,
            harvestRunId = event.harvestRunId,
            uri = event.uri,
            fdkId = event.fdkId,
            graph = event.graph,
            catalogGraph = event.catalogGraph,
            timestamp = event.timestamp,
        )
    }

    private fun saveTyped(
        archiveType: ArchiveType,
        type: String,
        harvestRunId: Any?,
        uri: Any?,
        fdkId: Any?,
        graph: Any?,
        catalogGraph: Any?,
        timestamp: Long,
    ) {
        savePayload(
            archiveType,
            mapOf(
                "type" to type,
                "harvestRunId" to harvestRunId?.toString(),
                "uri" to uri?.toString(),
                "fdkId" to fdkId?.toString(),
                "graph" to graph?.toString(),
                "catalogGraph" to catalogGraph?.toString(),
                "timestamp" to timestamp,
            ),
        )
    }

    private fun savePayload(archiveType: ArchiveType, payload: Map<String, Any?>) {
        val eventType = payload["type"]?.toString() ?: "unknown"
        val filename = "${payload["timestamp"]}_${payload["fdkId"]}.json"
        try {
            val result = saveAsFile(archiveDirectories[archiveType], filename, payload)
            archiveMetrics.recordSaved(archiveType, eventType, result.bytes, result.duration)
            LOGGER.debug("Event saved to {}", filename)
        } catch (e: Exception) {
            archiveMetrics.recordSaveError(archiveType, eventType)
            throw e
        }
    }

    private fun saveAsFile(dir: String, filename: String, payload: Map<String, Any?>): SaveResult {
        val timed =
            measureTimedValue {
                val dirPath = Paths.get(dir)
                Files.createDirectories(dirPath)
                val path = dirPath.resolve(filename)
                objectMapper.writeValue(path.toFile(), payload)
                Files.size(path)
            }
        return SaveResult(bytes = timed.value, duration = timed.duration)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(EventArchiveService::class.java)
    }

    private data class SaveResult(val bytes: Long, val duration: Duration)
}
