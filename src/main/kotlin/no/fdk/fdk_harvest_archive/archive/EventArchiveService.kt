package no.fdk.fdk_harvest_archive.archive

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.fdk.concept.ConceptEvent
import no.fdk.dataset.DatasetEvent
import no.fdk.dataservice.DataServiceEvent
import no.fdk.event.EventEvent
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.service.ServiceEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.nio.file.Files
import java.nio.file.Paths

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

    fun saveDataset(event: DatasetEvent) {
        val path = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(datasetDir, path, payload)
        LOGGER.debug("Dataset event saved to {}", path)
    }

    fun saveConcept(event: ConceptEvent) {
        val path = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(conceptDir, path, payload)
        LOGGER.debug("Concept event saved to {}", path)
    }

    fun saveDataService(event: DataServiceEvent) {
        val path = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(dataServiceDir, path, payload)
        LOGGER.debug("DataService event saved to {}", path)
    }

    fun saveInformationModel(event: InformationModelEvent) {
        val path = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(informationModelDir, path, payload)
        LOGGER.debug("InformationModel event saved to {}", path)
    }

    fun saveEvent(event: EventEvent) {
        val path = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(eventDir, path, payload)
        LOGGER.debug("Event event saved to {}", path)
    }

    fun saveService(event: ServiceEvent) {
        val path = "${event.timestamp}_${event.fdkId}.json"
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        saveAsFile(serviceDir, path, payload)
        LOGGER.debug("Service event saved to {}", path)
    }

    private fun saveAsFile(dir: String, filename: String, payload: Map<String, Any?>) {
        val dirPath = Paths.get(dir)
        Files.createDirectories(dirPath)
        val path = dirPath.resolve(filename)
        objectMapper.writeValue(path.toFile(), payload)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(EventArchiveService::class.java)
    }
}
