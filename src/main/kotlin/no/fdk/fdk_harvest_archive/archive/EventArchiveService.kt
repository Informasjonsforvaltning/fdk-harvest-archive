package no.fdk.fdk_harvest_archive.archive

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.fdk.dataset.DatasetEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.nio.file.Files
import java.nio.file.Paths

@Service
class EventArchiveService(
    @param:Value($$"${app.archive.dataset-dir}") private val datasetDir: String,
) {
    private val objectMapper = jacksonObjectMapper()

    fun saveDataset(event: DatasetEvent) {
        val dir = Paths.get(datasetDir)
        Files.createDirectories(dir)
        val filename = "${event.timestamp}_${event.fdkId}.json"
        val path = dir.resolve(filename)
        val payload = mapOf(
            "type" to event.type.name,
            "harvestRunId" to event.harvestRunId?.toString(),
            "uri" to event.uri?.toString(),
            "fdkId" to event.fdkId.toString(),
            "graph" to event.graph.toString(),
            "timestamp" to event.timestamp,
        )
        objectMapper.writeValue(path.toFile(), payload)
        LOGGER.debug("Dataset event saved to {}", path)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(EventArchiveService::class.java)
    }
}
