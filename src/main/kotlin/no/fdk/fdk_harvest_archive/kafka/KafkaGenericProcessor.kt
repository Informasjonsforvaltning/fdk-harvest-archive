package no.fdk.fdk_harvest_archive.kafka

import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
open class KafkaGenericProcessor(
    private val eventArchiveService: EventArchiveService,
) {

    fun process(event: GenericRecord, topic: String) {
        try {
            val payload = mapOf<String, Any?>(
                "type" to event.get("type")?.toString(),
                "harvestRunId" to event.get("harvestRunId")?.toString(),
                "uri" to event.get("uri")?.toString(),
                "fdkId" to event.get("fdkId")?.toString(),
                "graph" to event.get("graph")?.toString(),
                "timestamp" to event.get("timestamp")?.toString(),
            )
            eventArchiveService.saveGenericForTopic(topic, payload)
        } catch (e: Exception) {
            LOGGER.error("Error processing generic record with fdkId: {} and type: {}", event.get("fdkId"), event.get("type"), e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaGenericProcessor::class.java)
    }
}
