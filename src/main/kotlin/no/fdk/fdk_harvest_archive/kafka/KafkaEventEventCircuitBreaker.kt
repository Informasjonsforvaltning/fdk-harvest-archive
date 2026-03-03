package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Circuit-breaker-wrapped processor for [EventEvent] records.
 * Saves each event via [EventArchiveService.saveEvent]; failures open the circuit and trigger listener pause.
 */
@Component
open class KafkaEventEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
) : KafkaCircuitBreakerApi {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(record: ConsumerRecord<String, Any>) {
        try {
            when (val value = record.value()) {
                is EventEvent -> {
                    if (value.type != EventEventType.EVENT_HARVESTED && value.type != EventEventType.EVENT_REMOVED) {
                        LOGGER.debug("Skipping event event with type {}.", value.type)
                        return
                    }

                    eventArchiveService.saveEvent(value)
                }

                is GenericRecord -> genericProcessor.process(value, TOPIC)

                else -> LOGGER.warn(
                    "Skipping unsupported event record value type {} on topic {}",
                    value?.javaClass?.name,
                    record.topic()
                )
            }
        } catch (e: Exception) {
            LOGGER.error("Error processing event event", e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaEventEventCircuitBreaker::class.java)
        const val CIRCUIT_BREAKER_ID = "event-archive-cb"
        private const val TOPIC = "event-events"
    }
}
