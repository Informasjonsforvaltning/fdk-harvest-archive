package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.event.EventEvent
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
open class KafkaEventEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
) : KafkaCircuitBreakerApi<EventEvent> {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(record: ConsumerRecord<String, EventEvent>) {
        val event = record.value()

        LOGGER.debug("Processing event event - offset: {}, partition: {}", record.offset(), record.partition())
        try {
            eventArchiveService.saveEvent(event)
        } catch (e: Exception) {
            LOGGER.error("Error processing event event for fdkId: {}", event.fdkId, e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaEventEventCircuitBreaker::class.java)
        const val CIRCUIT_BREAKER_ID = "event-archive-cb"
    }
}
