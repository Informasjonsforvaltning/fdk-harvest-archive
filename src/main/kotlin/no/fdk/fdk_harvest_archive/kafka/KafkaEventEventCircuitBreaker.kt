package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
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
) : KafkaCircuitBreakerApi<EventEvent> {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(event: EventEvent) {
        if (event.type != EventEventType.EVENT_HARVESTED && event.type != EventEventType.EVENT_REMOVED) {
            LOGGER.debug("Skipping event event with type {}.", event.type)
            return
        }

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
