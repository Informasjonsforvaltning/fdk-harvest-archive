package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Circuit-breaker-wrapped processor for [ConceptEvent] records.
 * Saves each event via [EventArchiveService.saveConcept]; failures open the circuit and trigger listener pause.
 */
@Component
open class KafkaConceptEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
) : KafkaCircuitBreakerApi<ConceptEvent> {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(event: ConceptEvent) {
        if (event.type != ConceptEventType.CONCEPT_HARVESTED && event.type != ConceptEventType.CONCEPT_REMOVED) {
            LOGGER.debug("Skipping concept event with type {}.", event.type)
            return
        }

        try {
            eventArchiveService.saveConcept(event)
        } catch (e: Exception) {
            LOGGER.error("Error processing concept event for fdkId: {}", event.fdkId, e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaConceptEventCircuitBreaker::class.java)
        const val CIRCUIT_BREAKER_ID = "concept-archive-cb"
    }
}
