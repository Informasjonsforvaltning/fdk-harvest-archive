package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

/**
 * Circuit-breaker-wrapped processor for [ConceptEvent] records.
 * Saves each event via [EventArchiveService.saveConcept]; failures open the circuit and trigger listener pause.
 */
@Component
open class KafkaConceptEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
    @param:Qualifier("conceptArchiveCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) : KafkaCircuitBreakerApi {

    override fun process(record: ConsumerRecord<String, Any>) {
        try {
            circuitBreaker.executeRunnable {
                when (val value = record.value()) {
                    is ConceptEvent -> {
                        if (value.type != ConceptEventType.CONCEPT_HARVESTED && value.type != ConceptEventType.CONCEPT_REMOVED) {
                            LOGGER.debug("Skipping concept event with type {}.", value.type)
                            return@executeRunnable
                        }

                        eventArchiveService.saveConcept(value)
                    }

                    is GenericRecord -> genericProcessor.process(value, TOPIC)

                    else -> LOGGER.warn(
                        "Skipping unsupported concept record value type {} on topic {}",
                        value?.javaClass?.name,
                        record.topic()
                    )
                }
            }
        } catch (e: Exception) {
            LOGGER.error("Error processing concept event", e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaConceptEventCircuitBreaker::class.java)
        private const val TOPIC = "concept-events"
    }
}
