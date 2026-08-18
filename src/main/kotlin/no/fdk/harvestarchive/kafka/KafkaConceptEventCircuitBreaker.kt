package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
open class KafkaConceptEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
    @param:Qualifier("conceptArchiveCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) : KafkaCircuitBreakerApi {
    override fun process(record: ConsumerRecord<String, Any>): ProcessOutcome {
        try {
            return circuitBreaker.executeCallable {
                when (val value = record.value()) {
                    is ConceptEvent -> {
                        if (value.type != ConceptEventType.CONCEPT_HARVESTED && value.type != ConceptEventType.CONCEPT_REMOVED) {
                            LOGGER.debug("Skipping concept event with type {}.", value.type)
                            return@executeCallable ProcessOutcome.Skipped(ARCHIVE_TYPE)
                        }
                        eventArchiveService.saveConcept(value)
                        ProcessOutcome.Saved(ARCHIVE_TYPE)
                    }

                    is GenericRecord -> genericProcessor.process(value, ARCHIVE_TYPE.topicName)

                    else -> {
                        LOGGER.warn(
                            "Skipping unsupported concept record value type {} on topic {}",
                            value?.javaClass?.name,
                            record.topic(),
                        )
                        ProcessOutcome.Skipped(ARCHIVE_TYPE)
                    }
                }
            }
        } catch (e: Exception) {
            LOGGER.error("Error processing concept event", e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaConceptEventCircuitBreaker::class.java)
        private val ARCHIVE_TYPE = ArchiveType.CONCEPT
    }
}
