package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
open class KafkaInformationModelEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
    @param:Qualifier("informationModelArchiveCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) : KafkaCircuitBreakerApi {
    override fun process(record: ConsumerRecord<String, Any>): ProcessOutcome = circuitBreaker.executeCallable {
        try {
            when (val value = record.value()) {
                is InformationModelEvent -> {
                    if (value.type != InformationModelEventType.INFORMATION_MODEL_HARVESTED &&
                        value.type != InformationModelEventType.INFORMATION_MODEL_REMOVED
                    ) {
                        LOGGER.debug("Skipping information model event with type {}.", value.type)
                        return@executeCallable ProcessOutcome.Skipped(ARCHIVE_TYPE)
                    }
                    eventArchiveService.saveInformationModel(value)
                    ProcessOutcome.Saved(ARCHIVE_TYPE)
                }

                is GenericRecord -> genericProcessor.process(value, ARCHIVE_TYPE.topicName)

                else -> {
                    LOGGER.warn(
                        "Skipping unsupported information model record value type {} on topic {}",
                        value?.javaClass?.name,
                        record.topic(),
                    )
                    ProcessOutcome.Skipped(ARCHIVE_TYPE)
                }
            }
        } catch (e: Exception) {
            LOGGER.error("Error processing information model event", e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaInformationModelEventCircuitBreaker::class.java)
        private val ARCHIVE_TYPE = ArchiveType.INFORMATION_MODEL
    }
}
