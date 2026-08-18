package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import no.fdk.service.ServiceEvent
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
open class KafkaServiceEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
    @param:Qualifier("serviceArchiveCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) : KafkaCircuitBreakerApi {
    override fun process(record: ConsumerRecord<String, Any>): ProcessOutcome = circuitBreaker.executeCallable {
        try {
            when (val value = record.value()) {
                is ServiceEvent -> {
                    if (!ARCHIVE_TYPE.allowsEventType(value.type.name)) {
                        LOGGER.debug("Skipping service event with type {}.", value.type)
                        return@executeCallable ProcessOutcome.Skipped(ARCHIVE_TYPE)
                    }
                    eventArchiveService.saveService(value)
                    ProcessOutcome.Saved(ARCHIVE_TYPE)
                }

                is GenericRecord -> genericProcessor.process(value, ARCHIVE_TYPE.topicName)

                else -> {
                    LOGGER.warn(
                        "Skipping unsupported service record value type {} on topic {}",
                        value?.javaClass?.name,
                        record.topic(),
                    )
                    ProcessOutcome.Skipped(ARCHIVE_TYPE)
                }
            }
        } catch (e: Exception) {
            LOGGER.error("Error processing service event", e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaServiceEventCircuitBreaker::class.java)
        private val ARCHIVE_TYPE = ArchiveType.SERVICE
    }
}
