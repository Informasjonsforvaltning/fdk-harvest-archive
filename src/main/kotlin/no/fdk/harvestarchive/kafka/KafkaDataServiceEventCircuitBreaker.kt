package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

@Component
open class KafkaDataServiceEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
    @param:Qualifier("dataServiceArchiveCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) : KafkaCircuitBreakerApi {
    override fun process(record: ConsumerRecord<String, Any>): ProcessOutcome = circuitBreaker.executeCallable {
        try {
            when (val value = record.value()) {
                is DataServiceEvent -> {
                    if (value.type != DataServiceEventType.DATA_SERVICE_HARVESTED &&
                        value.type != DataServiceEventType.DATA_SERVICE_REMOVED
                    ) {
                        LOGGER.debug("Skipping data service event with type {}.", value.type)
                        return@executeCallable ProcessOutcome.Skipped(ARCHIVE_TYPE)
                    }
                    eventArchiveService.saveDataService(value)
                    ProcessOutcome.Saved(ARCHIVE_TYPE)
                }

                is GenericRecord -> genericProcessor.process(value, TOPIC)

                else -> {
                    LOGGER.warn(
                        "Skipping unsupported data service record value type {} on topic {}",
                        value?.javaClass?.name,
                        record.topic(),
                    )
                    ProcessOutcome.Skipped(ARCHIVE_TYPE)
                }
            }
        } catch (e: Exception) {
            LOGGER.error("Error processing data service event", e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaDataServiceEventCircuitBreaker::class.java)
        private const val TOPIC = "data-service-events"
        private val ARCHIVE_TYPE = ArchiveType.DATA_SERVICE
    }
}
