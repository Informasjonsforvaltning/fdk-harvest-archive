package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.dataset.DatasetEvent
import no.fdk.dataset.DatasetEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

/**
 * Circuit-breaker-wrapped processor for [DatasetEvent] records.
 * Saves each event via [EventArchiveService.saveDataset]; failures open the circuit and trigger listener pause.
 */
@Component
open class KafkaDatasetEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
    @param:Qualifier("datasetArchiveCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) : KafkaCircuitBreakerApi {

    override fun process(record: ConsumerRecord<String, Any>) {
        circuitBreaker.executeRunnable {
            try {
                when (val value = record.value()) {
                    is DatasetEvent -> {
                        if (value.type != DatasetEventType.DATASET_HARVESTED && value.type != DatasetEventType.DATASET_REMOVED) {
                            LOGGER.debug("Skipping dataset event with type {}.", value.type)
                            return@executeRunnable
                        }

                        eventArchiveService.saveDataset(value)
                    }

                    is GenericRecord -> genericProcessor.process(value, TOPIC)

                    else -> LOGGER.warn(
                        "Skipping unsupported dataset record value type {} on topic {}",
                        value?.javaClass?.name,
                        record.topic()
                    )
                }
            } catch (e: Exception) {
                LOGGER.error("Error processing dataset event", e)
                throw e
            }
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaDatasetEventCircuitBreaker::class.java)
        private const val TOPIC = "dataset-events"
    }
}
