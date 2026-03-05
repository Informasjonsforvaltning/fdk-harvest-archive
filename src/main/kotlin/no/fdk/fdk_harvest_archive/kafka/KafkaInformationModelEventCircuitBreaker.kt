package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

/**
 * Circuit-breaker-wrapped processor for [InformationModelEvent] records.
 * Saves each event via [EventArchiveService.saveInformationModel]; failures open the circuit and trigger listener pause.
 */
@Component
open class KafkaInformationModelEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
    @param:Qualifier("informationModelArchiveCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) : KafkaCircuitBreakerApi {

    override fun process(record: ConsumerRecord<String, Any>) {
        circuitBreaker.executeRunnable {
            try {
                when (val value = record.value()) {
                    is InformationModelEvent -> {
                        if (value.type != InformationModelEventType.INFORMATION_MODEL_HARVESTED && value.type != InformationModelEventType.INFORMATION_MODEL_REMOVED) {
                            LOGGER.debug("Skipping information model event with type {}.", value.type)
                            return@executeRunnable
                        }

                        eventArchiveService.saveInformationModel(value)
                    }

                    is GenericRecord -> genericProcessor.process(value, TOPIC)

                    else -> LOGGER.warn(
                        "Skipping unsupported information model record value type {} on topic {}",
                        value?.javaClass?.name,
                        record.topic()
                    )
                }
            } catch (e: Exception) {
                LOGGER.error("Error processing information model event", e)
                throw e
            }
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaInformationModelEventCircuitBreaker::class.java)
        private const val TOPIC = "information-model-events"
    }
}
