package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

/**
 * Circuit-breaker-wrapped processor for [ServiceEvent] records.
 * Saves each event via [EventArchiveService.saveService]; failures open the circuit and trigger listener pause.
 */
@Component
open class KafkaServiceEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
    @param:Qualifier("serviceArchiveCircuitBreaker")
    private val circuitBreaker: CircuitBreaker,
) : KafkaCircuitBreakerApi {

    override fun process(record: ConsumerRecord<String, Any>) {
        circuitBreaker.executeRunnable {
            try {
                when (val value = record.value()) {
                    is ServiceEvent -> {
                        if (value.type != ServiceEventType.SERVICE_HARVESTED && value.type != ServiceEventType.SERVICE_REMOVED) {
                            LOGGER.debug("Skipping service event with type {}.", value.type)
                            return@executeRunnable
                        }

                        eventArchiveService.saveService(value)
                    }

                    is GenericRecord -> genericProcessor.process(value, TOPIC)

                    else -> LOGGER.warn(
                        "Skipping unsupported service record value type {} on topic {}",
                        value?.javaClass?.name,
                        record.topic()
                    )
                }
            } catch (e: Exception) {
                LOGGER.error("Error processing service event", e)
                throw e
            }
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaServiceEventCircuitBreaker::class.java)
        private const val TOPIC = "service-events"
    }
}
