package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Circuit-breaker-wrapped processor for [DataServiceEvent] records.
 * Saves each event via [EventArchiveService.saveDataService]; failures open the circuit and trigger listener pause.
 */
@Component
open class KafkaDataServiceEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
    private val genericProcessor: KafkaGenericProcessor,
) : KafkaCircuitBreakerApi {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(record: ConsumerRecord<String, Any>) {
        try {
            when (val value = record.value()) {
                is DataServiceEvent -> {

                    if (value.type != DataServiceEventType.DATA_SERVICE_HARVESTED && value.type != DataServiceEventType.DATA_SERVICE_REMOVED) {
                        LOGGER.debug("Skipping data service event with type {}.", value.type)
                        return
                    }

                    eventArchiveService.saveDataService(value)
                }

                is GenericRecord -> genericProcessor.process(value, TOPIC)

                else -> LOGGER.warn(
                    "Skipping unsupported data service record value type {} on topic {}",
                    value?.javaClass?.name,
                    record.topic()
                )
            }
        } catch (e: Exception) {
            LOGGER.error("Error processing data service event", e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaDataServiceEventCircuitBreaker::class.java)
        const val CIRCUIT_BREAKER_ID = "dataservice-archive-cb"
        private const val TOPIC = "data-service-events"
    }
}
