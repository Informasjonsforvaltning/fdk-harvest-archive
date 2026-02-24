package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import no.fdk.informationmodel.InformationModelEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
open class KafkaInformationModelEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
) : KafkaCircuitBreakerApi<InformationModelEvent> {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(record: ConsumerRecord<String, InformationModelEvent>) {
        val event = record.value()

        LOGGER.debug("Processing information model event - offset: {}, partition: {}", record.offset(), record.partition())
        try {
            eventArchiveService.saveInformationModel(event)
        } catch (e: Exception) {
            LOGGER.error("Error processing information model event for fdkId: {}", event.fdkId, e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaInformationModelEventCircuitBreaker::class.java)
        const val CIRCUIT_BREAKER_ID = "informationmodel-archive-cb"
    }
}
