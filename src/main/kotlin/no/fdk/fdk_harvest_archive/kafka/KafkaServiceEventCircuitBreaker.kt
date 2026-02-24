package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import no.fdk.service.ServiceEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Circuit-breaker-wrapped processor for [ServiceEvent] records.
 * Saves each event via [EventArchiveService.saveService]; failures open the circuit and trigger listener pause.
 */
@Component
open class KafkaServiceEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
) : KafkaCircuitBreakerApi<ServiceEvent> {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(record: ConsumerRecord<String, ServiceEvent>) {
        val event = record.value()

        LOGGER.debug("Processing service event - offset: {}, partition: {}", record.offset(), record.partition())
        try {
            eventArchiveService.saveService(event)
        } catch (e: Exception) {
            LOGGER.error("Error processing service event for fdkId: {}", event.fdkId, e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaServiceEventCircuitBreaker::class.java)
        const val CIRCUIT_BREAKER_ID = "service-archive-cb"
    }
}
