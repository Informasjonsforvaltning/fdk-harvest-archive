package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.dataset.DatasetEvent
import no.fdk.dataset.DatasetEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Circuit-breaker-wrapped processor for [DatasetEvent] records.
 * Saves each event via [EventArchiveService.saveDataset]; failures open the circuit and trigger listener pause.
 */
@Component
open class KafkaDatasetEventCircuitBreaker(
    private val eventArchiveService: EventArchiveService,
) : KafkaCircuitBreakerApi<DatasetEvent> {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(event: DatasetEvent) {
        if (event.type != DatasetEventType.DATASET_HARVESTED && event.type != DatasetEventType.DATASET_REMOVED) {
            LOGGER.debug("Skipping dataset event with type {}.", event.type)
            return
        }

        try {
            eventArchiveService.saveDataset(event)
        } catch (e: Exception) {
            LOGGER.error("Error processing dataset event for fdkId: {}", event.fdkId, e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaDatasetEventCircuitBreaker::class.java)
        const val CIRCUIT_BREAKER_ID = "dataset-archive-cb"
    }
}
