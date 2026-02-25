package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
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
) : KafkaCircuitBreakerApi<DataServiceEvent> {

    @CircuitBreaker(name = CIRCUIT_BREAKER_ID)
    override fun process(event: DataServiceEvent) {

        if (event.type != DataServiceEventType.DATA_SERVICE_HARVESTED && event.type != DataServiceEventType.DATA_SERVICE_REMOVED) {
            LOGGER.debug("Skipping data service event with type {}.", event.type)
            return
        }

        try {
            eventArchiveService.saveDataService(event)
        } catch (e: Exception) {
            LOGGER.error("Error processing data service event for fdkId: {}", event.fdkId, e)
            throw e
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaDataServiceEventCircuitBreaker::class.java)
        const val CIRCUIT_BREAKER_ID = "dataservice-archive-cb"
    }
}
