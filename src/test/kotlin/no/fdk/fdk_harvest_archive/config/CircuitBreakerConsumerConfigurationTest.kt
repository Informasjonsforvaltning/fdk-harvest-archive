package no.fdk.fdk_harvest_archive.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.mockk.mockk
import io.mockk.verify
import no.fdk.fdk_harvest_archive.kafka.KafkaManager
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Duration

@Tag("unit")
class CircuitBreakerConsumerConfigurationTest {

    @Test
    fun `circuit breaker opens after repeated failures and pauses kafka listener`() {
        val kafkaManager = mockk<KafkaManager>(relaxed = true)

        val cbConfig = CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(2)
            .minimumNumberOfCalls(2)
            .failureRateThreshold(50.0f)
            .waitDurationInOpenState(Duration.ofMillis(10))
            .build()

        val registry = CircuitBreakerRegistry.of(cbConfig)
        HarvestCircuitBreakerConfig(kafkaManager).registerListeners(registry)
        val cb = registry.circuitBreaker(HarvestCircuitBreakerConfig.DATASET_CIRCUIT_BREAKER_ID)

        repeat(2) {
            try {
                cb.executeSupplier<String> { throw RuntimeException("boom") }
            } catch (_: Exception) {
                // ignore
            }
        }

        assertEquals(CircuitBreaker.State.OPEN, cb.state)
        verify(exactly = 1) { kafkaManager.pause("dataset-archive") }
    }

    @Test
    fun `circuit breaker half-open and closed resumes kafka listener`() {
        val kafkaManager = mockk<KafkaManager>(relaxed = true)

        val registry = CircuitBreakerRegistry.ofDefaults()
        HarvestCircuitBreakerConfig(kafkaManager).registerListeners(registry)
        val cb = registry.circuitBreaker(HarvestCircuitBreakerConfig.DATASET_CIRCUIT_BREAKER_ID)

        cb.transitionToOpenState()
        cb.transitionToHalfOpenState()
        cb.transitionToClosedState()

        // OPEN->HALF_OPEN triggers resume, HALF_OPEN->CLOSED triggers resume
        verify(atLeast = 1) { kafkaManager.resume("dataset-archive") }
    }

    @Test
    fun `bean methods create circuit breakers from registry`() {
        val kafkaManager = mockk<KafkaManager>(relaxed = true)

        val config = HarvestCircuitBreakerConfig(kafkaManager)
        val registry = config.circuitBreakerRegistry()

        val datasetCb = config.datasetArchiveCircuitBreaker(registry)
        val conceptCb = config.conceptArchiveCircuitBreaker(registry)
        val dataServiceCb = config.dataServiceArchiveCircuitBreaker(registry)
        val informationModelCb = config.informationModelArchiveCircuitBreaker(registry)
        val eventCb = config.eventArchiveCircuitBreaker(registry)
        val serviceCb = config.serviceArchiveCircuitBreaker(registry)

        assertEquals(CircuitBreaker.State.CLOSED, datasetCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, conceptCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, dataServiceCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, informationModelCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, eventCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, serviceCb.state)
    }
}
