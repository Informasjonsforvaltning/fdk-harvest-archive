package no.fdk.harvestarchive.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.mockk
import io.mockk.verify
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.kafka.KafkaManager
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Duration

@Tag("unit")
class CircuitBreakerConsumerConfigurationTest {
    @Test
    fun `circuit breaker opens after repeated failures and pauses kafka listener`() {
        val kafkaManager = mockk<KafkaManager>(relaxed = true)

        val cbConfig =
            CircuitBreakerConfig
                .custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(2)
                .minimumNumberOfCalls(2)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofMillis(10))
                .build()

        val registry = CircuitBreakerRegistry.of(cbConfig)
        HarvestCircuitBreakerConfig(kafkaManager, ArchiveMetrics(SimpleMeterRegistry())).registerListeners(registry)
        val cb = registry.circuitBreaker(ArchiveType.DATASET.circuitBreakerId)

        repeat(2) {
            try {
                cb.executeSupplier<String> { throw RuntimeException("boom") }
            } catch (_: Exception) {
                // ignore
            }
        }

        assertEquals(CircuitBreaker.State.OPEN, cb.state)
        verify(exactly = 1) { kafkaManager.pause(ArchiveType.DATASET.listenerId) }
    }

    @Test
    fun `circuit breaker half-open and closed resumes kafka listener`() {
        val kafkaManager = mockk<KafkaManager>(relaxed = true)

        val registry = CircuitBreakerRegistry.ofDefaults()
        HarvestCircuitBreakerConfig(kafkaManager, ArchiveMetrics(SimpleMeterRegistry())).registerListeners(registry)
        val cb = registry.circuitBreaker(ArchiveType.DATASET.circuitBreakerId)

        cb.transitionToOpenState()
        cb.transitionToHalfOpenState()
        cb.transitionToClosedState()

        // OPEN->HALF_OPEN triggers resume, HALF_OPEN->CLOSED triggers resume
        verify(atLeast = 1) { kafkaManager.resume(ArchiveType.DATASET.listenerId) }
    }

    @Test
    fun `bean methods create circuit breakers from registry`() {
        val kafkaManager = mockk<KafkaManager>(relaxed = true)

        val config = HarvestCircuitBreakerConfig(kafkaManager, ArchiveMetrics(SimpleMeterRegistry()))
        val registry = config.circuitBreakerRegistry()

        val datasetCb = config.datasetArchiveCircuitBreaker(registry)
        val conceptCb = config.conceptArchiveCircuitBreaker(registry)
        val dataServiceCb = config.dataServiceArchiveCircuitBreaker(registry)
        val informationModelCb = config.informationModelArchiveCircuitBreaker(registry)
        val eventCb = config.eventArchiveCircuitBreaker(registry)
        val serviceCb = config.serviceArchiveCircuitBreaker(registry)

        assertEquals(ArchiveType.DATASET.circuitBreakerId, datasetCb.name)
        assertEquals(ArchiveType.CONCEPT.circuitBreakerId, conceptCb.name)
        assertEquals(ArchiveType.DATA_SERVICE.circuitBreakerId, dataServiceCb.name)
        assertEquals(ArchiveType.INFORMATION_MODEL.circuitBreakerId, informationModelCb.name)
        assertEquals(ArchiveType.EVENT.circuitBreakerId, eventCb.name)
        assertEquals(ArchiveType.SERVICE.circuitBreakerId, serviceCb.name)
        assertEquals(CircuitBreaker.State.CLOSED, datasetCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, conceptCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, dataServiceCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, informationModelCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, eventCb.state)
        assertEquals(CircuitBreaker.State.CLOSED, serviceCb.state)
    }
}
