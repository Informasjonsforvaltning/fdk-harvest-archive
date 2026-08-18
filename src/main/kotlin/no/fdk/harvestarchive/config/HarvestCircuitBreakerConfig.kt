package no.fdk.harvestarchive.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreaker.StateTransition
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.kafka.KafkaManager
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
open class HarvestCircuitBreakerConfig(private val kafkaManager: KafkaManager, private val archiveMetrics: ArchiveMetrics) {
    @Bean
    open fun circuitBreakerRegistry(): CircuitBreakerRegistry {
        val defaultConfig =
            CircuitBreakerConfig
                .custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(50f)
                .waitDurationInOpenState(Duration.ofSeconds(60))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build()

        val registry = CircuitBreakerRegistry.of(defaultConfig)
        registerListeners(registry)
        return registry
    }

    open fun registerListeners(registry: CircuitBreakerRegistry) {
        ArchiveType.entries.forEach { archiveType ->
            attachListener(registry, archiveType)
        }
    }

    private fun attachListener(registry: CircuitBreakerRegistry, archiveType: ArchiveType) {
        registry
            .circuitBreaker(archiveType.circuitBreakerId)
            .eventPublisher
            .onStateTransition { event: CircuitBreakerOnStateTransitionEvent ->
                handleStateTransition(event, archiveType.listenerId)
            }
    }

    private fun handleStateTransition(event: CircuitBreakerOnStateTransitionEvent, listenerId: String) {
        LOGGER.debug("Handling state transition in circuit breaker {}", event)
        when (event.stateTransition) {
            StateTransition.CLOSED_TO_OPEN,
            StateTransition.CLOSED_TO_FORCED_OPEN,
            StateTransition.HALF_OPEN_TO_OPEN,
            -> {
                LOGGER.warn("Circuit breaker opened, pausing Kafka listener: {}", listenerId)
                kafkaManager.pause(listenerId)
                archiveMetrics.setListenerPaused(listenerId, true)
            }

            StateTransition.OPEN_TO_HALF_OPEN,
            StateTransition.HALF_OPEN_TO_CLOSED,
            StateTransition.FORCED_OPEN_TO_CLOSED,
            StateTransition.FORCED_OPEN_TO_HALF_OPEN,
            -> {
                LOGGER.info("Circuit breaker closed, resuming Kafka listener: {}", listenerId)
                kafkaManager.resume(listenerId)
                archiveMetrics.setListenerPaused(listenerId, false)
            }

            else -> {
                throw IllegalStateException("Unknown transition state: " + event.stateTransition)
            }
        }
    }

    @Bean
    open fun datasetArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(ArchiveType.DATASET.circuitBreakerId)

    @Bean
    open fun conceptArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(ArchiveType.CONCEPT.circuitBreakerId)

    @Bean
    open fun dataServiceArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(ArchiveType.DATA_SERVICE.circuitBreakerId)

    @Bean
    open fun informationModelArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(ArchiveType.INFORMATION_MODEL.circuitBreakerId)

    @Bean
    open fun eventArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(ArchiveType.EVENT.circuitBreakerId)

    @Bean
    open fun serviceArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(ArchiveType.SERVICE.circuitBreakerId)

    companion object {
        private val LOGGER = LoggerFactory.getLogger(HarvestCircuitBreakerConfig::class.java)
    }
}
