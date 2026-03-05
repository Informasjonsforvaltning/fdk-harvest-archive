package no.fdk.fdk_harvest_archive.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent
import io.github.resilience4j.circuitbreaker.CircuitBreaker.StateTransition
import no.fdk.fdk_harvest_archive.kafka.KafkaConceptEventCircuitBreaker
import no.fdk.fdk_harvest_archive.kafka.KafkaConceptEventConsumer
import no.fdk.fdk_harvest_archive.kafka.KafkaDataServiceEventCircuitBreaker
import no.fdk.fdk_harvest_archive.kafka.KafkaDataServiceEventConsumer
import no.fdk.fdk_harvest_archive.kafka.KafkaDatasetEventCircuitBreaker
import no.fdk.fdk_harvest_archive.kafka.KafkaDatasetEventConsumer
import no.fdk.fdk_harvest_archive.kafka.KafkaEventEventCircuitBreaker
import no.fdk.fdk_harvest_archive.kafka.KafkaEventEventConsumer
import no.fdk.fdk_harvest_archive.kafka.KafkaInformationModelEventCircuitBreaker
import no.fdk.fdk_harvest_archive.kafka.KafkaInformationModelEventConsumer
import no.fdk.fdk_harvest_archive.kafka.KafkaManager
import no.fdk.fdk_harvest_archive.kafka.KafkaServiceEventCircuitBreaker
import no.fdk.fdk_harvest_archive.kafka.KafkaServiceEventConsumer
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
open class HarvestCircuitBreakerConfig(
    private val kafkaManager: KafkaManager,
) {

    @Bean
    open fun circuitBreakerRegistry(): CircuitBreakerRegistry {
        val defaultConfig =
            CircuitBreakerConfig.custom()
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
        attachListener(
            registry,
            DATASET_CIRCUIT_BREAKER_ID,
            KafkaDatasetEventConsumer.LISTENER_ID,
        )
        attachListener(
            registry,
            CONCEPT_CIRCUIT_BREAKER_ID,
            KafkaConceptEventConsumer.LISTENER_ID,
        )
        attachListener(
            registry,
            DATA_SERVICE_CIRCUIT_BREAKER_ID,
            KafkaDataServiceEventConsumer.LISTENER_ID,
        )
        attachListener(
            registry,
            INFORMATION_MODEL_CIRCUIT_BREAKER_ID,
            KafkaInformationModelEventConsumer.LISTENER_ID,
        )
        attachListener(
            registry,
            EVENT_CIRCUIT_BREAKER_ID,
            KafkaEventEventConsumer.LISTENER_ID,
        )
        attachListener(
            registry,
            SERVICE_CIRCUIT_BREAKER_ID,
            KafkaServiceEventConsumer.LISTENER_ID,
        )
    }

    private fun attachListener(
        registry: CircuitBreakerRegistry,
        breakerId: String,
        listenerId: String,
    ) {
        registry.circuitBreaker(breakerId)
            .eventPublisher
            .onStateTransition { event: CircuitBreakerOnStateTransitionEvent ->
                handleStateTransition(event, listenerId)
            }
    }

    private fun handleStateTransition(
        event: CircuitBreakerOnStateTransitionEvent,
        listenerId: String,
    ) {
        LOGGER.debug("Handling state transition in circuit breaker {}", event)
        when (event.stateTransition) {
            StateTransition.CLOSED_TO_OPEN,
            StateTransition.CLOSED_TO_FORCED_OPEN,
            StateTransition.HALF_OPEN_TO_OPEN,
            -> {
                LOGGER.warn("Circuit breaker opened, pausing Kafka listener: {}", listenerId)
                kafkaManager.pause(listenerId)
            }

            StateTransition.OPEN_TO_HALF_OPEN,
            StateTransition.HALF_OPEN_TO_CLOSED,
            StateTransition.FORCED_OPEN_TO_CLOSED,
            StateTransition.FORCED_OPEN_TO_HALF_OPEN,
            -> {
                LOGGER.info("Circuit breaker closed, resuming Kafka listener: {}", listenerId)
                kafkaManager.resume(listenerId)
            }

            else -> throw IllegalStateException("Unknown transition state: " + event.stateTransition)
        }
    }

    @Bean
    open fun datasetArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(DATASET_CIRCUIT_BREAKER_ID)

    @Bean
    open fun conceptArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(CONCEPT_CIRCUIT_BREAKER_ID)

    @Bean
    open fun dataServiceArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(DATA_SERVICE_CIRCUIT_BREAKER_ID)

    @Bean
    open fun informationModelArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(INFORMATION_MODEL_CIRCUIT_BREAKER_ID)

    @Bean
    open fun eventArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(EVENT_CIRCUIT_BREAKER_ID)

    @Bean
    open fun serviceArchiveCircuitBreaker(registry: CircuitBreakerRegistry): CircuitBreaker =
        registry.circuitBreaker(SERVICE_CIRCUIT_BREAKER_ID)

    companion object {
        private val LOGGER = LoggerFactory.getLogger(HarvestCircuitBreakerConfig::class.java)
        const val CONCEPT_CIRCUIT_BREAKER_ID = "concept-archive-cb"
        const val DATA_SERVICE_CIRCUIT_BREAKER_ID = "dataservice-archive-cb"
        const val DATASET_CIRCUIT_BREAKER_ID = "dataset-archive-cb"
        const val EVENT_CIRCUIT_BREAKER_ID = "event-archive-cb"
        const val INFORMATION_MODEL_CIRCUIT_BREAKER_ID = "informationmodel-archive-cb"
        const val SERVICE_CIRCUIT_BREAKER_ID = "service-archive-cb"
    }
}
