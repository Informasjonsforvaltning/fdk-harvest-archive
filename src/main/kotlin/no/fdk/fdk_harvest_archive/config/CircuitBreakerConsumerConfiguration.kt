package no.fdk.fdk_harvest_archive.config

import io.github.resilience4j.circuitbreaker.CircuitBreaker.StateTransition
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent
import no.fdk.fdk_harvest_archive.kafka.KafkaConceptEventCircuitBreaker
import no.fdk.fdk_harvest_archive.kafka.KafkaConceptEventConsumer
import no.fdk.fdk_harvest_archive.kafka.KafkaDatasetEventCircuitBreaker
import no.fdk.fdk_harvest_archive.kafka.KafkaDatasetEventConsumer
import no.fdk.fdk_harvest_archive.kafka.KafkaManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Configuration

/**
 * Listens to circuit breaker state transitions and pauses/resumes the harvest Kafka consumer
 * when the breaker opens (e.g. after repeated harvest failures) or closes again.
 */
@Configuration
open class CircuitBreakerConsumerConfiguration(
    private val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val kafkaManager: KafkaManager,
) {
    init {
        LOGGER.debug("Configuring circuit breaker event listener")
        circuitBreakerRegistry.circuitBreaker(KafkaDatasetEventCircuitBreaker.CIRCUIT_BREAKER_ID)
            .eventPublisher
            .onStateTransition { event: CircuitBreakerOnStateTransitionEvent ->
                handleStateTransition(event, KafkaDatasetEventConsumer.LISTENER_ID)
            }

        circuitBreakerRegistry.circuitBreaker(KafkaConceptEventCircuitBreaker.CIRCUIT_BREAKER_ID)
            .eventPublisher
            .onStateTransition { event: CircuitBreakerOnStateTransitionEvent ->
                handleStateTransition(event, KafkaConceptEventConsumer.LISTENER_ID)
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

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(CircuitBreakerConsumerConfiguration::class.java)
    }
}
