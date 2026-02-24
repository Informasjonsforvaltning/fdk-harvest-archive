package no.fdk.fdk_harvest_archive.kafka

import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * Kafka listener for [ConceptEvent] on topic `concept-events`.
 * Processes only [no.fdk.concept.ConceptEventType.CONCEPT_HARVESTED] and [no.fdk.concept.ConceptEventType.CONCEPT_REMOVED];
 * other types are acknowledged and skipped. Delegates to the circuit breaker and nacks on failure.
 */
@Component
class KafkaConceptEventConsumer(
    private val circuitBreaker: KafkaCircuitBreakerApi<ConceptEvent>,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["concept-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeConceptEvent(
        record: ConsumerRecord<String, ConceptEvent>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received concept event - offset: {}, partition: {}", record.offset(), record.partition())

        val event = record.value()

        if (event.type != ConceptEventType.CONCEPT_HARVESTED && event.type != ConceptEventType.CONCEPT_REMOVED) {
            LOGGER.debug("Skipping concept event with type {}.", event.type)
            ack.acknowledge()
            return
        }

        try {
            circuitBreaker.process(record)
            ack.acknowledge()
        } catch (e: Exception) {
            ack.nack(Duration.ZERO)
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaConceptEventConsumer::class.java)
        const val LISTENER_ID = "concept-archive"
    }
}

