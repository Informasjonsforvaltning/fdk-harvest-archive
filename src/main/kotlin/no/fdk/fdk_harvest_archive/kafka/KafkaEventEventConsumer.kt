package no.fdk.fdk_harvest_archive.kafka

import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * Kafka listener for [EventEvent] on topic `event-events`.
 * Processes only [EventEventType.EVENT_HARVESTED] and [EventEventType.EVENT_REMOVED];
 * other types are acknowledged and skipped. Delegates to the circuit breaker and nacks on failure.
 */
@Component
class KafkaEventEventConsumer(
    private val circuitBreaker: KafkaCircuitBreakerApi<EventEvent>,
    private val genericCircuitBreaker: KafkaGenericCircuitBreaker,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["event-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeEventEvent(
        record: ConsumerRecord<String, Any>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received event event - offset: {}, partition: {}", record.offset(), record.partition())

        val event = record.value()

        try {
            if (event is SpecificRecord) {
                circuitBreaker.process(event as EventEvent)
            } else {
                genericCircuitBreaker.process(event as GenericRecord, TOPIC)
            }
            ack.acknowledge()
        } catch (e: Exception) {
            ack.nack(Duration.ZERO)
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaEventEventConsumer::class.java)
        const val LISTENER_ID = "event-archive"
        private const val TOPIC = "event-events"
    }
}
