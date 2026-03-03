package no.fdk.fdk_harvest_archive.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
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
    @param:Qualifier("kafkaEventEventCircuitBreaker")
    private val circuitBreaker: KafkaCircuitBreakerApi,
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

        try {
            circuitBreaker.process(record)
            ack.acknowledge()
        } catch (e: Exception) {
            ack.nack(Duration.ZERO)
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaEventEventConsumer::class.java)
        const val LISTENER_ID = "event-archive"
    }
}
