package no.fdk.fdk_harvest_archive.kafka

import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class KafkaEventEventConsumer(
    private val circuitBreaker: KafkaCircuitBreakerApi<EventEvent>,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["event-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeEventEvent(
        record: ConsumerRecord<String, EventEvent>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received event event - offset: {}, partition: {}", record.offset(), record.partition())

        val event = record.value()

        if (event.type != EventEventType.EVENT_HARVESTED && event.type != EventEventType.EVENT_REMOVED) {
            LOGGER.debug("Skipping event event with type {}.", event.type)
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
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaEventEventConsumer::class.java)
        const val LISTENER_ID = "event-archive"
    }
}
