package no.fdk.fdk_harvest_archive.kafka

import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * Kafka listener for [ServiceEvent] on topic `service-events`.
 * Processes only [ServiceEventType.SERVICE_HARVESTED] and [ServiceEventType.SERVICE_REMOVED];
 * other types are acknowledged and skipped. Delegates to the circuit breaker and nacks on failure.
 */
@Component
class KafkaServiceEventConsumer(
    private val circuitBreaker: KafkaServiceEventCircuitBreaker,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["service-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeServiceEvent(
        record: ConsumerRecord<String, Any>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received service event - offset: {}, partition: {}", record.offset(), record.partition())

        try {
            circuitBreaker.process(record)
            ack.acknowledge()
        } catch (e: Exception) {
            ack.nack(Duration.ZERO)
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaServiceEventConsumer::class.java)
        const val LISTENER_ID = "service-archive"
        private const val TOPIC = "service-events"
    }
}
