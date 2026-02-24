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

@Component
class KafkaServiceEventConsumer(
    private val circuitBreaker: KafkaCircuitBreakerApi<ServiceEvent>,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["service-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeServiceEvent(
        record: ConsumerRecord<String, ServiceEvent>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received service event - offset: {}, partition: {}", record.offset(), record.partition())

        val event = record.value()

        if (event.type != ServiceEventType.SERVICE_HARVESTED && event.type != ServiceEventType.SERVICE_REMOVED) {
            LOGGER.debug("Skipping service event with type {}.", event.type)
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
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaServiceEventConsumer::class.java)
        const val LISTENER_ID = "service-archive"
    }
}
