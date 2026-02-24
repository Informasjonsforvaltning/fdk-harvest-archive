package no.fdk.fdk_harvest_archive.kafka

import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class KafkaInformationModelEventConsumer(
    private val circuitBreaker: KafkaCircuitBreakerApi<InformationModelEvent>,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["informationmodel-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeInformationModelEvent(
        record: ConsumerRecord<String, InformationModelEvent>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received information model event - offset: {}, partition: {}", record.offset(), record.partition())

        val event = record.value()

        if (event.type != InformationModelEventType.INFORMATION_MODEL_HARVESTED && event.type != InformationModelEventType.INFORMATION_MODEL_REMOVED) {
            LOGGER.debug("Skipping information model event with type {}.", event.type)
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
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaInformationModelEventConsumer::class.java)
        const val LISTENER_ID = "informationmodel-archive"
    }
}
