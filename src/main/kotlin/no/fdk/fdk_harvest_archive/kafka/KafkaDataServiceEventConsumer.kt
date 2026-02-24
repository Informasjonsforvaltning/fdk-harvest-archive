package no.fdk.fdk_harvest_archive.kafka

import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class KafkaDataServiceEventConsumer(
    private val circuitBreaker: KafkaCircuitBreakerApi<DataServiceEvent>,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["data-service-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeDataServiceEvent(
        record: ConsumerRecord<String, DataServiceEvent>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received data service event - offset: {}, partition: {}", record.offset(), record.partition())

        val event = record.value()

        if (event.type != DataServiceEventType.DATA_SERVICE_HARVESTED && event.type != DataServiceEventType.DATA_SERVICE_REMOVED) {
            LOGGER.debug("Skipping data service event with type {}.", event.type)
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
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaDataServiceEventConsumer::class.java)
        const val LISTENER_ID = "dataservice-archive"
    }
}
