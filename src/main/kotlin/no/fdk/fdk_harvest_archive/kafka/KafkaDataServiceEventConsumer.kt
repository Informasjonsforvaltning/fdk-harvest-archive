package no.fdk.fdk_harvest_archive.kafka

import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
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
 * Kafka listener for [DataServiceEvent] on topic `data-service-events`.
 * Processes only [DataServiceEventType.DATA_SERVICE_HARVESTED] and [DataServiceEventType.DATA_SERVICE_REMOVED];
 * other types are acknowledged and skipped. Delegates to the circuit breaker and nacks on failure.
 */
@Component
class KafkaDataServiceEventConsumer(
    private val circuitBreaker: KafkaCircuitBreakerApi<DataServiceEvent>,
    private val genericCircuitBreaker: KafkaGenericCircuitBreaker,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["data-service-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeDataServiceEvent(
        record: ConsumerRecord<String, Any>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received data service event - offset: {}, partition: {}", record.offset(), record.partition())

        val event = record.value()

        try {
            if (event is SpecificRecord) {
                circuitBreaker.process(event as DataServiceEvent)
            } else {
                genericCircuitBreaker.process(event as GenericRecord, TOPIC)
            }
            ack.acknowledge()
        } catch (e: Exception) {
            ack.nack(Duration.ZERO)
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaDataServiceEventConsumer::class.java)
        const val LISTENER_ID = "dataservice-archive"
        private const val TOPIC = "data-service-events"
    }
}
