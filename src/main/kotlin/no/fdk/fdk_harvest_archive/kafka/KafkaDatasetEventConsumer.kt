package no.fdk.fdk_harvest_archive.kafka

import no.fdk.dataset.DatasetEvent
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
 * Kafka listener for [DatasetEvent] on topic `dataset-events`.
 * Processes only [no.fdk.dataset.DatasetEventType.DATASET_HARVESTED] and [no.fdk.dataset.DatasetEventType.DATASET_REMOVED];
 * other types are acknowledged and skipped. Delegates to the circuit breaker and nacks on failure.
 */
@Component
class KafkaDatasetEventConsumer(
    private val circuitBreaker: KafkaCircuitBreakerApi<DatasetEvent>,
    private val genericCircuitBreaker: KafkaGenericCircuitBreaker,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["dataset-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeDatasetEvent(
        record: ConsumerRecord<String, Any>,
        ack: Acknowledgment,
    ) {
        logger().debug("Received harvest event - offset: {}, partition: {}", record.offset(), record.partition())

        val event = record.value()

        try {
            if (event is SpecificRecord) {
                circuitBreaker.process(event as DatasetEvent)
            } else {
                genericCircuitBreaker.process(event as GenericRecord, TOPIC)
            }
            ack.acknowledge()
        } catch (e: Exception) {
            ack.nack(Duration.ZERO)
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaDatasetEventConsumer::class.java)
        const val LISTENER_ID = "dataset-archive"
        private const val TOPIC = "dataset-events"
    }
}
