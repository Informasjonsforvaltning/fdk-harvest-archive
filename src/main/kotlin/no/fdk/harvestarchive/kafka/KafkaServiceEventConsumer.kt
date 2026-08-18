package no.fdk.harvestarchive.kafka

import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class KafkaServiceEventConsumer(
    @param:Qualifier("kafkaServiceEventCircuitBreaker")
    circuitBreaker: KafkaCircuitBreakerApi,
    archiveMetrics: ArchiveMetrics,
) {
    private val handler = KafkaHarvestEventHandler(circuitBreaker, archiveMetrics, ArchiveType.SERVICE)

    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = [ArchiveType.TOPIC_SERVICE],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = ArchiveType.LISTENER_SERVICE,
    )
    fun consumeServiceEvent(record: ConsumerRecord<String, Any>, ack: Acknowledgment) {
        logger().debug("Received service event - offset: {}, partition: {}", record.offset(), record.partition())
        handler.process(record, ack)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaServiceEventConsumer::class.java)
    }
}
