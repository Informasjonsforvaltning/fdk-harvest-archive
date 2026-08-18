package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import no.fdk.harvestarchive.metrics.ArchiveMetrics.EventProcessingResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class KafkaConceptEventConsumer(
    @param:Qualifier("kafkaConceptEventCircuitBreaker")
    private val circuitBreaker: KafkaCircuitBreakerApi,
    private val archiveMetrics: ArchiveMetrics,
) {
    private fun logger(): Logger = LOGGER

    @KafkaListener(
        topics = ["concept-events"],
        groupId = "fdk-harvest-archive",
        containerFactory = "kafkaListenerContainerFactory",
        id = LISTENER_ID,
    )
    fun consumeConceptEvent(record: ConsumerRecord<String, Any>, ack: Acknowledgment) {
        logger().debug("Received concept event - offset: {}, partition: {}", record.offset(), record.partition())

        try {
            val outcome = circuitBreaker.process(record)
            val result = when (outcome) {
                is ProcessOutcome.Saved -> EventProcessingResult.ACKED
                is ProcessOutcome.Skipped -> EventProcessingResult.SKIPPED
            }
            archiveMetrics.recordEventProcessed(outcome.archiveType, result)
            ack.acknowledge()
        } catch (e: CallNotPermittedException) {
            archiveMetrics.recordEventProcessed(ARCHIVE_TYPE, EventProcessingResult.CIRCUIT_OPEN)
            ack.nack(Duration.ZERO)
        } catch (e: Exception) {
            archiveMetrics.recordEventProcessed(ARCHIVE_TYPE, EventProcessingResult.NACKED)
            ack.nack(Duration.ZERO)
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaConceptEventConsumer::class.java)
        private val ARCHIVE_TYPE = ArchiveType.CONCEPT
        const val LISTENER_ID = "concept-archive"
    }
}
