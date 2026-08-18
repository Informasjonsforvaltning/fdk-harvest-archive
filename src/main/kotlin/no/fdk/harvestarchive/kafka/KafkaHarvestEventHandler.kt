package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import no.fdk.harvestarchive.metrics.ArchiveMetrics.EventProcessingResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

internal class KafkaHarvestEventHandler(
    private val circuitBreaker: KafkaCircuitBreakerApi,
    private val archiveMetrics: ArchiveMetrics,
    private val archiveType: ArchiveType,
) {
    fun process(record: ConsumerRecord<String, Any>, ack: Acknowledgment) {
        try {
            val outcome = circuitBreaker.process(record)
            val result = when (outcome) {
                is ProcessOutcome.Saved -> EventProcessingResult.ACKED
                is ProcessOutcome.Skipped -> EventProcessingResult.SKIPPED
            }
            archiveMetrics.recordEventProcessed(outcome.archiveType, result)
            ack.acknowledge()
        } catch (_: CallNotPermittedException) {
            archiveMetrics.recordEventProcessed(archiveType, EventProcessingResult.CIRCUIT_OPEN)
            ack.nack(Duration.ZERO)
        } catch (_: Exception) {
            archiveMetrics.recordEventProcessed(archiveType, EventProcessingResult.NACKED)
            ack.nack(Duration.ZERO)
        }
    }
}
