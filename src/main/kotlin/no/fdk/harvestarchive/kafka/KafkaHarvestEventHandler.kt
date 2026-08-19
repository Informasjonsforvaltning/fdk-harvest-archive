package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import no.fdk.harvestarchive.metrics.ArchiveMetrics.EventProcessingResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
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
            when (outcome) {
                is ProcessOutcome.Saved -> {
                    ack.acknowledge()
                    archiveMetrics.recordEventProcessed(outcome.archiveType, EventProcessingResult.ACKED)
                }

                is ProcessOutcome.Skipped -> {
                    ack.acknowledge()
                    archiveMetrics.recordEventProcessed(outcome.archiveType, EventProcessingResult.SKIPPED, outcome.reason)
                }
            }
        } catch (_: CallNotPermittedException) {
            ack.nack(Duration.ZERO)
            archiveMetrics.recordEventProcessed(archiveType, EventProcessingResult.NACKED, "circuit_open")
        } catch (e: Exception) {
            LOGGER.error("Error processing harvest event", e)
            ack.nack(Duration.ZERO)
            archiveMetrics.recordEventProcessed(archiveType, EventProcessingResult.NACKED, "processing_error")
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(KafkaHarvestEventHandler::class.java)
    }
}
