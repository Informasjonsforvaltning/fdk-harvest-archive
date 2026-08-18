package no.fdk.harvestarchive.kafka

import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.ArchiveWrite
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Interface for circuit-breaker-wrapped Kafka record processors.
 * Returns a [ProcessOutcome] so consumers can distinguish saved vs skipped events for metrics.
 */
interface KafkaCircuitBreakerApi {
    fun process(record: ConsumerRecord<String, Any>): ProcessOutcome
}

sealed class ProcessOutcome(open val archiveType: ArchiveType?) {
    data class Saved(override val archiveType: ArchiveType) : ProcessOutcome(archiveType)

    data class Skipped(override val archiveType: ArchiveType?) : ProcessOutcome(archiveType)

    companion object {
        fun from(write: ArchiveWrite): ProcessOutcome = when (write) {
            is ArchiveWrite.Saved -> Saved(write.archiveType)
            is ArchiveWrite.Skipped -> Skipped(write.archiveType)
        }
    }
}
