package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import no.fdk.harvestarchive.metrics.assertEventProcessed
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaHarvestEventHandlerTest {
    private val circuitBreaker = mockk<KafkaCircuitBreakerApi>()
    private val registry = SimpleMeterRegistry()
    private val handler = KafkaHarvestEventHandler(circuitBreaker, ArchiveMetrics(registry), ArchiveType.DATASET)
    private val ack: Acknowledgment = mockk(relaxed = true)
    private val record: ConsumerRecord<String, Any> = ConsumerRecord("dataset-events", 0, 0L, "key", "value")

    @Test
    fun `acknowledge failure is counted as nacked not acked`() {
        every { circuitBreaker.process(any()) } returns ProcessOutcome.Saved(ArchiveType.DATASET)
        every { ack.acknowledge() } throws RuntimeException("ack failed")

        handler.process(record, ack)

        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        registry.assertEventProcessed(ArchiveType.DATASET, "nacked", "processing_error")
        registry.assertEventProcessed(ArchiveType.DATASET, "acked", count = 0.0)
    }

    @Test
    fun `circuit open nacks before recording circuit_open`() {
        val cb = CircuitBreaker.ofDefaults("dummy")
        every { circuitBreaker.process(any()) } throws CallNotPermittedException.createCallNotPermittedException(cb)

        handler.process(record, ack)

        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
        registry.assertEventProcessed(ArchiveType.DATASET, "nacked", "circuit_open")
    }
}
