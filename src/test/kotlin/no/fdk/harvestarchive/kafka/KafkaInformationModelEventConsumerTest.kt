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
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaInformationModelEventConsumerTest {
    private val circuitBreaker: KafkaInformationModelEventCircuitBreaker = mockk()
    private val registry = SimpleMeterRegistry()
    private val consumer = KafkaInformationModelEventConsumer(circuitBreaker, ArchiveMetrics(registry))
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaInformationModelEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeInformationModelEvent delegates record to circuit breaker and acknowledges`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", genericRecord)

        every { circuitBreaker.process(any()) } returns ProcessOutcome.Saved(no.fdk.harvestarchive.archive.ArchiveType.INFORMATION_MODEL)

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
        registry.assertEventProcessed(ArchiveType.INFORMATION_MODEL, "acked")
    }

    @Test
    fun `consumeInformationModelEvent processes INFORMATION_MODEL_HARVESTED and acknowledges on success`() {
        val event =
            InformationModelEvent
                .newBuilder()
                .setType(InformationModelEventType.INFORMATION_MODEL_HARVESTED)
                .setHarvestRunId("12")
                .setUri("https://informationmodel.test")
                .setFdkId("test-informationmodel-123")
                .setGraph("<http://example.org/informationmodel/123> a <http://www.w3.org/ns/dcat#Dataset> .")
                .setTimestamp(123)
                .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns ProcessOutcome.Saved(no.fdk.harvestarchive.archive.ArchiveType.INFORMATION_MODEL)

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeInformationModelEvent processes INFORMATION_MODEL_REMOVED and acknowledges on success`() {
        val event =
            InformationModelEvent
                .newBuilder()
                .setType(InformationModelEventType.INFORMATION_MODEL_REMOVED)
                .setHarvestRunId("12")
                .setUri("https://informationmodel.test")
                .setFdkId("test-informationmodel-123")
                .setGraph("")
                .setTimestamp(123)
                .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns ProcessOutcome.Saved(no.fdk.harvestarchive.archive.ArchiveType.INFORMATION_MODEL)

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeInformationModelEvent acknowledges skipped events`() {
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", "skip")
        every { circuitBreaker.process(any()) } returns ProcessOutcome.Skipped(ArchiveType.INFORMATION_MODEL)

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
        registry.assertEventProcessed(ArchiveType.INFORMATION_MODEL, "skipped")
    }

    @Test
    fun `consumeInformationModelEvent nacks on circuit breaker open`() {
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", "any")
        val cb = CircuitBreaker.ofDefaults("dummy")
        every { circuitBreaker.process(any()) } throws CallNotPermittedException.createCallNotPermittedException(cb)

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
        registry.assertEventProcessed(ArchiveType.INFORMATION_MODEL, "circuit_open")
    }

    @Test
    fun `consumeInformationModelEvent nacks on processing error`() {
        val event =
            InformationModelEvent
                .newBuilder()
                .setType(InformationModelEventType.INFORMATION_MODEL_HARVESTED)
                .setHarvestRunId("12")
                .setUri("https://informationmodel.test")
                .setFdkId("test-informationmodel-123")
                .setGraph("<http://example.org/informationmodel/123> a <http://www.w3.org/ns/dcat#Dataset> .")
                .setTimestamp(123)
                .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
        registry.assertEventProcessed(ArchiveType.INFORMATION_MODEL, "nacked")
    }
}
