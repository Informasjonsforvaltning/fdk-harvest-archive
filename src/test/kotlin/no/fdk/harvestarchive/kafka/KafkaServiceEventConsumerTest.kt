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
import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaServiceEventConsumerTest {
    private val circuitBreaker: KafkaServiceEventCircuitBreaker = mockk()
    private val registry = SimpleMeterRegistry()
    private val consumer = KafkaServiceEventConsumer(circuitBreaker, ArchiveMetrics(registry))
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaServiceEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeServiceEvent delegates record to circuit breaker and acknowledges`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        val record: ConsumerRecord<String, Any> = ConsumerRecord("service-events", 0, 0L, "key", genericRecord)

        every { circuitBreaker.process(any()) } returns ProcessOutcome.Saved(no.fdk.harvestarchive.archive.ArchiveType.SERVICE)

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
        registry.assertEventProcessed(ArchiveType.SERVICE, "acked")
    }

    @Test
    fun `consumeServiceEvent processes SERVICE_HARVESTED and acknowledges on success`() {
        val event =
            ServiceEvent
                .newBuilder()
                .setType(ServiceEventType.SERVICE_HARVESTED)
                .setHarvestRunId("12")
                .setUri("https://service.test")
                .setFdkId("test-service-123")
                .setGraph("<http://example.org/service/123> a <http://www.w3.org/ns/dcat#DataService> .")
                .setTimestamp(123)
                .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("service-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns ProcessOutcome.Saved(no.fdk.harvestarchive.archive.ArchiveType.SERVICE)

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeServiceEvent processes SERVICE_REMOVED and acknowledges on success`() {
        val event =
            ServiceEvent
                .newBuilder()
                .setType(ServiceEventType.SERVICE_REMOVED)
                .setHarvestRunId("12")
                .setUri("https://service.test")
                .setFdkId("test-service-123")
                .setGraph("")
                .setTimestamp(123)
                .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("service-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns ProcessOutcome.Saved(no.fdk.harvestarchive.archive.ArchiveType.SERVICE)

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeServiceEvent acknowledges skipped events`() {
        val record: ConsumerRecord<String, Any> = ConsumerRecord("service-events", 0, 0L, "key", "skip")
        every { circuitBreaker.process(any()) } returns ProcessOutcome.Skipped(ArchiveType.SERVICE, "unsupported_payload")

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
        registry.assertEventProcessed(ArchiveType.SERVICE, "skipped", "unsupported_payload")
    }

    @Test
    fun `consumeServiceEvent nacks on circuit breaker open`() {
        val record: ConsumerRecord<String, Any> = ConsumerRecord("service-events", 0, 0L, "key", "any")
        val cb = CircuitBreaker.ofDefaults("dummy")
        every { circuitBreaker.process(any()) } throws CallNotPermittedException.createCallNotPermittedException(cb)

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
        registry.assertEventProcessed(ArchiveType.SERVICE, "nacked", "circuit_open")
    }

    @Test
    fun `consumeServiceEvent nacks on processing error`() {
        val event =
            ServiceEvent
                .newBuilder()
                .setType(ServiceEventType.SERVICE_HARVESTED)
                .setHarvestRunId("12")
                .setUri("https://service.test")
                .setFdkId("test-service-123")
                .setGraph("<http://example.org/service/123> a <http://www.w3.org/ns/dcat#DataService> .")
                .setTimestamp(123)
                .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("service-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
        registry.assertEventProcessed(ArchiveType.SERVICE, "nacked", "processing_error")
    }
}
