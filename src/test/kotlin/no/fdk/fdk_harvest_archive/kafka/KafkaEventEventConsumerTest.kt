package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaEventEventConsumerTest {

    private val circuitBreaker: KafkaEventEventCircuitBreaker = mockk()
    private val genericCircuitBreaker: KafkaGenericCircuitBreaker = mockk(relaxed = true)
    private val consumer = KafkaEventEventConsumer(circuitBreaker, genericCircuitBreaker)
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaEventEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeEventEvent delegates generic record to generic circuit breaker with topic and acknowledges`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        val record: ConsumerRecord<String, Any> = ConsumerRecord("event-events", 0, 0L, "key", genericRecord)

        every { genericCircuitBreaker.process(any(), any()) } returns Unit

        consumer.consumeEventEvent(record, ack)

        verify(exactly = 1) { genericCircuitBreaker.process(genericRecord, "event-events") }
        verify(exactly = 0) { circuitBreaker.process(any<EventEvent>()) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeEventEvent processes EVENT_HARVESTED and acknowledges on success`() {
        val event = EventEvent.newBuilder()
            .setType(EventEventType.EVENT_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://event.test")
            .setFdkId("test-event-123")
            .setGraph("<http://example.org/event/123> a <http://schema.org/Event> .")
            .setTimestamp(123)
            .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("event-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeEventEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(event) }
        verify(exactly = 0) { genericCircuitBreaker.process(any(), "event-events") }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeEventEvent processes EVENT_REMOVED and acknowledges on success`() {
        val event = EventEvent.newBuilder()
            .setType(EventEventType.EVENT_REMOVED)
            .setHarvestRunId("12")
            .setUri("https://event.test")
            .setFdkId("test-event-123")
            .setGraph("")
            .setTimestamp(123)
            .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("event-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeEventEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(event) }
        verify(exactly = 0) { genericCircuitBreaker.process(any(), "event-events") }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeEventEvent nacks on processing error`() {
        val event = EventEvent.newBuilder()
            .setType(EventEventType.EVENT_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://event.test")
            .setFdkId("test-event-123")
            .setGraph("<http://example.org/event/123> a <http://schema.org/Event> .")
            .setTimestamp(123)
            .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("event-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeEventEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(event) }
        verify(exactly = 0) { genericCircuitBreaker.process(any(), "event-events") }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
    }
}
