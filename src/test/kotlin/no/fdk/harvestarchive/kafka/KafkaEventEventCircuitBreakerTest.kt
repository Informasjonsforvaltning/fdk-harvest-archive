package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaEventEventCircuitBreakerTest {
    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val genericProcessor = mockk<KafkaGenericProcessor>(relaxed = true)
    private val circuitBreakerRegistration: CircuitBreaker = CircuitBreaker.ofDefaults("test-event-cb")
    private val circuitBreaker = KafkaEventEventCircuitBreaker(eventArchiveService, genericProcessor, circuitBreakerRegistration)

    private fun recordFor(event: EventEvent): org.apache.kafka.clients.consumer.ConsumerRecord<String, Any> =
        org.apache.kafka.clients.consumer
            .ConsumerRecord("event-events", 0, 0L, "key", event as Any)

    @Test
    fun `process calls eventArchiveService saveEvent with event and returns Saved`() {
        val event =
            EventEvent
                .newBuilder()
                .setType(EventEventType.EVENT_HARVESTED)
                .setHarvestRunId("run-1")
                .setUri("https://example.com/event/1")
                .setFdkId("event-123")
                .setGraph("<> a <http://example.org/Event> .")
                .setTimestamp(1700000000000L)
                .build()
        every { eventArchiveService.saveEvent(any()) } returns Unit

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.EVENT))
        verify(exactly = 1) { eventArchiveService.saveEvent(event) }
    }

    @Test
    fun `reasoned events are skipped and return Skipped`() {
        val event =
            EventEvent
                .newBuilder()
                .setType(EventEventType.EVENT_REASONED)
                .setHarvestRunId("12")
                .setUri("https://event.test")
                .setFdkId("test-event-123")
                .setGraph("<http://example.org/event/123>")
                .setTimestamp(123)
                .build()

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.EVENT, "unsupported_event_type"))
        verify(exactly = 0) { eventArchiveService.saveEvent(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveEvent throws`() {
        val event =
            EventEvent
                .newBuilder()
                .setType(EventEventType.EVENT_REMOVED)
                .setFdkId("fail-id")
                .setGraph("")
                .setTimestamp(1L)
                .build()
        every { eventArchiveService.saveEvent(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(recordFor(event))
        }

        verify(exactly = 1) { eventArchiveService.saveEvent(event) }
    }

    @Test
    fun `unsupported value type returns Skipped`() {
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                "event-events",
                0,
                0L,
                "key",
                3.14,
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.EVENT, "unsupported_payload"))
        verify(exactly = 0) { eventArchiveService.saveEvent(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }

    @Test
    fun `generic harvested record returns Saved from generic processor`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericProcessor.process(genericRecord, ArchiveType.EVENT.topicName) } returns
            ProcessOutcome.Saved(ArchiveType.EVENT)
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                ArchiveType.TOPIC_EVENT,
                0,
                0L,
                "key",
                genericRecord,
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.EVENT))
        verify(exactly = 1) { genericProcessor.process(genericRecord, ArchiveType.EVENT.topicName) }
        verify(exactly = 0) { eventArchiveService.saveEvent(any()) }
    }
}
