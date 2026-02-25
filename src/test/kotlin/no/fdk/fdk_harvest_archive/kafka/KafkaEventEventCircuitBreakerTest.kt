package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaEventEventCircuitBreakerTest {

    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val circuitBreaker = KafkaEventEventCircuitBreaker(eventArchiveService)

    @Test
    fun `process calls eventArchiveService saveEvent with event`() {
        val event = EventEvent.newBuilder()
            .setType(EventEventType.EVENT_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/event/1")
            .setFdkId("event-123")
            .setGraph("<> a <http://example.org/Event> .")
            .setTimestamp(1700000000000L)
            .build()
        every { eventArchiveService.saveEvent(any()) } returns Unit

        circuitBreaker.process(event)

        verify(exactly = 1) { eventArchiveService.saveEvent(event) }
    }

    @Test
    fun `reasoned events are skipped`() {
        val event = EventEvent.newBuilder()
            .setType(EventEventType.EVENT_REASONED)
            .setHarvestRunId("12")
            .setUri("https://event.test")
            .setFdkId("test-event-123")
            .setGraph("<http://example.org/event/123>")
            .setTimestamp(123)
            .build()

        circuitBreaker.process(event)

        verify(exactly = 0) { eventArchiveService.saveEvent(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveEvent throws`() {
        val event = EventEvent.newBuilder()
            .setType(EventEventType.EVENT_REMOVED)
            .setFdkId("fail-id")
            .setGraph("")
            .setTimestamp(1L)
            .build()
        every { eventArchiveService.saveEvent(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(event)
        }

        verify(exactly = 1) { eventArchiveService.saveEvent(event) }
    }
}
