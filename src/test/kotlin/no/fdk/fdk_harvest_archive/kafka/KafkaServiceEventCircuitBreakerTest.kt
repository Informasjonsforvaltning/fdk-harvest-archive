package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaServiceEventCircuitBreakerTest {

    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val circuitBreaker = KafkaServiceEventCircuitBreaker(eventArchiveService)

    @Test
    fun `process calls eventArchiveService saveService with event`() {
        val event = ServiceEvent.newBuilder()
            .setType(ServiceEventType.SERVICE_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/service/1")
            .setFdkId("service-123")
            .setGraph("<> a <http://example.org/Service> .")
            .setTimestamp(1700000000000L)
            .build()
        every { eventArchiveService.saveService(any()) } returns Unit

        circuitBreaker.process(event)

        verify(exactly = 1) { eventArchiveService.saveService(event) }
    }

    @Test
    fun `reasoned events are skipped`() {
        val event = ServiceEvent.newBuilder()
            .setType(ServiceEventType.SERVICE_REASONED)
            .setHarvestRunId("12")
            .setUri("https://service.test")
            .setFdkId("test-service-123")
            .setGraph("<http://example.org/service/123>")
            .setTimestamp(123)
            .build()

        circuitBreaker.process(event)

        verify(exactly = 0) { eventArchiveService.saveService(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveService throws`() {
        val event = ServiceEvent.newBuilder()
            .setType(ServiceEventType.SERVICE_REMOVED)
            .setFdkId("fail-id")
            .setGraph("")
            .setTimestamp(1L)
            .build()
        every { eventArchiveService.saveService(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(event)
        }

        verify(exactly = 1) { eventArchiveService.saveService(event) }
    }
}
