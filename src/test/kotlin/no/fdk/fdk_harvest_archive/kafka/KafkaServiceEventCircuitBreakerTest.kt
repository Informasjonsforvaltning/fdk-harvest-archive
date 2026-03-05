package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
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
    private val genericProcessor = mockk<KafkaGenericProcessor>(relaxed = true)
    private val circuitBreakerRegistration: CircuitBreaker = CircuitBreaker.ofDefaults("test-service-cb")
    private val circuitBreaker = KafkaServiceEventCircuitBreaker(eventArchiveService, genericProcessor, circuitBreakerRegistration)

    private fun recordFor(event: ServiceEvent): org.apache.kafka.clients.consumer.ConsumerRecord<String, Any> =
        org.apache.kafka.clients.consumer.ConsumerRecord("service-events", 0, 0L, "key", event as Any)

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

        circuitBreaker.process(recordFor(event))

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

        circuitBreaker.process(recordFor(event))

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
            circuitBreaker.process(recordFor(event))
        }

        verify(exactly = 1) { eventArchiveService.saveService(event) }
    }

    @Test
    fun `unsupported value type is skipped and genericProcessor not called`() {
        val record = org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
            "service-events",
            0,
            0L,
            "key",
            true,
        )

        circuitBreaker.process(record)

        verify(exactly = 0) { eventArchiveService.saveService(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }
}
