package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
import org.apache.avro.generic.GenericRecord
import org.assertj.core.api.Assertions.assertThat
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
        org.apache.kafka.clients.consumer
            .ConsumerRecord("service-events", 0, 0L, "key", event as Any)

    @Test
    fun `process calls eventArchiveService saveService with event and returns Saved`() {
        val event =
            ServiceEvent
                .newBuilder()
                .setType(ServiceEventType.SERVICE_HARVESTED)
                .setHarvestRunId("run-1")
                .setUri("https://example.com/service/1")
                .setFdkId("service-123")
                .setGraph("<> a <http://example.org/Service> .")
                .setTimestamp(1700000000000L)
                .build()
        every { eventArchiveService.saveService(any()) } returns Unit

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.SERVICE))
        verify(exactly = 1) { eventArchiveService.saveService(event) }
    }

    @Test
    fun `reasoned events are skipped and return Skipped`() {
        val event =
            ServiceEvent
                .newBuilder()
                .setType(ServiceEventType.SERVICE_REASONED)
                .setHarvestRunId("12")
                .setUri("https://service.test")
                .setFdkId("test-service-123")
                .setGraph("<http://example.org/service/123>")
                .setTimestamp(123)
                .build()

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.SERVICE))
        verify(exactly = 0) { eventArchiveService.saveService(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveService throws`() {
        val event =
            ServiceEvent
                .newBuilder()
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
    fun `unsupported value type returns Skipped`() {
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                "service-events",
                0,
                0L,
                "key",
                true,
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.SERVICE))
        verify(exactly = 0) { eventArchiveService.saveService(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }

    @Test
    fun `generic harvested record returns Saved from generic processor`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericProcessor.process(genericRecord, ArchiveType.SERVICE.topicName) } returns
            ProcessOutcome.Saved(ArchiveType.SERVICE)
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                ArchiveType.TOPIC_SERVICE,
                0,
                0L,
                "key",
                genericRecord,
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.SERVICE))
        verify(exactly = 1) { genericProcessor.process(genericRecord, ArchiveType.SERVICE.topicName) }
        verify(exactly = 0) { eventArchiveService.saveService(any()) }
    }
}
