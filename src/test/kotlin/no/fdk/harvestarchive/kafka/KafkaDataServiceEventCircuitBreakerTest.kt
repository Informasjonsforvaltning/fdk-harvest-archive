package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaDataServiceEventCircuitBreakerTest {
    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val genericProcessor = mockk<KafkaGenericProcessor>(relaxed = true)
    private val circuitBreakerRegistration: CircuitBreaker = CircuitBreaker.ofDefaults("test-dataservice-cb")
    private val circuitBreaker = KafkaDataServiceEventCircuitBreaker(eventArchiveService, genericProcessor, circuitBreakerRegistration)

    private fun recordFor(event: DataServiceEvent): org.apache.kafka.clients.consumer.ConsumerRecord<String, Any> =
        org.apache.kafka.clients.consumer
            .ConsumerRecord("data-service-events", 0, 0L, "key", event as Any)

    @Test
    fun `process calls eventArchiveService saveDataService with event and returns Saved`() {
        val event =
            DataServiceEvent
                .newBuilder()
                .setType(DataServiceEventType.DATA_SERVICE_HARVESTED)
                .setHarvestRunId("run-1")
                .setUri("https://example.com/dataservice/1")
                .setFdkId("dataservice-123")
                .setGraph("<> a <http://example.org/DataService> .")
                .setTimestamp(1700000000000L)
                .build()
        every { eventArchiveService.saveDataService(any()) } returns Unit

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.DATA_SERVICE))
        verify(exactly = 1) { eventArchiveService.saveDataService(event) }
    }

    @Test
    fun `reasoned events are skipped and return Skipped`() {
        val event =
            DataServiceEvent
                .newBuilder()
                .setType(DataServiceEventType.DATA_SERVICE_REASONED)
                .setHarvestRunId("12")
                .setUri("https://dataservice.test")
                .setFdkId("test-dataservice-123")
                .setGraph("<http://example.org/dataservice/123>")
                .setTimestamp(123)
                .build()

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.DATA_SERVICE))
        verify(exactly = 0) { eventArchiveService.saveDataService(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveDataService throws`() {
        val event =
            DataServiceEvent
                .newBuilder()
                .setType(DataServiceEventType.DATA_SERVICE_REMOVED)
                .setFdkId("fail-id")
                .setGraph("")
                .setTimestamp(1L)
                .build()
        every { eventArchiveService.saveDataService(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(recordFor(event))
        }

        verify(exactly = 1) { eventArchiveService.saveDataService(event) }
    }

    @Test
    fun `unsupported value type returns Skipped`() {
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                "data-service-events",
                0,
                0L,
                "key",
                listOf("unexpected"),
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.DATA_SERVICE))
        verify(exactly = 0) { eventArchiveService.saveDataService(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }
}
