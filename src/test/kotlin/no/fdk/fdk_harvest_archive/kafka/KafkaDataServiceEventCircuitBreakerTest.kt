package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
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
        org.apache.kafka.clients.consumer.ConsumerRecord("data-service-events", 0, 0L, "key", event as Any)

    @Test
    fun `process calls eventArchiveService saveDataService with event`() {
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/dataservice/1")
            .setFdkId("dataservice-123")
            .setGraph("<> a <http://example.org/DataService> .")
            .setTimestamp(1700000000000L)
            .build()
        every { eventArchiveService.saveDataService(any()) } returns Unit

        circuitBreaker.process(recordFor(event))

        verify(exactly = 1) { eventArchiveService.saveDataService(event) }
    }

    @Test
    fun `reasoned events are skipped`() {
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_REASONED)
            .setHarvestRunId("12")
            .setUri("https://dataservice.test")
            .setFdkId("test-dataservice-123")
            .setGraph("<http://example.org/dataservice/123>")
            .setTimestamp(123)
            .build()

        circuitBreaker.process(recordFor(event))

        verify(exactly = 0) { eventArchiveService.saveDataService(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveDataService throws`() {
        val event = DataServiceEvent.newBuilder()
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
    fun `unsupported value type is skipped and genericProcessor not called`() {
        val record = org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
            "data-service-events",
            0,
            0L,
            "key",
            listOf("unexpected"),
        )

        circuitBreaker.process(record)

        verify(exactly = 0) { eventArchiveService.saveDataService(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }
}
