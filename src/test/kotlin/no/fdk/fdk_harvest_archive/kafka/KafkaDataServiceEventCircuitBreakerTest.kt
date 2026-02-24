package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaDataServiceEventCircuitBreakerTest {

    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val circuitBreaker = KafkaDataServiceEventCircuitBreaker(eventArchiveService)

    @Test
    fun `process calls eventArchiveService saveDataService with record value`() {
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/dataservice/1")
            .setFdkId("dataservice-123")
            .setGraph("<> a <http://example.org/DataService> .")
            .setTimestamp(1700000000000L)
            .build()
        val record = ConsumerRecord<String, DataServiceEvent>("data-service-events", 0, 42L, "dataservice-123", event)

        every { eventArchiveService.saveDataService(any()) } returns Unit

        circuitBreaker.process(record)

        verify(exactly = 1) { eventArchiveService.saveDataService(event) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveDataService throws`() {
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_REMOVED)
            .setFdkId("fail-id")
            .setGraph("")
            .setTimestamp(1L)
            .build()
        val record = ConsumerRecord<String, DataServiceEvent>("data-service-events", 1, 0L, "fail-id", event)

        every { eventArchiveService.saveDataService(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(record)
        }

        verify(exactly = 1) { eventArchiveService.saveDataService(event) }
    }
}
