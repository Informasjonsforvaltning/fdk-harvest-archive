package no.fdk.fdk_harvest_archive.kafka

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
    private val circuitBreaker = KafkaDataServiceEventCircuitBreaker(eventArchiveService)

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

        circuitBreaker.process(event)

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

        circuitBreaker.process(event)

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
            circuitBreaker.process(event)
        }

        verify(exactly = 1) { eventArchiveService.saveDataService(event) }
    }
}
