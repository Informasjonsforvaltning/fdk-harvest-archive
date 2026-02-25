package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaInformationModelEventCircuitBreakerTest {

    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val circuitBreaker = KafkaInformationModelEventCircuitBreaker(eventArchiveService)

    @Test
    fun `process calls eventArchiveService saveInformationModel with event`() {
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/informationmodel/1")
            .setFdkId("informationmodel-123")
            .setGraph("<> a <http://example.org/InformationModel> .")
            .setTimestamp(1700000000000L)
            .build()
        every { eventArchiveService.saveInformationModel(any()) } returns Unit

        circuitBreaker.process(event)

        verify(exactly = 1) { eventArchiveService.saveInformationModel(event) }
    }

    @Test
    fun `reasoned events are skipped`() {
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_REASONED)
            .setHarvestRunId("12")
            .setUri("https://informationmodel.test")
            .setFdkId("test-informationmodel-123")
            .setGraph("<http://example.org/informationmodel/123>")
            .setTimestamp(123)
            .build()

        circuitBreaker.process(event)

        verify(exactly = 0) { eventArchiveService.saveInformationModel(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveInformationModel throws`() {
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_REMOVED)
            .setFdkId("fail-id")
            .setGraph("")
            .setTimestamp(1L)
            .build()
        every { eventArchiveService.saveInformationModel(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(event)
        }

        verify(exactly = 1) { eventArchiveService.saveInformationModel(event) }
    }
}
