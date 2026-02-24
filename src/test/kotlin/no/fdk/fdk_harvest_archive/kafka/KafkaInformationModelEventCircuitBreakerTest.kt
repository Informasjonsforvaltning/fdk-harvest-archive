package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaInformationModelEventCircuitBreakerTest {

    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val circuitBreaker = KafkaInformationModelEventCircuitBreaker(eventArchiveService)

    @Test
    fun `process calls eventArchiveService saveInformationModel with record value`() {
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/informationmodel/1")
            .setFdkId("informationmodel-123")
            .setGraph("<> a <http://example.org/InformationModel> .")
            .setTimestamp(1700000000000L)
            .build()
        val record = ConsumerRecord<String, InformationModelEvent>("information-model-events", 0, 42L, "informationmodel-123", event)

        every { eventArchiveService.saveInformationModel(any()) } returns Unit

        circuitBreaker.process(record)

        verify(exactly = 1) { eventArchiveService.saveInformationModel(event) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveInformationModel throws`() {
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_REMOVED)
            .setFdkId("fail-id")
            .setGraph("")
            .setTimestamp(1L)
            .build()
        val record = ConsumerRecord<String, InformationModelEvent>("information-model-events", 1, 0L, "fail-id", event)

        every { eventArchiveService.saveInformationModel(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(record)
        }

        verify(exactly = 1) { eventArchiveService.saveInformationModel(event) }
    }
}
