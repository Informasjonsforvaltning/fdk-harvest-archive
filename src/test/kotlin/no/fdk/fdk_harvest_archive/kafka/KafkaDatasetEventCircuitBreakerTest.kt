package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.dataset.DatasetEvent
import no.fdk.dataset.DatasetEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaDatasetEventCircuitBreakerTest {

    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val genericProcessor = mockk<KafkaGenericProcessor>(relaxed = true)
    private val circuitBreaker = KafkaDatasetEventCircuitBreaker(eventArchiveService, genericProcessor)

    private fun recordFor(event: DatasetEvent): org.apache.kafka.clients.consumer.ConsumerRecord<String, Any> =
        org.apache.kafka.clients.consumer.ConsumerRecord("dataset-events", 0, 0L, "key", event as Any)

    @Test
    fun `process calls eventArchiveService saveDataset with event`() {
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/1")
            .setFdkId("fdk-123")
            .setGraph("<> a <http://example.org/Dataset> .")
            .setTimestamp(1700000000000L)
            .build()
        every { eventArchiveService.saveDataset(any()) } returns Unit

        circuitBreaker.process(recordFor(event))

        verify(exactly = 1) { eventArchiveService.saveDataset(event) }
    }

    @Test
    fun `reasoned events are skipped`() {
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_REASONED)
            .setHarvestRunId("12")
            .setUri("https://dataset.test")
            .setFdkId("test-dataset-123")
            .setGraph("<http://example.org/dataset/123>")
            .setTimestamp(123)
            .build()

        circuitBreaker.process(recordFor(event))

        verify(exactly = 0) { eventArchiveService.saveDataset(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveDataset throws`() {
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_REMOVED)
            .setFdkId("fail-id")
            .setGraph("")
            .setTimestamp(1L)
            .build()
        every { eventArchiveService.saveDataset(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(recordFor(event))
        }

        verify(exactly = 1) { eventArchiveService.saveDataset(event) }
    }

    @Test
    fun `unsupported value type is skipped and genericProcessor not called`() {
        val record = org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
            "dataset-events",
            0,
            0L,
            "key",
            "not-an-event",
        )

        circuitBreaker.process(record)

        verify(exactly = 0) { eventArchiveService.saveDataset(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }
}
