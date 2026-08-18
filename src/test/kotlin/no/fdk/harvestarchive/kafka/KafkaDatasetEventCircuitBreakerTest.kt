package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.dataset.DatasetEvent
import no.fdk.dataset.DatasetEventType
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaDatasetEventCircuitBreakerTest {
    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val genericProcessor = mockk<KafkaGenericProcessor>(relaxed = true)
    private val circuitBreakerRegistration: CircuitBreaker = CircuitBreaker.ofDefaults("test-dataset-cb")
    private val circuitBreaker = KafkaDatasetEventCircuitBreaker(eventArchiveService, genericProcessor, circuitBreakerRegistration)

    private fun recordFor(event: DatasetEvent): org.apache.kafka.clients.consumer.ConsumerRecord<String, Any> =
        org.apache.kafka.clients.consumer
            .ConsumerRecord("dataset-events", 0, 0L, "key", event as Any)

    @Test
    fun `process calls eventArchiveService saveDataset with event and returns Saved`() {
        val event =
            DatasetEvent
                .newBuilder()
                .setType(DatasetEventType.DATASET_HARVESTED)
                .setHarvestRunId("run-1")
                .setUri("https://example.com/1")
                .setFdkId("fdk-123")
                .setGraph("<> a <http://example.org/Dataset> .")
                .setTimestamp(1700000000000L)
                .build()
        every { eventArchiveService.saveDataset(any()) } returns Unit

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.DATASET))
        verify(exactly = 1) { eventArchiveService.saveDataset(event) }
    }

    @Test
    fun `reasoned events are skipped and return Skipped`() {
        val event =
            DatasetEvent
                .newBuilder()
                .setType(DatasetEventType.DATASET_REASONED)
                .setHarvestRunId("12")
                .setUri("https://dataset.test")
                .setFdkId("test-dataset-123")
                .setGraph("<http://example.org/dataset/123>")
                .setTimestamp(123)
                .build()

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.DATASET))
        verify(exactly = 0) { eventArchiveService.saveDataset(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveDataset throws`() {
        val event =
            DatasetEvent
                .newBuilder()
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
    fun `unsupported value type returns Skipped`() {
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                "dataset-events",
                0,
                0L,
                "key",
                "not-an-event",
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.DATASET))
        verify(exactly = 0) { eventArchiveService.saveDataset(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }

    @Test
    fun `generic DATASET_REASONED record returns Skipped not Saved`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericRecord.get("type") } returns DatasetEventType.DATASET_REASONED.name
        every { genericRecord.get("fdkId") } returns "test-dataset-123"
        every { genericRecord.get("timestamp") } returns 123L
        every { eventArchiveService.saveGenericForTopic(any(), any()) } returns ProcessOutcome.Skipped(ArchiveType.DATASET)

        val processor = KafkaGenericProcessor(eventArchiveService)
        val breaker = KafkaDatasetEventCircuitBreaker(eventArchiveService, processor, circuitBreakerRegistration)
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                "dataset-events",
                0,
                0L,
                "key",
                genericRecord,
            )

        val outcome = breaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.DATASET))
        verify(exactly = 0) { eventArchiveService.saveDataset(any()) }
    }

    @Test
    fun `generic harvested record returns Saved from generic processor`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericProcessor.process(genericRecord, ArchiveType.DATASET.topicName) } returns
            ProcessOutcome.Saved(ArchiveType.DATASET)
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                ArchiveType.TOPIC_DATASET,
                0,
                0L,
                "key",
                genericRecord,
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.DATASET))
        verify(exactly = 1) { genericProcessor.process(genericRecord, ArchiveType.DATASET.topicName) }
        verify(exactly = 0) { eventArchiveService.saveDataset(any()) }
    }
}
