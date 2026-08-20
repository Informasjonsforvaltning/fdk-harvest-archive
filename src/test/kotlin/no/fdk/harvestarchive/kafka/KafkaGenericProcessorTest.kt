package no.fdk.harvestarchive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.ArchiveWrite
import no.fdk.harvestarchive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaGenericProcessorTest {
    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val processor = KafkaGenericProcessor(eventArchiveService)

    @Test
    fun `process calls eventArchiveService saveGenericForTopic with topic and payload from generic record`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericRecord.get("type") } returns "DATASET_HARVESTED"
        every { genericRecord.get("harvestRunId") } returns "run-1"
        every { genericRecord.get("uri") } returns "https://example.com/1"
        every { genericRecord.get("fdkId") } returns "fdk-123"
        every { genericRecord.get("graph") } returns "<> a <http://example.org/Dataset> ."
        every { genericRecord.get("timestamp") } returns 1700000000000L

        every { eventArchiveService.saveGenericForTopic(any(), any()) } returns ArchiveWrite.Saved(ArchiveType.DATASET)

        val outcome = processor.process(genericRecord, "dataset-events")

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.DATASET))

        verify(exactly = 1) {
            eventArchiveService.saveGenericForTopic(
                "dataset-events",
                match { payload ->
                    payload["type"] == "DATASET_HARVESTED" &&
                        payload["harvestRunId"] == "run-1" &&
                        payload["uri"] == "https://example.com/1" &&
                        payload["fdkId"] == "fdk-123" &&
                        payload["graph"] == "<> a <http://example.org/Dataset> ." &&
                        payload["timestamp"] == "1700000000000"
                },
            )
        }
    }

    @Test
    fun `process converts numeric timestamp to string in payload`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericRecord.get("type") } returns "CONCEPT_REMOVED"
        every { genericRecord.get("harvestRunId") } returns null
        every { genericRecord.get("uri") } returns null
        every { genericRecord.get("fdkId") } returns "concept-1"
        every { genericRecord.get("graph") } returns ""
        every { genericRecord.get("timestamp") } returns 123L

        every { eventArchiveService.saveGenericForTopic(any(), any()) } returns ArchiveWrite.Saved(ArchiveType.CONCEPT)

        val outcome = processor.process(genericRecord, "concept-events")

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.CONCEPT))

        verify(exactly = 1) {
            eventArchiveService.saveGenericForTopic(
                "concept-events",
                match { payload ->
                    payload["timestamp"] == "123" && payload["type"] == "CONCEPT_REMOVED"
                },
            )
        }
    }

    @Test
    fun `process rethrows when eventArchiveService saveGenericForTopic throws`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericRecord.get("type") } returns "DATASET_HARVESTED"
        every { genericRecord.get("harvestRunId") } returns null
        every { genericRecord.get("uri") } returns null
        every { genericRecord.get("fdkId") } returns "fail-id"
        every { genericRecord.get("graph") } returns ""
        every { genericRecord.get("timestamp") } returns 1L

        every { eventArchiveService.saveGenericForTopic(any(), any()) } throws RuntimeException("write failed")

        val thrown =
            assertThrows(RuntimeException::class.java) {
                processor.process(genericRecord, "dataset-events")
            }
        assertEquals("write failed", thrown.message)

        verify(exactly = 1) { eventArchiveService.saveGenericForTopic("dataset-events", any()) }
    }

    @Test
    fun `process returns Skipped when saveGenericForTopic skips the payload`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericRecord.get("type") } returns "DATASET_REASONED"
        every { eventArchiveService.saveGenericForTopic(any(), any()) } returns
            ArchiveWrite.Skipped(ArchiveType.DATASET, "unsupported_event_type")

        val outcome = processor.process(genericRecord, "dataset-events")

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.DATASET, "unsupported_event_type"))
    }
}
