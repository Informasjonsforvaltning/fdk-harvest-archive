package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaGenericCircuitBreakerTest {

    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val circuitBreaker = KafkaGenericCircuitBreaker(eventArchiveService)

    @Test
    fun `process calls eventArchiveService saveGenericForTopic with topic and payload from generic record`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericRecord.get("type") } returns "DATASET_HARVESTED"
        every { genericRecord.get("harvestRunId") } returns "run-1"
        every { genericRecord.get("uri") } returns "https://example.com/1"
        every { genericRecord.get("fdkId") } returns "fdk-123"
        every { genericRecord.get("graph") } returns "<> a <http://example.org/Dataset> ."
        every { genericRecord.get("timestamp") } returns 1700000000000L

        every { eventArchiveService.saveGenericForTopic(any(), any()) } returns Unit

        circuitBreaker.process(genericRecord, "dataset-events")

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

        every { eventArchiveService.saveGenericForTopic(any(), any()) } returns Unit

        circuitBreaker.process(genericRecord, "concept-events")

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

        val thrown = assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(genericRecord, "dataset-events")
        }
        assertEquals("write failed", thrown.message)

        verify(exactly = 1) { eventArchiveService.saveGenericForTopic("dataset-events", any()) }
    }
}
