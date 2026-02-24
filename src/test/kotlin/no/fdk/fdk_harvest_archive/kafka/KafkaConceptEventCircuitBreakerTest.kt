package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaConceptEventCircuitBreakerTest {

    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val circuitBreaker = KafkaConceptEventCircuitBreaker(eventArchiveService)

    @Test
    fun `process calls eventArchiveService saveConcept with record value`() {
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/concept/1")
            .setFdkId("concept-123")
            .setGraph("<> a <http://example.org/Concept> .")
            .setTimestamp(1700000000000L)
            .build()
        val record = ConsumerRecord("concept-events", 0, 42L, "concept-123", event)

        every { eventArchiveService.saveConcept(any()) } returns Unit

        circuitBreaker.process(record)

        verify(exactly = 1) { eventArchiveService.saveConcept(event) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveConcept throws`() {
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_REMOVED)
            .setFdkId("fail-id")
            .setGraph("")
            .setTimestamp(1L)
            .build()
        val record = ConsumerRecord("concept-events", 1, 0L, "fail-id", event)

        every { eventArchiveService.saveConcept(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(record)
        }

        verify(exactly = 1) { eventArchiveService.saveConcept(event) }
    }
}
