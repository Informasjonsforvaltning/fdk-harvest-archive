package no.fdk.fdk_harvest_archive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.fdk_harvest_archive.archive.EventArchiveService
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaConceptEventCircuitBreakerTest {

    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val genericProcessor = mockk<KafkaGenericProcessor>(relaxed = true)
    private val circuitBreakerRegistration: CircuitBreaker = CircuitBreaker.ofDefaults("test-concept-cb")
    private val circuitBreaker = KafkaConceptEventCircuitBreaker(eventArchiveService, genericProcessor, circuitBreakerRegistration)

    private fun recordFor(event: ConceptEvent): org.apache.kafka.clients.consumer.ConsumerRecord<String, Any> =
        org.apache.kafka.clients.consumer.ConsumerRecord("concept-events", 0, 0L, "key", event as Any)

    @Test
    fun `process calls eventArchiveService saveConcept with event`() {
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/concept/1")
            .setFdkId("concept-123")
            .setGraph("<> a <http://example.org/Concept> .")
            .setTimestamp(1700000000000L)
            .build()
        every { eventArchiveService.saveConcept(any()) } returns Unit

        circuitBreaker.process(recordFor(event))

        verify(exactly = 1) { eventArchiveService.saveConcept(event) }
    }

    @Test
    fun `reasoned events are skipped`() {
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_REASONED)
            .setHarvestRunId("12")
            .setUri("https://concept.test")
            .setFdkId("test-concept-123")
            .setGraph("<http://example.org/concept/123>")
            .setTimestamp(123)
            .build()

        circuitBreaker.process(recordFor(event))

        verify(exactly = 0) { eventArchiveService.saveConcept(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveConcept throws`() {
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_REMOVED)
            .setFdkId("fail-id")
            .setGraph("")
            .setTimestamp(1L)
            .build()
        every { eventArchiveService.saveConcept(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(recordFor(event))
        }

        verify(exactly = 1) { eventArchiveService.saveConcept(event) }
    }

    @Test
    fun `unsupported value type is skipped and genericProcessor not called`() {
        val record = org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
            "concept-events",
            0,
            0L,
            "key",
            42,
        )

        circuitBreaker.process(record)

        verify(exactly = 0) { eventArchiveService.saveConcept(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }
}
