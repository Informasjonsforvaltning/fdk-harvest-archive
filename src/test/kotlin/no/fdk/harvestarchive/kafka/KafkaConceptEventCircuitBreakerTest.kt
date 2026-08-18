package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import org.apache.avro.generic.GenericRecord
import org.assertj.core.api.Assertions.assertThat
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
        org.apache.kafka.clients.consumer
            .ConsumerRecord("concept-events", 0, 0L, "key", event as Any)

    @Test
    fun `process calls eventArchiveService saveConcept with event and returns Saved`() {
        val event =
            ConceptEvent
                .newBuilder()
                .setType(ConceptEventType.CONCEPT_HARVESTED)
                .setHarvestRunId("run-1")
                .setUri("https://example.com/concept/1")
                .setFdkId("concept-123")
                .setGraph("<> a <http://example.org/Concept> .")
                .setTimestamp(1700000000000L)
                .build()
        every { eventArchiveService.saveConcept(any()) } returns Unit

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.CONCEPT))
        verify(exactly = 1) { eventArchiveService.saveConcept(event) }
    }

    @Test
    fun `reasoned events are skipped and return Skipped`() {
        val event =
            ConceptEvent
                .newBuilder()
                .setType(ConceptEventType.CONCEPT_REASONED)
                .setHarvestRunId("12")
                .setUri("https://concept.test")
                .setFdkId("test-concept-123")
                .setGraph("<http://example.org/concept/123>")
                .setTimestamp(123)
                .build()

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.CONCEPT))
        verify(exactly = 0) { eventArchiveService.saveConcept(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveConcept throws`() {
        val event =
            ConceptEvent
                .newBuilder()
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
    fun `unsupported value type returns Skipped`() {
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                "concept-events",
                0,
                0L,
                "key",
                42,
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.CONCEPT))
        verify(exactly = 0) { eventArchiveService.saveConcept(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }

    @Test
    fun `generic harvested record returns Saved from generic processor`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        every { genericProcessor.process(genericRecord, ArchiveType.CONCEPT.topicName) } returns
            ProcessOutcome.Saved(ArchiveType.CONCEPT)
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                ArchiveType.TOPIC_CONCEPT,
                0,
                0L,
                "key",
                genericRecord,
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.CONCEPT))
        verify(exactly = 1) { genericProcessor.process(genericRecord, ArchiveType.CONCEPT.topicName) }
        verify(exactly = 0) { eventArchiveService.saveConcept(any()) }
    }
}
