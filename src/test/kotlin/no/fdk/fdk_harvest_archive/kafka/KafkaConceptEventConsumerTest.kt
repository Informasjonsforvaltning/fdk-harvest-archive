package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaConceptEventConsumerTest {

    private val circuitBreaker: KafkaConceptEventCircuitBreaker = mockk()
    private val consumer = KafkaConceptEventConsumer(circuitBreaker)
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaConceptEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeConceptEvent skips CONCEPT_REASONED and acknowledges`() {
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_REASONED)
            .setHarvestRunId("12")
            .setUri("https://concept.test")
            .setFdkId("test-concept-123")
            .setGraph("<http://example.org/concept/123> a <http://www.w3.org/2004/02/skos/core#Concept> .")
            .setTimestamp(123)
            .build()
        val record = ConsumerRecord("concept-events", 0, 0L, "key", event)

        consumer.consumeConceptEvent(record, ack)

        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { circuitBreaker.process(any()) }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeConceptEvent processes CONCEPT_HARVESTED and acknowledges on success`() {
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://concept.test")
            .setFdkId("test-concept-123")
            .setGraph("<http://example.org/concept/123> a <http://www.w3.org/2004/02/skos/core#Concept> .")
            .setTimestamp(123)
            .build()
        val record = ConsumerRecord("concept-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeConceptEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeConceptEvent processes CONCEPT_REMOVED and acknowledges on success`() {
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_REMOVED)
            .setHarvestRunId("12")
            .setUri("https://concept.test")
            .setFdkId("test-concept-123")
            .setGraph("")
            .setTimestamp(123)
            .build()
        val record = ConsumerRecord("concept-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeConceptEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeConceptEvent nacks on processing error`() {
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://concept.test")
            .setFdkId("test-concept-123")
            .setGraph("<http://example.org/concept/123> a <http://www.w3.org/2004/02/skos/core#Concept> .")
            .setTimestamp(123)
            .build()
        val record = ConsumerRecord("concept-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeConceptEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
    }
}
