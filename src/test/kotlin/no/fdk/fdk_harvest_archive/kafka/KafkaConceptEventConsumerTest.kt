package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaConceptEventConsumerTest {

    private val circuitBreaker: KafkaConceptEventCircuitBreaker = mockk()
    private val genericCircuitBreaker: KafkaGenericCircuitBreaker = mockk(relaxed = true)
    private val consumer = KafkaConceptEventConsumer(circuitBreaker, genericCircuitBreaker)
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaConceptEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeConceptEvent delegates generic record to generic circuit breaker with topic and acknowledges`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        val record: ConsumerRecord<String, Any> = ConsumerRecord("concept-events", 0, 0L, "key", genericRecord)

        every { genericCircuitBreaker.process(any(), any()) } returns Unit

        consumer.consumeConceptEvent(record, ack)

        verify(exactly = 1) { genericCircuitBreaker.process(genericRecord, "concept-events") }
        verify(exactly = 0) { circuitBreaker.process(any<ConceptEvent>()) }
        verify(exactly = 1) { ack.acknowledge() }
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
        val record: ConsumerRecord<String, Any> = ConsumerRecord("concept-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeConceptEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(event) }
        verify(exactly = 0) { genericCircuitBreaker.process(any(), "concept-events") }
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
        val record: ConsumerRecord<String, Any> = ConsumerRecord("concept-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeConceptEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(event) }
        verify(exactly = 0) { genericCircuitBreaker.process(any(), "concept-events") }
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
        val record: ConsumerRecord<String, Any> = ConsumerRecord("concept-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeConceptEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(event) }
        verify(exactly = 0) { genericCircuitBreaker.process(any(), "concept-events") }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
    }
}
