package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaInformationModelEventConsumerTest {

    private val circuitBreaker: KafkaInformationModelEventCircuitBreaker = mockk()
    private val consumer = KafkaInformationModelEventConsumer(circuitBreaker)
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaInformationModelEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeInformationModelEvent delegates record to circuit breaker and acknowledges`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", genericRecord)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeInformationModelEvent processes INFORMATION_MODEL_HARVESTED and acknowledges on success`() {
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://informationmodel.test")
            .setFdkId("test-informationmodel-123")
            .setGraph("<http://example.org/informationmodel/123> a <http://www.w3.org/ns/dcat#Dataset> .")
            .setTimestamp(123)
            .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeInformationModelEvent processes INFORMATION_MODEL_REMOVED and acknowledges on success`() {
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_REMOVED)
            .setHarvestRunId("12")
            .setUri("https://informationmodel.test")
            .setFdkId("test-informationmodel-123")
            .setGraph("")
            .setTimestamp(123)
            .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeInformationModelEvent nacks on processing error`() {
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://informationmodel.test")
            .setFdkId("test-informationmodel-123")
            .setGraph("<http://example.org/informationmodel/123> a <http://www.w3.org/ns/dcat#Dataset> .")
            .setTimestamp(123)
            .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("information-model-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeInformationModelEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
    }
}
