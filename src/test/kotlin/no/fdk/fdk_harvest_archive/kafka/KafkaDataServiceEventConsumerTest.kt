package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaDataServiceEventConsumerTest {

    private val circuitBreaker: KafkaDataServiceEventCircuitBreaker = mockk()
    private val genericCircuitBreaker: KafkaGenericCircuitBreaker = mockk(relaxed = true)
    private val consumer = KafkaDataServiceEventConsumer(circuitBreaker, genericCircuitBreaker)
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaDataServiceEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeDataServiceEvent delegates generic record to generic circuit breaker with topic and acknowledges`() {
        val genericRecord = mockk<GenericRecord>(relaxed = true)
        val record: ConsumerRecord<String, Any> = ConsumerRecord("data-service-events", 0, 0L, "key", genericRecord)

        every { genericCircuitBreaker.process(any(), any()) } returns Unit

        consumer.consumeDataServiceEvent(record, ack)

        verify(exactly = 1) { genericCircuitBreaker.process(genericRecord, "data-service-events") }
        verify(exactly = 0) { circuitBreaker.process(any<DataServiceEvent>()) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeDataServiceEvent processes DATA_SERVICE_HARVESTED and acknowledges on success`() {
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://dataservice.test")
            .setFdkId("test-dataservice-123")
            .setGraph("<http://example.org/dataservice/123> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(123)
            .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("data-service-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeDataServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(event) }
        verify(exactly = 0) { genericCircuitBreaker.process(any(), "data-service-events") }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeDataServiceEvent processes DATA_SERVICE_REMOVED and acknowledges on success`() {
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_REMOVED)
            .setHarvestRunId("12")
            .setUri("https://dataservice.test")
            .setFdkId("test-dataservice-123")
            .setGraph("")
            .setTimestamp(123)
            .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("data-service-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeDataServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(event) }
        verify(exactly = 0) { genericCircuitBreaker.process(any(), "data-service-events") }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeDataServiceEvent nacks on processing error`() {
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://dataservice.test")
            .setFdkId("test-dataservice-123")
            .setGraph("<http://example.org/dataservice/123> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(123)
            .build()
        val record: ConsumerRecord<String, Any> = ConsumerRecord("data-service-events", 0, 0L, "key", event as Any)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeDataServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(event) }
        verify(exactly = 0) { genericCircuitBreaker.process(any(), "data-service-events") }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
    }
}
