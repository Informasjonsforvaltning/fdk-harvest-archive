package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaDataServiceEventConsumerTest {

    private val circuitBreaker: KafkaDataServiceEventCircuitBreaker = mockk()
    private val consumer = KafkaDataServiceEventConsumer(circuitBreaker)
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaDataServiceEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeDataServiceEvent skips DATA_SERVICE_REASONED and acknowledges`() {
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_REASONED)
            .setHarvestRunId("12")
            .setUri("https://dataservice.test")
            .setFdkId("test-dataservice-123")
            .setGraph("<http://example.org/dataservice/123> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(123)
            .build()
        val record = ConsumerRecord("dataservice-events", 0, 0L, "key", event)

        consumer.consumeDataServiceEvent(record, ack)

        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { circuitBreaker.process(any()) }
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
        val record = ConsumerRecord("dataservice-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeDataServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
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
        val record = ConsumerRecord("dataservice-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeDataServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
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
        val record = ConsumerRecord("dataservice-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeDataServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
    }
}
