package no.fdk.fdk_harvest_archive.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import java.time.Duration

@Tag("unit")
class KafkaServiceEventConsumerTest {

    private val circuitBreaker: KafkaServiceEventCircuitBreaker = mockk()
    private val consumer = KafkaServiceEventConsumer(circuitBreaker)
    private val ack: Acknowledgment = mockk(relaxed = true)

    @Test
    fun `consumer has non-null logger so logging never throws NPE`() {
        val loggerMethod = KafkaServiceEventConsumer::class.java.getDeclaredMethod("logger")
        loggerMethod.isAccessible = true
        assertThat(loggerMethod.invoke(consumer)).isNotNull()
    }

    @Test
    fun `consumeServiceEvent skips SERVICE_REASONED and acknowledges`() {
        val event = ServiceEvent.newBuilder()
            .setType(ServiceEventType.SERVICE_REASONED)
            .setHarvestRunId("12")
            .setUri("https://service.test")
            .setFdkId("test-service-123")
            .setGraph("<http://example.org/service/123> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(123)
            .build()
        val record = ConsumerRecord("service-events", 0, 0L, "key", event)

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { circuitBreaker.process(any()) }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeServiceEvent processes SERVICE_HARVESTED and acknowledges on success`() {
        val event = ServiceEvent.newBuilder()
            .setType(ServiceEventType.SERVICE_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://service.test")
            .setFdkId("test-service-123")
            .setGraph("<http://example.org/service/123> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(123)
            .build()
        val record = ConsumerRecord("service-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeServiceEvent processes SERVICE_REMOVED and acknowledges on success`() {
        val event = ServiceEvent.newBuilder()
            .setType(ServiceEventType.SERVICE_REMOVED)
            .setHarvestRunId("12")
            .setUri("https://service.test")
            .setFdkId("test-service-123")
            .setGraph("")
            .setTimestamp(123)
            .build()
        val record = ConsumerRecord("service-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } returns Unit

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.acknowledge() }
        verify(exactly = 0) { ack.nack(any<Duration>()) }
    }

    @Test
    fun `consumeServiceEvent nacks on processing error`() {
        val event = ServiceEvent.newBuilder()
            .setType(ServiceEventType.SERVICE_HARVESTED)
            .setHarvestRunId("12")
            .setUri("https://service.test")
            .setFdkId("test-service-123")
            .setGraph("<http://example.org/service/123> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(123)
            .build()
        val record = ConsumerRecord("service-events", 0, 0L, "key", event)

        every { circuitBreaker.process(any()) } throws RuntimeException("boom")

        consumer.consumeServiceEvent(record, ack)

        verify(exactly = 1) { circuitBreaker.process(record) }
        verify(exactly = 1) { ack.nack(Duration.ZERO) }
        verify(exactly = 0) { ack.acknowledge() }
    }
}
