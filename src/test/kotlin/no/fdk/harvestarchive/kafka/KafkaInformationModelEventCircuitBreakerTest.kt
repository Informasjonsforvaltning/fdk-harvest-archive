package no.fdk.harvestarchive.kafka

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fdk.harvestarchive.archive.ArchiveType
import no.fdk.harvestarchive.archive.EventArchiveService
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class KafkaInformationModelEventCircuitBreakerTest {
    private val eventArchiveService = mockk<EventArchiveService>(relaxed = true)
    private val genericProcessor = mockk<KafkaGenericProcessor>(relaxed = true)
    private val circuitBreakerRegistration: CircuitBreaker = CircuitBreaker.ofDefaults("test-informationmodel-cb")
    private val circuitBreaker = KafkaInformationModelEventCircuitBreaker(eventArchiveService, genericProcessor, circuitBreakerRegistration)

    private fun recordFor(event: InformationModelEvent): org.apache.kafka.clients.consumer.ConsumerRecord<String, Any> =
        org.apache.kafka.clients.consumer
            .ConsumerRecord("information-model-events", 0, 0L, "key", event as Any)

    @Test
    fun `process calls eventArchiveService saveInformationModel with event and returns Saved`() {
        val event =
            InformationModelEvent
                .newBuilder()
                .setType(InformationModelEventType.INFORMATION_MODEL_HARVESTED)
                .setHarvestRunId("run-1")
                .setUri("https://example.com/informationmodel/1")
                .setFdkId("informationmodel-123")
                .setGraph("<> a <http://example.org/InformationModel> .")
                .setTimestamp(1700000000000L)
                .build()
        every { eventArchiveService.saveInformationModel(any()) } returns Unit

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Saved(ArchiveType.INFORMATION_MODEL))
        verify(exactly = 1) { eventArchiveService.saveInformationModel(event) }
    }

    @Test
    fun `reasoned events are skipped and return Skipped`() {
        val event =
            InformationModelEvent
                .newBuilder()
                .setType(InformationModelEventType.INFORMATION_MODEL_REASONED)
                .setHarvestRunId("12")
                .setUri("https://informationmodel.test")
                .setFdkId("test-informationmodel-123")
                .setGraph("<http://example.org/informationmodel/123>")
                .setTimestamp(123)
                .build()

        val outcome = circuitBreaker.process(recordFor(event))

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.INFORMATION_MODEL))
        verify(exactly = 0) { eventArchiveService.saveInformationModel(any()) }
    }

    @Test
    fun `process rethrows when eventArchiveService saveInformationModel throws`() {
        val event =
            InformationModelEvent
                .newBuilder()
                .setType(InformationModelEventType.INFORMATION_MODEL_REMOVED)
                .setFdkId("fail-id")
                .setGraph("")
                .setTimestamp(1L)
                .build()
        every { eventArchiveService.saveInformationModel(any()) } throws RuntimeException("write failed")

        assertThrows(RuntimeException::class.java) {
            circuitBreaker.process(recordFor(event))
        }

        verify(exactly = 1) { eventArchiveService.saveInformationModel(event) }
    }

    @Test
    fun `unsupported value type returns Skipped`() {
        val record =
            org.apache.kafka.clients.consumer.ConsumerRecord<String, Any>(
                "information-model-events",
                0,
                0L,
                "key",
                mapOf("unexpected" to true),
            )

        val outcome = circuitBreaker.process(record)

        assertThat(outcome).isEqualTo(ProcessOutcome.Skipped(ArchiveType.INFORMATION_MODEL))
        verify(exactly = 0) { eventArchiveService.saveInformationModel(any()) }
        verify(exactly = 0) { genericProcessor.process(any(), any()) }
    }
}
