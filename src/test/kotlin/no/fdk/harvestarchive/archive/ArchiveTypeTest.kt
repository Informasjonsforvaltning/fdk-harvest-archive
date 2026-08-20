package no.fdk.harvestarchive.archive

import no.fdk.dataset.DatasetEventType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class ArchiveTypeTest {
    @Test
    fun `fromTopic resolves archive type by kafka topic`() {
        assertThat(ArchiveType.fromTopic(ArchiveType.TOPIC_DATASET)).isEqualTo(ArchiveType.DATASET)
        assertThat(ArchiveType.fromTopic(ArchiveType.TOPIC_CONCEPT)).isEqualTo(ArchiveType.CONCEPT)
        assertThat(ArchiveType.fromTopic("unknown-topic")).isNull()
    }

    @Test
    fun `allowsEventType accepts only harvested and removed events for type`() {
        assertThat(ArchiveType.DATASET.allowsEventType(DatasetEventType.DATASET_HARVESTED.name)).isTrue()
        assertThat(ArchiveType.DATASET.allowsEventType(DatasetEventType.DATASET_REMOVED.name)).isTrue()
        assertThat(ArchiveType.DATASET.allowsEventType(DatasetEventType.DATASET_REASONED.name)).isFalse()
    }

    @Test
    fun `each archive type exposes unique ids for metrics and circuit breakers`() {
        val metricTags = ArchiveType.entries.map { it.metricTag }
        val circuitBreakerIds = ArchiveType.entries.map { it.circuitBreakerId }
        val listenerIds = ArchiveType.entries.map { it.listenerId }

        assertThat(metricTags).doesNotHaveDuplicates()
        assertThat(circuitBreakerIds).doesNotHaveDuplicates()
        assertThat(listenerIds).doesNotHaveDuplicates()
    }

    @Test
    fun `enum ids match the constants used by kafka listeners`() {
        ArchiveType.entries.forEach { type ->
            assertThat(type.topicName).isEqualTo(
                when (type) {
                    ArchiveType.DATASET -> ArchiveType.TOPIC_DATASET
                    ArchiveType.CONCEPT -> ArchiveType.TOPIC_CONCEPT
                    ArchiveType.DATA_SERVICE -> ArchiveType.TOPIC_DATA_SERVICE
                    ArchiveType.INFORMATION_MODEL -> ArchiveType.TOPIC_INFORMATION_MODEL
                    ArchiveType.EVENT -> ArchiveType.TOPIC_EVENT
                    ArchiveType.SERVICE -> ArchiveType.TOPIC_SERVICE
                },
            )
            assertThat(type.listenerId).isEqualTo(
                when (type) {
                    ArchiveType.DATASET -> ArchiveType.LISTENER_DATASET
                    ArchiveType.CONCEPT -> ArchiveType.LISTENER_CONCEPT
                    ArchiveType.DATA_SERVICE -> ArchiveType.LISTENER_DATA_SERVICE
                    ArchiveType.INFORMATION_MODEL -> ArchiveType.LISTENER_INFORMATION_MODEL
                    ArchiveType.EVENT -> ArchiveType.LISTENER_EVENT
                    ArchiveType.SERVICE -> ArchiveType.LISTENER_SERVICE
                },
            )
            assertThat(type.circuitBreakerId).isEqualTo(
                when (type) {
                    ArchiveType.DATASET -> ArchiveType.CIRCUIT_BREAKER_DATASET
                    ArchiveType.CONCEPT -> ArchiveType.CIRCUIT_BREAKER_CONCEPT
                    ArchiveType.DATA_SERVICE -> ArchiveType.CIRCUIT_BREAKER_DATA_SERVICE
                    ArchiveType.INFORMATION_MODEL -> ArchiveType.CIRCUIT_BREAKER_INFORMATION_MODEL
                    ArchiveType.EVENT -> ArchiveType.CIRCUIT_BREAKER_EVENT
                    ArchiveType.SERVICE -> ArchiveType.CIRCUIT_BREAKER_SERVICE
                },
            )
        }
    }
}
