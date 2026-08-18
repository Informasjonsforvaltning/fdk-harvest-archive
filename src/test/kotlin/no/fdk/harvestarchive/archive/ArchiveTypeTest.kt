package no.fdk.harvestarchive.archive

import no.fdk.dataset.DatasetEventType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("unit")
class ArchiveTypeTest {
    @Test
    fun `fromTopic resolves archive type by kafka topic`() {
        assertThat(ArchiveType.fromTopic("dataset-events")).isEqualTo(ArchiveType.DATASET)
        assertThat(ArchiveType.fromTopic("concept-events")).isEqualTo(ArchiveType.CONCEPT)
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
}
