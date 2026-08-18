package no.fdk.harvestarchive.metrics

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.fdk.harvestarchive.archive.ArchiveType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

@Tag("unit")
class ArchiveMetricsTest {
    private lateinit var registry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        ArchiveMetrics.bind(registry)
    }

    @AfterEach
    fun tearDown() {
        ArchiveType.entries.forEach { ArchiveMetrics.setListenerPaused(it.listenerId, false) }
        registry.clear()
    }

    @Test
    fun `recordSaved increments files saved counter timer and byte summary`() {
        ArchiveMetrics.recordSaved(ArchiveType.DATASET, "DATASET_HARVESTED", 128, 10.milliseconds)

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_archive_files_saved_total",
                    "type",
                    "datasets",
                    "event_type",
                    "harvested",
                    "status",
                    "success",
                ).count(),
        )
        assertEquals(1L, registry.timer("harvest_archive_save_time", "type", "datasets").count())
        assertEquals(128.0, registry.summary("harvest_archive_file_bytes", "type", "datasets").totalAmount())
    }

    @Test
    fun `recordSaveError increments files saved error counter`() {
        ArchiveMetrics.recordSaveError(ArchiveType.CONCEPT, "CONCEPT_REMOVED")

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_archive_files_saved_total",
                    "type",
                    "concepts",
                    "event_type",
                    "removed",
                    "status",
                    "error",
                ).count(),
        )
    }

    @Test
    fun `recordSkipped increments skipped counter with reason`() {
        ArchiveMetrics.recordSkipped(ArchiveType.DATASET, ArchiveMetrics.SkipReason.DISALLOWED_TYPE)
        ArchiveMetrics.recordSkipped(null, ArchiveMetrics.SkipReason.UNKNOWN_TOPIC)

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_archive_skipped_total",
                    "type",
                    "datasets",
                    "reason",
                    "disallowed_type",
                ).count(),
        )
        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_archive_skipped_total",
                    "type",
                    "unknown",
                    "reason",
                    "unknown_topic",
                ).count(),
        )
    }

    @Test
    fun `recordZip increments zip counters and summaries`() {
        ArchiveMetrics.recordZip(ArchiveType.SERVICE, ArchiveMetrics.ZipStatus.SUCCESS, 3, 256, 20.milliseconds)

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_archive_zip_total",
                    "type",
                    "services",
                    "status",
                    "success",
                ).count(),
        )
        assertEquals(3.0, registry.summary("harvest_archive_zip_files", "type", "services").totalAmount())
        assertEquals(256.0, registry.summary("harvest_archive_zip_bytes", "type", "services").totalAmount())
        assertEquals(
            1L,
            registry.timer("harvest_archive_zip_time", "type", "services", "status", "success").count(),
        )
    }

    @Test
    fun `recordEventProcessed increments processing total`() {
        ArchiveMetrics.recordEventProcessed(ArchiveType.DATASET, ArchiveMetrics.EventProcessingResult.ACKED)
        ArchiveMetrics.recordEventProcessed(null, ArchiveMetrics.EventProcessingResult.CIRCUIT_OPEN)

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_archive_event_processing_total",
                    "type",
                    "datasets",
                    "result",
                    "acked",
                ).count(),
        )
        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_archive_event_processing_total",
                    "type",
                    "unknown",
                    "result",
                    "circuit_open",
                ).count(),
        )
    }

    @Test
    fun `registerGauges exposes listener paused and directory gauges`() {
        ArchiveMetrics.registerGauges()

        assertEquals(
            0.0,
            registry.find("kafka_listener_paused").tag("listener", "dataset-archive").gauge()?.value(),
        )
        assertEquals(
            0.0,
            registry.find("harvest_archive_dir_bytes").tag("type", "datasets").gauge()?.value(),
        )

        ArchiveMetrics.setListenerPaused("dataset-archive", true)
        ArchiveMetrics.updateDirectorySnapshot(ArchiveType.DATASET, 1024, 7)

        assertEquals(
            1.0,
            registry.find("kafka_listener_paused").tag("listener", "dataset-archive").gauge()?.value(),
        )
        assertEquals(
            1024.0,
            registry.find("harvest_archive_dir_bytes").tag("type", "datasets").gauge()?.value(),
        )
        assertEquals(
            7.0,
            registry.find("harvest_archive_dir_files").tag("type", "datasets").gauge()?.value(),
        )
    }
}
