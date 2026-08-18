package no.fdk.harvestarchive.metrics

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.fdk.harvestarchive.archive.ArchiveType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

@Tag("unit")
class ArchiveMetricsTest {
    private lateinit var registry: SimpleMeterRegistry
    private lateinit var metrics: ArchiveMetrics

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        metrics = ArchiveMetrics(registry)
    }

    @Test
    fun `recordSaved increments files saved counter timer and byte summary`() {
        metrics.recordSaved(ArchiveType.DATASET, "DATASET_HARVESTED", 128, 10.milliseconds)

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
        metrics.recordSaveError(ArchiveType.CONCEPT, "CONCEPT_REMOVED")

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
    fun `recordZip increments zip counters and summaries`() {
        metrics.recordZip(ArchiveType.SERVICE, 3, 256, 20.milliseconds)

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
    fun `recordZipError increments error counter and timer without summaries`() {
        metrics.recordZipError(ArchiveType.SERVICE, 15.milliseconds)

        assertEquals(
            1.0,
            registry
                .counter(
                    "harvest_archive_zip_total",
                    "type",
                    "services",
                    "status",
                    "error",
                ).count(),
        )
        assertEquals(
            1L,
            registry.timer("harvest_archive_zip_time", "type", "services", "status", "error").count(),
        )
        assertEquals(0.0, registry.find("harvest_archive_zip_files").tag("type", "services").summary()?.totalAmount() ?: 0.0)
        assertEquals(0.0, registry.find("harvest_archive_zip_bytes").tag("type", "services").summary()?.totalAmount() ?: 0.0)
    }

    @Test
    fun `recordEventProcessed increments processing total`() {
        metrics.recordEventProcessed(ArchiveType.DATASET, ArchiveMetrics.EventProcessingResult.ACKED)
        metrics.recordEventProcessed(null, ArchiveMetrics.EventProcessingResult.CIRCUIT_OPEN)

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
    fun `gauges expose listener paused and directory snapshots`() {
        assertEquals(
            0.0,
            registry.find("kafka_listener_paused").tag("listener", "dataset-archive").gauge()?.value(),
        )
        assertEquals(
            0.0,
            registry.find("harvest_archive_dir_bytes").tag("type", "datasets").gauge()?.value(),
        )

        metrics.setListenerPaused("dataset-archive", true)
        metrics.updateDirectorySnapshot(ArchiveType.DATASET, 1024, 7)

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
