package no.fdk.harvestarchive.metrics

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import no.fdk.harvestarchive.archive.ArchiveType
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.toJavaDuration

@Component
class ArchiveMetrics(private val registry: MeterRegistry) {
    private val listenerPaused = ConcurrentHashMap<String, AtomicInteger>()
    private val dirBytes = ConcurrentHashMap<ArchiveType, AtomicLong>()
    private val dirFiles = ConcurrentHashMap<ArchiveType, AtomicLong>()

    init {
        registerGauges()
    }

    fun recordSaved(type: ArchiveType, eventType: String, bytes: Long, duration: Duration) {
        val eventTypeLabel = metricEventType(eventType)
        registry
            .counter(
                "harvest_archive_files_saved_total",
                "type",
                type.metricTag,
                "event_type",
                eventTypeLabel,
                "status",
                "success",
            ).increment()
        registry
            .summary("harvest_archive_file_bytes", "type", type.metricTag)
            .record(bytes.toDouble())
        registry
            .timer("harvest_archive_save_time", "type", type.metricTag)
            .record(duration.toJavaDuration())
    }

    fun recordSaveError(type: ArchiveType, eventType: String) {
        registry
            .counter(
                "harvest_archive_files_saved_total",
                "type",
                type.metricTag,
                "event_type",
                metricEventType(eventType),
                "status",
                "error",
            ).increment()
    }

    fun recordSkipped(type: ArchiveType?, reason: SkipReason) {
        registry
            .counter(
                "harvest_archive_skipped_total",
                "type",
                type?.metricTag ?: "unknown",
                "reason",
                reason.label,
            ).increment()
    }

    fun recordZip(type: ArchiveType, status: ZipStatus, fileCount: Int, zipBytes: Long, duration: Duration) {
        registry
            .counter(
                "harvest_archive_zip_total",
                "type",
                type.metricTag,
                "status",
                status.label,
            ).increment()
        registry
            .summary("harvest_archive_zip_files", "type", type.metricTag)
            .record(fileCount.toDouble())
        registry
            .summary("harvest_archive_zip_bytes", "type", type.metricTag)
            .record(zipBytes.toDouble())
        registry
            .timer("harvest_archive_zip_time", "type", type.metricTag, "status", status.label)
            .record(duration.toJavaDuration())
    }

    fun recordEventProcessed(type: ArchiveType?, result: EventProcessingResult) {
        registry
            .counter(
                "harvest_archive_event_processing_total",
                "type",
                type?.metricTag ?: "unknown",
                "result",
                result.label,
            ).increment()
    }

    fun setListenerPaused(listenerId: String, paused: Boolean) {
        listenerPausedValue(listenerId).set(if (paused) 1 else 0)
    }

    fun updateDirectorySnapshot(type: ArchiveType, bytes: Long, fileCount: Long) {
        dirBytesValue(type).set(bytes)
        dirFilesValue(type).set(fileCount)
    }

    fun metricEventType(eventType: String): String = when {
        eventType.endsWith("_HARVESTED") -> "harvested"
        eventType.endsWith("_REMOVED") -> "removed"
        else -> "other"
    }

    private fun registerGauges() {
        ArchiveType.entries.forEach { type ->
            Gauge
                .builder("kafka_listener_paused") { listenerPausedValue(type.listenerId).get().toDouble() }
                .description("1 when the harvest-archive Kafka listener is paused, otherwise 0")
                .tag("listener", type.listenerId)
                .register(registry)
            Gauge
                .builder("harvest_archive_dir_bytes") { dirBytesValue(type).get().toDouble() }
                .description("Last scanned size in bytes of the unzipped archive directory")
                .tag("type", type.metricTag)
                .register(registry)
            Gauge
                .builder("harvest_archive_dir_files") { dirFilesValue(type).get().toDouble() }
                .description("Last scanned file count of the unzipped archive directory")
                .tag("type", type.metricTag)
                .register(registry)
        }
    }

    private fun listenerPausedValue(listenerId: String): AtomicInteger = listenerPaused.computeIfAbsent(listenerId) { AtomicInteger(0) }

    private fun dirBytesValue(type: ArchiveType): AtomicLong = dirBytes.computeIfAbsent(type) { AtomicLong(0) }

    private fun dirFilesValue(type: ArchiveType): AtomicLong = dirFiles.computeIfAbsent(type) { AtomicLong(0) }

    enum class SkipReason(val label: String) {
        UNKNOWN_TOPIC("unknown_topic"),
        MISSING_TYPE("missing_type"),
        DISALLOWED_TYPE("disallowed_type"),
        UNSUPPORTED_PAYLOAD("unsupported_payload"),
    }

    enum class ZipStatus(val label: String) {
        SUCCESS("success"),
        ERROR("error"),
    }

    enum class EventProcessingResult(val label: String) {
        ACKED("acked"),
        NACKED("nacked"),
        SKIPPED("skipped"),
        CIRCUIT_OPEN("circuit_open"),
    }
}
