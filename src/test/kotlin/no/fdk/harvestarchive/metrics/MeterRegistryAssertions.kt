package no.fdk.harvestarchive.metrics

import io.micrometer.core.instrument.MeterRegistry
import no.fdk.harvestarchive.archive.ArchiveType
import org.assertj.core.api.Assertions.assertThat

internal fun MeterRegistry.assertEventProcessed(type: ArchiveType, result: String, count: Double = 1.0) {
    assertThat(
        find("harvest_archive_event_processing_total")
            .tag("type", type.metricTag)
            .tag("result", result)
            .counter()
            ?.count() ?: 0.0,
    ).isEqualTo(count)
}

internal fun MeterRegistry.listenerPaused(listenerId: String): Double? =
    find("kafka_listener_paused").tag("listener", listenerId).gauge()?.value()
