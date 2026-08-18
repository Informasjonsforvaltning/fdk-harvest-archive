package no.fdk.harvestarchive.config

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import org.springframework.context.annotation.Configuration

/**
 * Binds Resilience4j circuit breaker metrics and custom harvest-archive meters
 * to the application [MeterRegistry].
 */
@Configuration
open class MetricsConfiguration(private val circuitBreakerRegistry: CircuitBreakerRegistry, private val meterRegistry: MeterRegistry) {
    @PostConstruct
    fun bindMetrics() {
        ArchiveMetrics.bind(meterRegistry)
        ArchiveMetrics.registerGauges()

        TaggedCircuitBreakerMetrics
            .ofCircuitBreakerRegistry(circuitBreakerRegistry)
            .bindTo(meterRegistry)
    }
}
