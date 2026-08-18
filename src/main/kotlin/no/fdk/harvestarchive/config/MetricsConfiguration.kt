package no.fdk.harvestarchive.config

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import org.springframework.context.annotation.Configuration

/**
 * Binds Resilience4j circuit breaker metrics to the application meter registry.
 */
@Configuration
open class MetricsConfiguration(private val circuitBreakerRegistry: CircuitBreakerRegistry, private val meterRegistry: MeterRegistry) {
    @PostConstruct
    fun bindMetrics() {
        TaggedCircuitBreakerMetrics
            .ofCircuitBreakerRegistry(circuitBreakerRegistry)
            .bindTo(meterRegistry)
    }
}
