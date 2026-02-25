package no.fdk.fdk_harvest_archive.kafka

/**
 * Interface to allow JDK dynamic proxies for AOP (Resilience4j circuit breaker aspect).
 */
interface KafkaCircuitBreakerApi<T> {
    fun process(event: T)
}
