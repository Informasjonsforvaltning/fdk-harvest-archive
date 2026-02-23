package no.fdk.fdk_harvest_archive.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Interface to allow JDK dynamic proxies for AOP (Resilience4j circuit breaker aspect).
 */
interface KafkaCircuitBreakerApi<T> {
    fun process(record: ConsumerRecord<String, T>)
}
