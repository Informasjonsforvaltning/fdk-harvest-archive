package no.fdk.fdk_harvest_archive.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Interface to allow JDK dynamic proxies for AOP (Resilience4j circuit breaker aspect).
 * Implementations receive the full [ConsumerRecord] and are responsible for handling
 * both typed Avro records and generic records.
 */
interface KafkaCircuitBreakerApi {
    fun process(record: ConsumerRecord<String, Any>)
}
