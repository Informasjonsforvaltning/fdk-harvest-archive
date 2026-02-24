package no.fdk.fdk_harvest_archive.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.listener.ContainerProperties

/**
 * Kafka consumer configuration: bootstrap servers, Schema Registry, and a shared listener
 * container factory for Avro (SpecificRecord) values with manual acknowledgment.
 */
@Configuration
@EnableKafka
open class KafkaConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @param:Value("\${spring.kafka.consumer.properties.schema.registry.url}") private val schemaRegistryUrl: String,
) {
    @Bean
    open fun consumerFactory(): ConsumerFactory<String, SpecificRecord> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = "fdk-harvest-archive"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        props["schema.registry.url"] = schemaRegistryUrl
        props["specific.avro.reader"] = true
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    open fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, SpecificRecord> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, SpecificRecord>()
        factory.setConsumerFactory(consumerFactory())
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        factory.setConcurrency(4)
        return factory
    }

}
