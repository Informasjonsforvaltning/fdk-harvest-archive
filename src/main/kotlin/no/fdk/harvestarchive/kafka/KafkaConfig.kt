package no.fdk.harvestarchive.kafka

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.subject.RecordNameStrategy
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
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
    @param:Value("\${spring.kafka.consumer.group-id:fdk-harvest-archive}") private val groupId: String,
) {
    @Bean
    open fun consumerFactory(): ConsumerFactory<String, SpecificRecord> {
        val props = schemaRegistrySerdeConfig(schemaRegistryUrl).apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
            put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false)
        }
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

    companion object {
        fun schemaRegistrySerdeConfig(schemaRegistryUrl: String): MutableMap<String, Any> = mutableMapOf(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
            AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION to true,
            AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY to RecordNameStrategy::class.java,
            AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY to RecordNameStrategy::class.java,
        )
    }
}
