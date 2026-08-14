package no.fdk.harvestarchive.kafka

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.subject.RecordNameStrategy
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.kafka.listener.ContainerProperties

@Tag("unit")
class KafkaConfigTest {

    @Test
    fun `listener container factory uses MANUAL ack mode so Acknowledgment is available to listeners`() {
        val factory = kafkaConfig().kafkaListenerContainerFactory()

        assertThat(factory.containerProperties.ackMode)
            .isEqualTo(ContainerProperties.AckMode.MANUAL)
    }

    @Test
    fun `consumer factory uses RecordNameStrategy and schema registry settings`() {
        val props = kafkaConfig().consumerFactory().configurationProperties

        assertThat(props[AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY])
            .isEqualTo(RecordNameStrategy::class.java)
        assertThat(props[AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY])
            .isEqualTo(RecordNameStrategy::class.java)
        assertThat(props[AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS]).isEqualTo(false)
        assertThat(props[AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION]).isEqualTo(true)
        assertThat(props[KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG]).isEqualTo(true)
        assertThat(props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG]).isEqualTo(false)
        assertThat(props[ConsumerConfig.GROUP_ID_CONFIG]).isEqualTo("fdk-harvest-archive")
    }

    private fun kafkaConfig() = KafkaConfig(
        bootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        groupId = "fdk-harvest-archive",
    )
}
