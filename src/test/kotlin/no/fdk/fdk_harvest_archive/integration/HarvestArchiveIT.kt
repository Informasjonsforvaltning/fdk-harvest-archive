package no.fdk.fdk_harvest_archive.integration

import no.fdk.concept.ConceptEvent
import no.fdk.concept.ConceptEventType
import no.fdk.dataset.DatasetEvent
import no.fdk.dataset.DatasetEventType
import no.fdk.dataservice.DataServiceEvent
import no.fdk.dataservice.DataServiceEventType
import no.fdk.event.EventEvent
import no.fdk.event.EventEventType
import no.fdk.informationmodel.InformationModelEvent
import no.fdk.informationmodel.InformationModelEventType
import no.fdk.service.ServiceEvent
import no.fdk.service.ServiceEventType
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/**
 * Integration tests: real Kafka (Testcontainers), mock Schema Registry, Spring context.
 * Each test produces one event to its topic and asserts the app consumes and saves it as JSON.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Tag("integration")
class HarvestArchiveIT {

    companion object {
        private val archiveRoot: Path = Files.createTempDirectory("dev-archive")

        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka"))

        private const val SCHEMA_REGISTRY_SCOPE = "fdk-harvest-archive"
        private const val MOCK_SCHEMA_REGISTRY_URL = "mock://$SCHEMA_REGISTRY_SCOPE"

        @JvmStatic
        @DynamicPropertySource
        fun kafkaProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
            registry.add("spring.kafka.consumer.properties.schema.registry.url") { MOCK_SCHEMA_REGISTRY_URL }
            registry.add("spring.kafka.listener.auto-startup") { "true" }
            registry.add("app.archive.dataset-dir") { archiveRoot.resolve("datasets").toString() }
            registry.add("app.archive.concept-dir") { archiveRoot.resolve("concepts").toString() }
            registry.add("app.archive.data-service-dir") { archiveRoot.resolve("data_services").toString() }
            registry.add("app.archive.information-model-dir") { archiveRoot.resolve("information_models").toString() }
            registry.add("app.archive.event-dir") { archiveRoot.resolve("events").toString() }
            registry.add("app.archive.service-dir") { archiveRoot.resolve("services").toString() }
            registry.add("app.archive.zip-check-interval-ms") { "300000" }
        }
    }

    @Test
    fun `consumes DatasetEvent from Kafka and saves as JSON file`() {
        val topic = "dataset-events"
        val fdkId = "integration-test-dataset-123"
        val timestamp = System.currentTimeMillis()
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_HARVESTED)
            .setHarvestRunId("run-dataset")
            .setUri("https://example.com/dataset")
            .setFdkId(fdkId)
            .setGraph("<> a <http://www.w3.org/ns/dcat#Dataset> .")
            .setTimestamp(timestamp)
            .build()

        produceAvroEvent(topic, fdkId, event)

        val datasetDir = archiveRoot.resolve("datasets")
        val expectedFile = datasetDir.resolve("${timestamp}_${fdkId}.json")
        val content = awaitFileWithContent(expectedFile, fdkId, timeoutSeconds = 30)
        Assertions.assertThat(content)
            .withFailMessage("Archive file did not contain '%s' within 30s: %s", fdkId, expectedFile)
            .isNotNull
            .contains(fdkId)
            .contains("DATASET_HARVESTED")
            .contains("https://example.com/dataset")
    }

    @Test
    fun `consumes ConceptEvent from Kafka and saves as JSON file`() {
        val topic = "concept-events"
        val fdkId = "integration-test-concept-456"
        val timestamp = System.currentTimeMillis()
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_HARVESTED)
            .setHarvestRunId("run-concept")
            .setUri("https://example.com/concept")
            .setFdkId(fdkId)
            .setGraph("<> a <http://www.w3.org/2004/02/skos/core#Concept> .")
            .setTimestamp(timestamp)
            .build()

        produceAvroEvent(topic, fdkId, event)

        val conceptDir = archiveRoot.resolve("concepts")
        val expectedFile = conceptDir.resolve("${timestamp}_${fdkId}.json")
        val content = awaitFileWithContent(expectedFile, fdkId, timeoutSeconds = 30)
        Assertions.assertThat(content)
            .withFailMessage("Archive file did not contain '%s' within 30s: %s", fdkId, expectedFile)
            .isNotNull
            .contains(fdkId)
            .contains("CONCEPT_HARVESTED")
            .contains("https://example.com/concept")
    }

    @Test
    fun `consumes DataServiceEvent from Kafka and saves as JSON file`() {
        val topic = "data-service-events"
        val fdkId = "integration-test-dataservice-789"
        val timestamp = System.currentTimeMillis()
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_HARVESTED)
            .setHarvestRunId("run-dataservice")
            .setUri("https://example.com/dataservice")
            .setFdkId(fdkId)
            .setGraph("<> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(timestamp)
            .build()

        produceAvroEvent(topic, fdkId, event)

        val dataserviceDir = archiveRoot.resolve("data_services")
        val expectedFile = dataserviceDir.resolve("${timestamp}_${fdkId}.json")
        val content = awaitFileWithContent(expectedFile, fdkId, timeoutSeconds = 30)
        Assertions.assertThat(content)
            .withFailMessage("Archive file did not contain '%s' within 30s: %s", fdkId, expectedFile)
            .isNotNull
            .contains(fdkId)
            .contains("DATA_SERVICE_HARVESTED")
            .contains("https://example.com/dataservice")
    }

    @Test
    fun `consumes InformationModelEvent from Kafka and saves as JSON file`() {
        val topic = "information-model-events"
        val fdkId = "integration-test-informationmodel-101"
        val timestamp = System.currentTimeMillis()
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_HARVESTED)
            .setHarvestRunId("run-informationmodel")
            .setUri("https://example.com/informationmodel")
            .setFdkId(fdkId)
            .setGraph("<> a <http://www.w3.org/ns/dcat#Dataset> .")
            .setTimestamp(timestamp)
            .build()

        produceAvroEvent(topic, fdkId, event)

        val informationmodelDir = archiveRoot.resolve("information_models")
        val expectedFile = informationmodelDir.resolve("${timestamp}_${fdkId}.json")
        val content = awaitFileWithContent(expectedFile, fdkId, timeoutSeconds = 30)
        Assertions.assertThat(content)
            .withFailMessage("Archive file did not contain '%s' within 30s: %s", fdkId, expectedFile)
            .isNotNull
            .contains(fdkId)
            .contains("INFORMATION_MODEL_HARVESTED")
            .contains("https://example.com/informationmodel")
    }

    @Test
    fun `consumes EventEvent from Kafka and saves as JSON file`() {
        val topic = "event-events"
        val fdkId = "integration-test-event-202"
        val timestamp = System.currentTimeMillis()
        val event = EventEvent.newBuilder()
            .setType(EventEventType.EVENT_HARVESTED)
            .setHarvestRunId("run-event")
            .setUri("https://example.com/event")
            .setFdkId(fdkId)
            .setGraph("<> a <http://schema.org/Event> .")
            .setTimestamp(timestamp)
            .build()

        produceAvroEvent(topic, fdkId, event)

        val eventDir = archiveRoot.resolve("events")
        val expectedFile = eventDir.resolve("${timestamp}_${fdkId}.json")
        val content = awaitFileWithContent(expectedFile, fdkId, timeoutSeconds = 30)
        Assertions.assertThat(content)
            .withFailMessage("Archive file did not contain '%s' within 30s: %s", fdkId, expectedFile)
            .isNotNull
            .contains(fdkId)
            .contains("EVENT_HARVESTED")
            .contains("https://example.com/event")
    }

    @Test
    fun `consumes ServiceEvent from Kafka and saves as JSON file`() {
        val topic = "service-events"
        val fdkId = "integration-test-service-303"
        val timestamp = System.currentTimeMillis()
        val event = ServiceEvent.newBuilder()
            .setType(ServiceEventType.SERVICE_HARVESTED)
            .setHarvestRunId("run-service")
            .setUri("https://example.com/service")
            .setFdkId(fdkId)
            .setGraph("<> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(timestamp)
            .build()

        produceAvroEvent(topic, fdkId, event)

        val serviceDir = archiveRoot.resolve("services")
        val expectedFile = serviceDir.resolve("${timestamp}_${fdkId}.json")
        val content = awaitFileWithContent(expectedFile, fdkId, timeoutSeconds = 30)
        Assertions.assertThat(content)
            .withFailMessage("Archive file did not contain '%s' within 30s: %s", fdkId, expectedFile)
            .isNotNull
            .contains(fdkId)
            .contains("SERVICE_HARVESTED")
            .contains("https://example.com/service")
    }

    private fun produceAvroEvent(topic: String, key: String, event: Any) {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "io.confluent.kafka.serializers.KafkaAvroSerializer",
            "schema.registry.url" to MOCK_SCHEMA_REGISTRY_URL,
        )
        KafkaProducer<String, Any>(props).use { producer ->
            producer.send(ProducerRecord(topic, key, event)).get(10, TimeUnit.SECONDS)
        }
    }

    private fun awaitFileWithContent(path: Path, expectedSubstring: String, timeoutSeconds: Long): String? {
        val deadline = System.currentTimeMillis() + timeoutSeconds * 1000
        val pollIntervalMs = 200L
        while (System.currentTimeMillis() < deadline) {
            if (path.toFile().exists()) {
                val content = try {
                    path.toFile().readText()
                } catch (_: Exception) {
                    null
                }
                if (content != null && content.contains(expectedSubstring)) return content
            }
            Thread.sleep(pollIntervalMs)
        }
        return null
    }
}
