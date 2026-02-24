package no.fdk.fdk_harvest_archive.archive

import com.fasterxml.jackson.module.kotlin.readValue
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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

@Tag("unit")
class EventArchiveServiceTest {

    @Test
    fun `saveDataset creates directory and writes JSON file`(@TempDir tempDir: Path) {
        val service = EventArchiveService(
            datasetDir = tempDir.resolve("datasets").toString(),
            conceptDir = tempDir.resolve("concepts").toString(),
            dataServiceDir = tempDir.resolve("data_services").toString(),
            informationModelDir = tempDir.resolve("information_models").toString(),
            eventDir = tempDir.resolve("events").toString(),
            serviceDir = tempDir.resolve("services").toString(),
        )
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/dataset/1")
            .setFdkId("test-dataset-123")
            .setGraph("<http://example.org/dataset/123> a <http://www.w3.org/ns/dcat#Dataset> .")
            .setTimestamp(1700000000000L)
            .build()

        service.saveDataset(event)

        val expectedFile = tempDir.resolve("datasets").resolve("1700000000000_test-dataset-123.json")
        assertThat(expectedFile).exists().isRegularFile
        val content = expectedFile.toFile().readText()
        assertThat(content)
            .contains("test-dataset-123")
            .contains("1700000000000")
            .contains("DATASET_HARVESTED")
            .contains("run-1")
            .contains("https://example.com/dataset/1")
    }

    @Test
    fun `saveDataset creates subdirectory when path has multiple segments`(@TempDir tempDir: Path) {
        val datasetDir = tempDir.resolve("datasets").toString()
        val service = EventArchiveService(
            datasetDir = datasetDir,
            conceptDir = tempDir.resolve("concepts").toString(),
            dataServiceDir = tempDir.resolve("data_services").toString(),
            informationModelDir = tempDir.resolve("information_models").toString(),
            eventDir = tempDir.resolve("events").toString(),
            serviceDir = tempDir.resolve("services").toString(),
        )
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_REMOVED)
            .setFdkId("my-id")
            .setGraph("")
            .setTimestamp(1L)
            .build()

        service.saveDataset(event)

        val expectedFile = Path.of(datasetDir).resolve("1_my-id.json")
        assertThat(expectedFile).exists().isRegularFile
        assertThat(expectedFile.toFile().readText()).contains("my-id")
    }

    @Test
    fun `saveDataset writes valid JSON that can be read back`(@TempDir tempDir: Path) {
        val service = EventArchiveService(
            datasetDir = tempDir.resolve("datasets").toString(),
            conceptDir = tempDir.resolve("concepts").toString(),
            dataServiceDir = tempDir.resolve("data_services").toString(),
            informationModelDir = tempDir.resolve("information_models").toString(),
            eventDir = tempDir.resolve("events").toString(),
            serviceDir = tempDir.resolve("services").toString(),
        )
        val objectMapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_HARVESTED)
            .setFdkId("json-roundtrip")
            .setGraph("<> a <http://example.org/Dataset> .")
            .setTimestamp(42L)
            .build()

        service.saveDataset(event)

        val file = tempDir.resolve("datasets").resolve("42_json-roundtrip.json").toFile()
        val read = objectMapper.readValue<Map<String, Any?>>(file)
        assertThat(read["fdkId"]).isEqualTo("json-roundtrip")
        assertThat(read["timestamp"]).isEqualTo(42)
        assertThat(read["type"]).isEqualTo("DATASET_HARVESTED")
    }

    @Test
    fun `saveConcept writes concept event JSON to concept directory`(@TempDir tempDir: Path) {
        val conceptDir = tempDir.resolve("concepts").toString()
        val service = EventArchiveService(
            datasetDir = tempDir.resolve("datasets").toString(),
            conceptDir = conceptDir,
            dataServiceDir = tempDir.resolve("data_services").toString(),
            informationModelDir = tempDir.resolve("information_models").toString(),
            eventDir = tempDir.resolve("events").toString(),
            serviceDir = tempDir.resolve("services").toString(),
        )
        val event = ConceptEvent.newBuilder()
            .setType(ConceptEventType.CONCEPT_HARVESTED)
            .setHarvestRunId("run-2")
            .setUri("https://example.com/concept/1")
            .setFdkId("concept-1")
            .setGraph("<> a <http://example.org/Concept> .")
            .setTimestamp(100L)
            .build()

        service.saveConcept(event)

        val expectedFile = Path.of(conceptDir).resolve("100_concept-1.json")
        assertThat(expectedFile).exists().isRegularFile
        val content = expectedFile.toFile().readText()
        assertThat(content).contains("CONCEPT_HARVESTED").contains("concept-1")
    }

    @Test
    fun `saveDataService writes data service event JSON to data service directory`(@TempDir tempDir: Path) {
        val dataServiceDir = tempDir.resolve("data_services").toString()
        val service = EventArchiveService(
            datasetDir = tempDir.resolve("datasets").toString(),
            conceptDir = tempDir.resolve("concepts").toString(),
            dataServiceDir = dataServiceDir,
            informationModelDir = tempDir.resolve("information_models").toString(),
            eventDir = tempDir.resolve("events").toString(),
            serviceDir = tempDir.resolve("services").toString(),
        )
        val event = DataServiceEvent.newBuilder()
            .setType(DataServiceEventType.DATA_SERVICE_HARVESTED)
            .setHarvestRunId("run-ds")
            .setUri("https://example.com/dataservice/1")
            .setFdkId("dataservice-1")
            .setGraph("<> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(200L)
            .build()

        service.saveDataService(event)

        val expectedFile = Path.of(dataServiceDir).resolve("200_dataservice-1.json")
        assertThat(expectedFile).exists().isRegularFile
        val content = expectedFile.toFile().readText()
        assertThat(content).contains("DATA_SERVICE_HARVESTED").contains("dataservice-1")
    }

    @Test
    fun `saveInformationModel writes information model event JSON to information model directory`(@TempDir tempDir: Path) {
        val informationModelDir = tempDir.resolve("information_models").toString()
        val service = EventArchiveService(
            datasetDir = tempDir.resolve("datasets").toString(),
            conceptDir = tempDir.resolve("concepts").toString(),
            dataServiceDir = tempDir.resolve("data_services").toString(),
            informationModelDir = informationModelDir,
            eventDir = tempDir.resolve("events").toString(),
            serviceDir = tempDir.resolve("services").toString(),
        )
        val event = InformationModelEvent.newBuilder()
            .setType(InformationModelEventType.INFORMATION_MODEL_HARVESTED)
            .setHarvestRunId("run-im")
            .setUri("https://example.com/informationmodel/1")
            .setFdkId("informationmodel-1")
            .setGraph("<> a <http://www.w3.org/ns/dcat#Dataset> .")
            .setTimestamp(300L)
            .build()

        service.saveInformationModel(event)

        val expectedFile = Path.of(informationModelDir).resolve("300_informationmodel-1.json")
        assertThat(expectedFile).exists().isRegularFile
        val content = expectedFile.toFile().readText()
        assertThat(content).contains("INFORMATION_MODEL_HARVESTED").contains("informationmodel-1")
    }

    @Test
    fun `saveEvent writes event event JSON to event directory`(@TempDir tempDir: Path) {
        val eventDir = tempDir.resolve("events").toString()
        val service = EventArchiveService(
            datasetDir = tempDir.resolve("datasets").toString(),
            conceptDir = tempDir.resolve("concepts").toString(),
            dataServiceDir = tempDir.resolve("data_services").toString(),
            informationModelDir = tempDir.resolve("information_models").toString(),
            eventDir = eventDir,
            serviceDir = tempDir.resolve("services").toString(),
        )
        val event = EventEvent.newBuilder()
            .setType(EventEventType.EVENT_HARVESTED)
            .setHarvestRunId("run-ev")
            .setUri("https://example.com/event/1")
            .setFdkId("event-1")
            .setGraph("<> a <http://schema.org/Event> .")
            .setTimestamp(400L)
            .build()

        service.saveEvent(event)

        val expectedFile = Path.of(eventDir).resolve("400_event-1.json")
        assertThat(expectedFile).exists().isRegularFile
        val content = expectedFile.toFile().readText()
        assertThat(content).contains("EVENT_HARVESTED").contains("event-1")
    }

    @Test
    fun `saveService writes service event JSON to service directory`(@TempDir tempDir: Path) {
        val serviceDir = tempDir.resolve("services").toString()
        val service = EventArchiveService(
            datasetDir = tempDir.resolve("datasets").toString(),
            conceptDir = tempDir.resolve("concepts").toString(),
            dataServiceDir = tempDir.resolve("data_services").toString(),
            informationModelDir = tempDir.resolve("information_models").toString(),
            eventDir = tempDir.resolve("events").toString(),
            serviceDir = serviceDir,
        )
        val event = ServiceEvent.newBuilder()
            .setType(ServiceEventType.SERVICE_HARVESTED)
            .setHarvestRunId("run-svc")
            .setUri("https://example.com/service/1")
            .setFdkId("service-1")
            .setGraph("<> a <http://www.w3.org/ns/dcat#DataService> .")
            .setTimestamp(500L)
            .build()

        service.saveService(event)

        val expectedFile = Path.of(serviceDir).resolve("500_service-1.json")
        assertThat(expectedFile).exists().isRegularFile
        val content = expectedFile.toFile().readText()
        assertThat(content).contains("SERVICE_HARVESTED").contains("service-1")
    }
}
