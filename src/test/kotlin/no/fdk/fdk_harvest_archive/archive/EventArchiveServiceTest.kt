package no.fdk.fdk_harvest_archive.archive

import com.fasterxml.jackson.module.kotlin.readValue
import no.fdk.dataset.DatasetEvent
import no.fdk.dataset.DatasetEventType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

@Tag("unit")
class EventArchiveServiceTest {

    @Test
    fun `saveDataset creates directory and writes JSON file`(@TempDir tempDir: Path) {
        val service = EventArchiveService(tempDir.toString())
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_HARVESTED)
            .setHarvestRunId("run-1")
            .setUri("https://example.com/dataset/1")
            .setFdkId("test-dataset-123")
            .setGraph("<http://example.org/dataset/123> a <http://www.w3.org/ns/dcat#Dataset> .")
            .setTimestamp(1700000000000L)
            .build()

        service.saveDataset(event)

        val expectedFile = tempDir.resolve("1700000000000_test-dataset-123.json")
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
        val datasetDir = tempDir.resolve("archive").resolve("datasets").toString()
        val service = EventArchiveService(datasetDir)
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
        val service = EventArchiveService(tempDir.toString())
        val objectMapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
        val event = DatasetEvent.newBuilder()
            .setType(DatasetEventType.DATASET_HARVESTED)
            .setFdkId("json-roundtrip")
            .setGraph("<> a <http://example.org/Dataset> .")
            .setTimestamp(42L)
            .build()

        service.saveDataset(event)

        val file = tempDir.resolve("42_json-roundtrip.json").toFile()
        val read = objectMapper.readValue<Map<String, Any?>>(file)
        assertThat(read["fdkId"]).isEqualTo("json-roundtrip")
        assertThat(read["timestamp"]).isEqualTo(42)
        assertThat(read["type"]).isEqualTo("DATASET_HARVESTED")
    }
}
