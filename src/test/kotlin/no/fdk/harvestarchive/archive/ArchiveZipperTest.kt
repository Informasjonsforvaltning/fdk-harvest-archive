package no.fdk.harvestarchive.archive

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.fdk.harvestarchive.metrics.ArchiveMetrics
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions

@Tag("unit")
class ArchiveZipperTest {
    private lateinit var registry: SimpleMeterRegistry
    private lateinit var archiveMetrics: ArchiveMetrics

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        archiveMetrics = ArchiveMetrics(registry)
    }

    private fun zipperFor(tempDir: Path, zipThresholdBytes: Long = 10L * 1024 * 1024 * 1024, zipMaxFileCount: Int = 2000) = ArchiveZipper(
        datasetDir = tempDir.resolve("datasets").toString(),
        conceptDir = tempDir.resolve("concepts").toString(),
        dataServiceDir = tempDir.resolve("data_services").toString(),
        informationModelDir = tempDir.resolve("information_models").toString(),
        eventDir = tempDir.resolve("events").toString(),
        serviceDir = tempDir.resolve("services").toString(),
        zipThresholdBytes = zipThresholdBytes,
        zipMaxFileCount = zipMaxFileCount,
        archiveMetrics = archiveMetrics,
    )

    @Test
    fun `zipIfOverThreshold creates zip and deletes source files when over threshold`(@TempDir tempDir: Path) {
        val datasetDir = tempDir.resolve("datasets")
        Files.createDirectories(datasetDir)
        Files.writeString(datasetDir.resolve("1_abc.json"), """{"type":"DATASET_HARVESTED"}""")

        val zipper = zipperFor(tempDir)
        zipper.zipIfOverThreshold(ArchiveType.DATASET, datasetDir, thresholdBytes = 1L)

        val zips =
            Files
                .list(tempDir)
                .filter { it.fileName.toString().endsWith(".zip") }
                .toList()
        assertThat(zips).isNotEmpty()
        assertThat(Files.list(datasetDir).toList()).isEmpty()
    }

    @Test
    fun `zipIfOverThreshold does not create zip when under threshold`(@TempDir tempDir: Path) {
        val datasetDir = tempDir.resolve("datasets")
        Files.createDirectories(datasetDir)
        Files.writeString(datasetDir.resolve("1_abc.json"), """{"type":"DATASET_HARVESTED"}""")

        val zipper = zipperFor(tempDir)
        zipper.zipIfOverThreshold(ArchiveType.DATASET, datasetDir, thresholdBytes = 10L * 1024 * 1024 * 1024)

        val zips =
            Files
                .list(tempDir)
                .filter { it.fileName.toString().endsWith(".zip") }
                .toList()
        assertThat(zips).isEmpty()
    }

    @Test
    fun `zipIfOverThreshold records zip metrics on success`(@TempDir tempDir: Path) {
        val datasetDir = tempDir.resolve("datasets")
        Files.createDirectories(datasetDir)
        Files.writeString(datasetDir.resolve("1_abc.json"), """{"type":"DATASET_HARVESTED"}""")
        Files.writeString(datasetDir.resolve("2_def.json"), """{"type":"DATASET_REMOVED"}""")

        val zipper = zipperFor(tempDir)
        zipper.zipIfOverThreshold(ArchiveType.DATASET, datasetDir, thresholdBytes = 1L)

        assertThat(
            registry
                .counter("harvest_archive_zip_total", "type", "datasets", "status", "success")
                .count(),
        ).isEqualTo(1.0)
        assertThat(
            registry
                .summary("harvest_archive_zip_files", "type", "datasets")
                .totalAmount(),
        ).isEqualTo(2.0)
        assertThat(
            registry
                .summary("harvest_archive_zip_bytes", "type", "datasets")
                .totalAmount(),
        ).isGreaterThan(0.0)
        assertThat(
            registry
                .timer("harvest_archive_zip_time", "type", "datasets", "status", "success")
                .count(),
        ).isEqualTo(1L)
    }

    @Test
    fun `zipIfOverThreshold updates directory snapshot gauges when under threshold`(@TempDir tempDir: Path) {
        val datasetDir = tempDir.resolve("datasets")
        Files.createDirectories(datasetDir)
        Files.writeString(datasetDir.resolve("1_abc.json"), """{"data":"hello"}""")

        val zipper = zipperFor(tempDir)
        zipper.zipIfOverThreshold(ArchiveType.DATASET, datasetDir, thresholdBytes = 10L * 1024 * 1024 * 1024)

        val bytesGauge = registry.find("harvest_archive_dir_bytes").tag("type", "datasets").gauge()
        val filesGauge = registry.find("harvest_archive_dir_files").tag("type", "datasets").gauge()
        assertThat(bytesGauge?.value()).isGreaterThan(0.0)
        assertThat(filesGauge?.value()).isEqualTo(1.0)
    }

    @Test
    fun `zipIfOverThreshold refreshes directory snapshot gauges after zip`(@TempDir tempDir: Path) {
        val datasetDir = tempDir.resolve("datasets")
        Files.createDirectories(datasetDir)
        Files.writeString(datasetDir.resolve("1_abc.json"), """{"data":"hello"}""")
        Files.writeString(datasetDir.resolve("2_def.json"), """{"data":"world"}""")

        val zipper = zipperFor(tempDir)
        zipper.zipIfOverThreshold(ArchiveType.DATASET, datasetDir, thresholdBytes = 1L)

        val bytesGauge = registry.find("harvest_archive_dir_bytes").tag("type", "datasets").gauge()
        val filesGauge = registry.find("harvest_archive_dir_files").tag("type", "datasets").gauge()
        assertThat(bytesGauge?.value()).isEqualTo(0.0)
        assertThat(filesGauge?.value()).isEqualTo(0.0)
    }

    @Test
    fun `zipIfOverThreshold respects maxFileCount`(@TempDir tempDir: Path) {
        val datasetDir = tempDir.resolve("datasets")
        Files.createDirectories(datasetDir)
        repeat(5) { i ->
            Files.writeString(datasetDir.resolve("${i}_file.json"), """{"i":$i}""")
        }

        val zipper = zipperFor(tempDir)
        zipper.zipIfOverThreshold(ArchiveType.DATASET, datasetDir, thresholdBytes = 1L, maxFileCount = 2)

        val remainingFiles =
            Files
                .list(datasetDir)
                .filter { Files.isRegularFile(it) }
                .toList()
        assertThat(remainingFiles).hasSize(3)
        assertThat(
            registry.summary("harvest_archive_zip_files", "type", "datasets").totalAmount(),
        ).isEqualTo(2.0)
        assertThat(
            registry.find("harvest_archive_dir_files").tag("type", "datasets").gauge()?.value(),
        ).isEqualTo(3.0)
    }

    @Test
    fun `checkAndZipAll zips existing dirs over threshold and skips missing dirs`(@TempDir tempDir: Path) {
        val datasetDir = tempDir.resolve("datasets")
        val conceptDir = tempDir.resolve("concepts")
        Files.createDirectories(datasetDir)
        Files.createDirectories(conceptDir)
        Files.writeString(datasetDir.resolve("1_abc.json"), """{"type":"DATASET_HARVESTED"}""")
        Files.writeString(conceptDir.resolve("1_def.json"), """{"type":"CONCEPT_HARVESTED"}""")

        zipperFor(tempDir, zipThresholdBytes = 1L).checkAndZipAll()

        assertThat(Files.list(datasetDir).use { it.toList() }).isEmpty()
        assertThat(Files.list(conceptDir).use { it.toList() }).isEmpty()
        val zips = Files.list(tempDir).use { paths -> paths.filter { it.fileName.toString().endsWith(".zip") }.toList() }
        assertThat(zips).hasSize(2)
        assertThat(
            registry.counter("harvest_archive_zip_total", "type", "datasets", "status", "success").count(),
        ).isEqualTo(1.0)
        assertThat(
            registry.counter("harvest_archive_zip_total", "type", "concepts", "status", "success").count(),
        ).isEqualTo(1.0)
        assertThat(registry.find("harvest_archive_zip_total").tag("type", "services").counter()).isNull()
    }

    @Test
    fun `zipIfOverThreshold records zip error metrics when reading a file fails`(@TempDir tempDir: Path) {
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"))

        val datasetDir = tempDir.resolve("datasets")
        Files.createDirectories(datasetDir)
        val source = datasetDir.resolve("1_abc.json")
        Files.writeString(source, """{"type":"DATASET_HARVESTED"}""")
        Files.setPosixFilePermissions(source, PosixFilePermissions.fromString("---------"))

        try {
            zipperFor(tempDir).zipIfOverThreshold(ArchiveType.DATASET, datasetDir, thresholdBytes = 1L)
        } finally {
            Files.setPosixFilePermissions(source, PosixFilePermissions.fromString("rw-r--r--"))
        }

        assertThat(Files.exists(source)).isTrue()
        assertThat(
            registry.counter("harvest_archive_zip_total", "type", "datasets", "status", "error").count(),
        ).isEqualTo(1.0)
        assertThat(
            registry.timer("harvest_archive_zip_time", "type", "datasets", "status", "error").count(),
        ).isEqualTo(1L)
        assertThat(
            registry.find("harvest_archive_zip_total").tag("type", "datasets").tag("status", "success").counter()?.count() ?: 0.0,
        ).isEqualTo(0.0)
        assertThat(
            registry.find("harvest_archive_zip_files").tag("type", "datasets").summary()?.totalAmount() ?: 0.0,
        ).isEqualTo(0.0)
        assertThat(
            registry.find("harvest_archive_dir_files").tag("type", "datasets").gauge()?.value(),
        ).isEqualTo(1.0)
    }
}
