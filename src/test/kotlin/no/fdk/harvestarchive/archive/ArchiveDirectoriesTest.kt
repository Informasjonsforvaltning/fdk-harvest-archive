package no.fdk.harvestarchive.archive

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

@Tag("unit")
class ArchiveDirectoriesTest {
    @Test
    fun `maps every archive type to its configured directory`(@TempDir tempDir: Path) {
        val directories = archiveDirectories(tempDir)

        ArchiveType.entries.forEach { type ->
            assertThat(directories[type]).isEqualTo(tempDir.resolve(type.metricTag).toString())
        }
    }
}
