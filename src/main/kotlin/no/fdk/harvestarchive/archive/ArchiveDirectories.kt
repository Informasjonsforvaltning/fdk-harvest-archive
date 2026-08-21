package no.fdk.harvestarchive.archive

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * Runtime archive directory paths keyed by [ArchiveType].
 */
@Component
class ArchiveDirectories(
    @Value($$"${app.archive.dataset-dir}") datasetDir: String,
    @Value($$"${app.archive.concept-dir}") conceptDir: String,
    @Value($$"${app.archive.data-service-dir}") dataServiceDir: String,
    @Value($$"${app.archive.information-model-dir}") informationModelDir: String,
    @Value($$"${app.archive.event-dir}") eventDir: String,
    @Value($$"${app.archive.service-dir}") serviceDir: String,
) {
    private val dirs: Map<ArchiveType, String> =
        mapOf(
            ArchiveType.DATASET to datasetDir,
            ArchiveType.CONCEPT to conceptDir,
            ArchiveType.DATA_SERVICE to dataServiceDir,
            ArchiveType.INFORMATION_MODEL to informationModelDir,
            ArchiveType.EVENT to eventDir,
            ArchiveType.SERVICE to serviceDir,
        )

    init {
        check(dirs.keys == ArchiveType.entries.toSet()) {
            "ArchiveDirectories must map every ArchiveType"
        }
    }

    operator fun get(type: ArchiveType): String = dirs.getValue(type)

    fun forEach(action: (ArchiveType, String) -> Unit) {
        dirs.forEach { (type, dir) -> action(type, dir) }
    }
}
