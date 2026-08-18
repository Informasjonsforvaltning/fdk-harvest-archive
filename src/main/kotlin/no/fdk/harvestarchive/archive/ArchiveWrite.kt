package no.fdk.harvestarchive.archive

sealed class ArchiveWrite(open val archiveType: ArchiveType?) {
    data class Saved(override val archiveType: ArchiveType) : ArchiveWrite(archiveType)

    data class Skipped(override val archiveType: ArchiveType?) : ArchiveWrite(archiveType)
}
