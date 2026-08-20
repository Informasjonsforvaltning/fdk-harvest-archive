package no.fdk.harvestarchive.archive

import java.nio.file.Path

internal fun archiveDirectories(tempDir: Path) = ArchiveDirectories(
    datasetDir = tempDir.resolve("datasets").toString(),
    conceptDir = tempDir.resolve("concepts").toString(),
    dataServiceDir = tempDir.resolve("data_services").toString(),
    informationModelDir = tempDir.resolve("information_models").toString(),
    eventDir = tempDir.resolve("events").toString(),
    serviceDir = tempDir.resolve("services").toString(),
)
