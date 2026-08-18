package no.fdk.harvestarchive.archive

import no.fdk.concept.ConceptEventType
import no.fdk.dataservice.DataServiceEventType
import no.fdk.dataset.DatasetEventType
import no.fdk.event.EventEventType
import no.fdk.informationmodel.InformationModelEventType
import no.fdk.service.ServiceEventType

/**
 * Canonical mapping for each harvest archive resource type: Kafka topic, archive directory,
 * allowed event types, circuit breaker id, listener id, and Prometheus metric tag.
 */
enum class ArchiveType(
    val metricTag: String,
    val topicName: String,
    val circuitBreakerId: String,
    val listenerId: String,
    val allowedEventTypes: Set<String>,
) {
    DATASET(
        metricTag = "datasets",
        topicName = "dataset-events",
        circuitBreakerId = "dataset-archive-cb",
        listenerId = "dataset-archive",
        allowedEventTypes =
        setOf(
            DatasetEventType.DATASET_HARVESTED.name,
            DatasetEventType.DATASET_REMOVED.name,
        ),
    ),
    CONCEPT(
        metricTag = "concepts",
        topicName = "concept-events",
        circuitBreakerId = "concept-archive-cb",
        listenerId = "concept-archive",
        allowedEventTypes =
        setOf(
            ConceptEventType.CONCEPT_HARVESTED.name,
            ConceptEventType.CONCEPT_REMOVED.name,
        ),
    ),
    DATA_SERVICE(
        metricTag = "data_services",
        topicName = "data-service-events",
        circuitBreakerId = "dataservice-archive-cb",
        listenerId = "dataservice-archive",
        allowedEventTypes =
        setOf(
            DataServiceEventType.DATA_SERVICE_HARVESTED.name,
            DataServiceEventType.DATA_SERVICE_REMOVED.name,
        ),
    ),
    INFORMATION_MODEL(
        metricTag = "information_models",
        topicName = "information-model-events",
        circuitBreakerId = "informationmodel-archive-cb",
        listenerId = "informationmodel-archive",
        allowedEventTypes =
        setOf(
            InformationModelEventType.INFORMATION_MODEL_HARVESTED.name,
            InformationModelEventType.INFORMATION_MODEL_REMOVED.name,
        ),
    ),
    EVENT(
        metricTag = "events",
        topicName = "event-events",
        circuitBreakerId = "event-archive-cb",
        listenerId = "event-archive",
        allowedEventTypes =
        setOf(
            EventEventType.EVENT_HARVESTED.name,
            EventEventType.EVENT_REMOVED.name,
        ),
    ),
    SERVICE(
        metricTag = "services",
        topicName = "service-events",
        circuitBreakerId = "service-archive-cb",
        listenerId = "service-archive",
        allowedEventTypes =
        setOf(
            ServiceEventType.SERVICE_HARVESTED.name,
            ServiceEventType.SERVICE_REMOVED.name,
        ),
    ),
    ;

    fun allowsEventType(eventType: String): Boolean = eventType in allowedEventTypes

    companion object {
        private val byTopic = entries.associateBy { it.topicName }

        fun fromTopic(topic: String): ArchiveType? = byTopic[topic]
    }
}
