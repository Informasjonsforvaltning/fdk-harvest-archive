package no.fdk.harvestarchive.archive

import no.fdk.concept.ConceptEventType
import no.fdk.dataservice.DataServiceEventType
import no.fdk.dataset.DatasetEventType
import no.fdk.event.EventEventType
import no.fdk.informationmodel.InformationModelEventType
import no.fdk.service.ServiceEventType

private const val DATASET_TOPIC_NAME = "dataset-events"
private const val CONCEPT_TOPIC_NAME = "concept-events"
private const val DATA_SERVICE_TOPIC_NAME = "data-service-events"
private const val INFORMATION_MODEL_TOPIC_NAME = "information-model-events"
private const val EVENT_TOPIC_NAME = "event-events"
private const val SERVICE_TOPIC_NAME = "service-events"

private const val DATASET_CIRCUIT_BREAKER = "dataset-archive-cb"
private const val CONCEPT_CIRCUIT_BREAKER = "concept-archive-cb"
private const val DATA_SERVICE_CIRCUIT_BREAKER = "dataservice-archive-cb"
private const val INFORMATION_MODEL_CIRCUIT_BREAKER = "informationmodel-archive-cb"
private const val EVENT_CIRCUIT_BREAKER = "event-archive-cb"
private const val SERVICE_CIRCUIT_BREAKER = "service-archive-cb"

private const val DATASET_LISTENER = "dataset-archive"
private const val CONCEPT_LISTENER = "concept-archive"
private const val DATA_SERVICE_LISTENER = "dataservice-archive"
private const val INFORMATION_MODEL_LISTENER = "informationmodel-archive"
private const val EVENT_LISTENER = "event-archive"
private const val SERVICE_LISTENER = "service-archive"

/**
 * Canonical mapping for each harvest archive resource type: Kafka topic, allowed event types,
 * circuit breaker id, listener id, and Prometheus metric tag.
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
        topicName = DATASET_TOPIC_NAME,
        circuitBreakerId = DATASET_CIRCUIT_BREAKER,
        listenerId = DATASET_LISTENER,
        allowedEventTypes =
        setOf(
            DatasetEventType.DATASET_HARVESTED.name,
            DatasetEventType.DATASET_REMOVED.name,
        ),
    ),
    CONCEPT(
        metricTag = "concepts",
        topicName = CONCEPT_TOPIC_NAME,
        circuitBreakerId = CONCEPT_CIRCUIT_BREAKER,
        listenerId = CONCEPT_LISTENER,
        allowedEventTypes =
        setOf(
            ConceptEventType.CONCEPT_HARVESTED.name,
            ConceptEventType.CONCEPT_REMOVED.name,
        ),
    ),
    DATA_SERVICE(
        metricTag = "data_services",
        topicName = DATA_SERVICE_TOPIC_NAME,
        circuitBreakerId = DATA_SERVICE_CIRCUIT_BREAKER,
        listenerId = DATA_SERVICE_LISTENER,
        allowedEventTypes =
        setOf(
            DataServiceEventType.DATA_SERVICE_HARVESTED.name,
            DataServiceEventType.DATA_SERVICE_REMOVED.name,
        ),
    ),
    INFORMATION_MODEL(
        metricTag = "information_models",
        topicName = INFORMATION_MODEL_TOPIC_NAME,
        circuitBreakerId = INFORMATION_MODEL_CIRCUIT_BREAKER,
        listenerId = INFORMATION_MODEL_LISTENER,
        allowedEventTypes =
        setOf(
            InformationModelEventType.INFORMATION_MODEL_HARVESTED.name,
            InformationModelEventType.INFORMATION_MODEL_REMOVED.name,
        ),
    ),
    EVENT(
        metricTag = "events",
        topicName = EVENT_TOPIC_NAME,
        circuitBreakerId = EVENT_CIRCUIT_BREAKER,
        listenerId = EVENT_LISTENER,
        allowedEventTypes =
        setOf(
            EventEventType.EVENT_HARVESTED.name,
            EventEventType.EVENT_REMOVED.name,
        ),
    ),
    SERVICE(
        metricTag = "services",
        topicName = SERVICE_TOPIC_NAME,
        circuitBreakerId = SERVICE_CIRCUIT_BREAKER,
        listenerId = SERVICE_LISTENER,
        allowedEventTypes =
        setOf(
            ServiceEventType.SERVICE_HARVESTED.name,
            ServiceEventType.SERVICE_REMOVED.name,
        ),
    ),
    ;

    fun allowsEventType(eventType: String): Boolean = eventType in allowedEventTypes

    companion object {
        const val TOPIC_DATASET = DATASET_TOPIC_NAME
        const val TOPIC_CONCEPT = CONCEPT_TOPIC_NAME
        const val TOPIC_DATA_SERVICE = DATA_SERVICE_TOPIC_NAME
        const val TOPIC_INFORMATION_MODEL = INFORMATION_MODEL_TOPIC_NAME
        const val TOPIC_EVENT = EVENT_TOPIC_NAME
        const val TOPIC_SERVICE = SERVICE_TOPIC_NAME

        const val CIRCUIT_BREAKER_DATASET = DATASET_CIRCUIT_BREAKER
        const val CIRCUIT_BREAKER_CONCEPT = CONCEPT_CIRCUIT_BREAKER
        const val CIRCUIT_BREAKER_DATA_SERVICE = DATA_SERVICE_CIRCUIT_BREAKER
        const val CIRCUIT_BREAKER_INFORMATION_MODEL = INFORMATION_MODEL_CIRCUIT_BREAKER
        const val CIRCUIT_BREAKER_EVENT = EVENT_CIRCUIT_BREAKER
        const val CIRCUIT_BREAKER_SERVICE = SERVICE_CIRCUIT_BREAKER

        const val LISTENER_DATASET = DATASET_LISTENER
        const val LISTENER_CONCEPT = CONCEPT_LISTENER
        const val LISTENER_DATA_SERVICE = DATA_SERVICE_LISTENER
        const val LISTENER_INFORMATION_MODEL = INFORMATION_MODEL_LISTENER
        const val LISTENER_EVENT = EVENT_LISTENER
        const val LISTENER_SERVICE = SERVICE_LISTENER

        private val byTopic = entries.associateBy { it.topicName }

        fun fromTopic(topic: String): ArchiveType? = byTopic[topic]
    }
}
