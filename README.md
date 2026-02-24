# FDK Harvest Archive

Spring Boot application that consumes harvest event messages from Kafka and archives them as JSON files.

## What it does

- **Consumes** Avro events from six Kafka topics:
  - `dataset-events` → DatasetEvent (DATASET_HARVESTED, DATASET_REMOVED)
  - `concept-events` → ConceptEvent (CONCEPT_HARVESTED, CONCEPT_REMOVED)
  - `data-service-events` → DataServiceEvent (DATA_SERVICE_HARVESTED, DATA_SERVICE_REMOVED)
  - `information-model-events` → InformationModelEvent (INFORMATION_MODEL_HARVESTED, INFORMATION_MODEL_REMOVED)
  - `event-events` → EventEvent (EVENT_HARVESTED, EVENT_REMOVED)
  - `service-events` → ServiceEvent (SERVICE_HARVESTED, SERVICE_REMOVED)
- **Filters** by event type: only `*_HARVESTED` and `*_REMOVED` are archived; `*_REASONED` is acknowledged and skipped.
- **Archives** each event as a JSON file `{timestamp}_{fdkId}.json` under the corresponding archive directory.

## Configuration

| Environment variable | Default | Description |
|-----------------------|---------|-------------|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `SPRING_KAFKA_CONSUMER_PROPERTIES_SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry URL |
| `SPRING_KAFKA_LISTENER_AUTO_STARTUP` | `false` | Start listeners on boot (set `true` if Kafka is up at startup) |
| `APP_ARCHIVE_DATASET_DIR` | `archive/datasets` | Directory for dataset event JSON files |
| `APP_ARCHIVE_CONCEPT_DIR` | `archive/concepts` | Directory for concept event JSON files |
| `APP_ARCHIVE_DATASERVICE_DIR` | `archive/data_services` | Directory for data service event JSON files |
| `APP_ARCHIVE_INFORMATIONMODEL_DIR` | `archive/information_models` | Directory for information model event JSON files |
| `APP_ARCHIVE_EVENT_DIR` | `archive/events` | Directory for event event JSON files |
| `APP_ARCHIVE_SERVICE_DIR` | `archive/services` | Directory for service event JSON files |

Topic names can be overridden via `APP_KAFKA_TOPIC_*` (see `application.yml`).

## Running locally

1. **Start Kafka and Schema Registry** (creates topics and exposes Kafka on 9092, Schema Registry on 8081):

   ```bash
   docker compose up -d kafka schema-registry kafka-init
   ```

2. **Run the application** (optional: set `SPRING_KAFKA_LISTENER_AUTO_STARTUP=true` if Kafka is already up):

   ```bash
   mvn spring-boot:run -Dspring-boot.run.profiles=develop
   ```

   Or run the built JAR:

   ```bash
   mvn package -DskipTests
   java -jar target/fdk-harvest-archive.jar
   ```

3. **Health and metrics**: `http://localhost:8080/actuator/health`, `http://localhost:8080/actuator/prometheus`.

## Building and testing

- **Unit tests** (no Docker, tag `unit`):

  ```bash
  mvn test
  ```

- **Integration tests** (Testcontainers Kafka, tag `integration`):

  ```bash
  mvn verify
  ```

  Requires Docker. Integration tests use a single Kafka container and a mock Schema Registry; they produce one event per type and assert the corresponding JSON file is written.

## Project layout

- `src/main/kotlin/` – Application code
  - `archive/EventArchiveService.kt` – Writes event payloads to JSON files under configured dirs
  - `kafka/` – Kafka config, listeners (one per event type), circuit breakers, and pause/resume via `KafkaManager`
  - `config/CircuitBreakerConsumerConfiguration.kt` – Subscribes to circuit breaker state and pauses/resumes the matching Kafka listener
- `kafka/schemas/` – Avro schemas (`.avsc`) for each event type
- `docker-compose.yml` – Kafka, Schema Registry, and topic creation for local runs
- `scripts/` – Optional helpers (e.g. register schema, send test events)
