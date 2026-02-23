#!/usr/bin/env python3
"""
Send a DatasetEvent to the dataset-events topic (Kafka + Schema Registry).

Install deps (optional, for running this script):
  pip install confluent-kafka fastavro requests

Usage:
  python3 scripts/send-dataset-event.py

Env:
  KAFKA_BOOTSTRAP_SERVERS  default localhost:9092
  SCHEMA_REGISTRY_URL      default http://localhost:8081
  KAFKA_TOPIC              default dataset-events
"""
import json
import os
import struct
import sys

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    sys.exit(1)
try:
    import fastavro
except ImportError:
    print("pip install fastavro", file=sys.stderr)
    sys.exit(1)
try:
    from confluent_kafka import Producer
except ImportError:
    print("pip install confluent-kafka", file=sys.stderr)
    sys.exit(1)


SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "dataset-events")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_PATH = os.path.join(REPO_ROOT, "kafka", "schemas", "no.fdk.dataset.DatasetEvent.avsc")
SUBJECT = "dataset-events-value"


def get_or_register_schema_id():
    with open(SCHEMA_PATH) as f:
        schema_str = f.read()
    schema = json.loads(schema_str)

    # Check existing
    r = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions/latest", timeout=10)
    if r.status_code == 200:
        return r.json()["id"], schema

    # Register
    r = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions",
        json={"schema": schema_str},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["id"], schema


def encode_avro_confluent(schema_id: int, schema: dict, record: dict) -> bytes:
    from io import BytesIO
    buf = BytesIO()
    buf.write(b"\x00")
    buf.write(struct.pack(">I", schema_id))
    writer = BytesIO()
    fastavro.schemaless_writer(writer, schema, record)
    buf.write(writer.getvalue())
    return buf.getvalue()


def default_event():
    return {
        "type": "DATASET_HARVESTED",
        "harvestRunId": None,
        "uri": None,
        "fdkId": "test-dataset-123",
        "graph": "<http://example.org/dataset/123> a <http://www.w3.org/ns/dcat#Dataset> .",
        "timestamp": int(__import__("time").time() * 1000),
    }


def main():
    if not sys.stdin.isatty():
        event = json.load(sys.stdin)
    else:
        event = default_event()

    schema_id, schema = get_or_register_schema_id()
    payload = encode_avro_confluent(schema_id, schema, event)

    key = event.get("fdkId", "test-key")
    if isinstance(key, str):
        key = key.encode("utf-8")

    conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)
    producer.produce(KAFKA_TOPIC, key=key, value=payload)
    producer.flush()
    print(f"Sent DatasetEvent to {KAFKA_TOPIC}: type={event.get('type')}, fdkId={event.get('fdkId')}", file=sys.stderr)


if __name__ == "__main__":
    main()
