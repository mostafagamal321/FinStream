import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_event_id() -> str:
    return str(uuid.uuid4())


def load_schema(schema_path: Path) -> str:
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    schema_text = schema_path.read_text(encoding="utf-8").strip()

    if not schema_text:
        raise ValueError(f"Schema file is empty: {schema_path}")

    return schema_text


def create_avro_producer(
    bootstrap_servers: str,
    schema_registry_url: str,
    schema_path: Path,
) -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    schema_str = load_schema(schema_path)

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
    )

    producer_config = {
        "bootstrap.servers": bootstrap_servers,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "acks": "all",
        "retries": 10,
        "retry.backoff.ms": 500,
        "linger.ms": 20,
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
    }

    return SerializingProducer(producer_config)


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")
        return

    print(
        f"Delivered topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )