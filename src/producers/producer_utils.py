import json
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer


def load_avro_schema(schema_path: Path) -> str:
    """
    Load an Avro schema file as a string.
    Schema Registry expects the schema body as text.
    """
    schema_path = Path(schema_path)

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    return schema_path.read_text(encoding="utf-8")


def delivery_report(err, msg) -> None:
    """
    Kafka delivery callback.
    Called after Kafka accepts or rejects a message.
    """
    if err is not None:
        print(f"[KAFKA DELIVERY FAILED] {err}")
        return

    print(
        "[KAFKA DELIVERED] "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def create_avro_producer(
    bootstrap_servers: str,
    schema_registry_url: str,
    schema_path: Path,
) -> SerializingProducer:
    """
    Create a Kafka producer that serializes message values using Avro
    and registers/validates schemas through Confluent Schema Registry.
    """
    schema_registry_client = SchemaRegistryClient(
        {
            "url": schema_registry_url,
        }
    )

    avro_schema = load_avro_schema(schema_path)

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=avro_schema,
        conf={
            "auto.register.schemas": True,
        },
    )

    producer_config = {
        "bootstrap.servers": bootstrap_servers,

        # Avro value serialization
        "value.serializer": avro_serializer,

        # Kafka message key should be bytes
        "key.serializer": lambda key, ctx: key.encode("utf-8") if key else None,

        # Safer producer settings
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 10,
    }

    return SerializingProducer(producer_config)


def create_json_producer(bootstrap_servers: str) -> Producer:
    """
    Plain JSON producer.
    Useful for DLQ messages or quick debugging topics.
    """
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
        }
    )


def send_to_dlq(
    bootstrap_servers: str,
    dlq_topic: str,
    failed_record: dict[str, Any],
    error_message: str,
    source: str,
) -> None:
    """
    Send failed events to the Dead Letter Queue as JSON.

    Later, if you want the DLQ to also use Avro, we can modify this
    to use DLQ_SCHEMA_PATH and create_avro_producer().
    """
    producer = create_json_producer(bootstrap_servers)

    dlq_event = {
        "source": source,
        "error_message": error_message,
        "failed_record": failed_record,
    }

    producer.produce(
        topic=dlq_topic,
        key=source.encode("utf-8"),
        value=json.dumps(dlq_event, default=str).encode("utf-8"),
        callback=delivery_report,
    )

    producer.flush()