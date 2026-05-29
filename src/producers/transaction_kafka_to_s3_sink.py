import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from src.producers.config import (
    AWS_REGION,
    FINSTREAM_BRONZE_BUCKET,
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    S3_TRANSACTIONS_BRONZE_PREFIX,
    TOPIC_TRANSACTIONS_RAW,
)
from src.producers.producer_utils import load_schema
from src.producers.config import TRANSACTION_SCHEMA_PATH


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_s3_key(prefix: str) -> str:
    now = utc_now()
    return (
        f"{prefix}/"
        f"event_date={now.date().isoformat()}/"
        f"hour={now.hour:02d}/"
        f"transactions_{int(time.time() * 1000)}.jsonl"
    )


def json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def flush_to_s3(
    s3_client,
    bucket: str,
    key: str,
    records: List[Dict[str, Any]],
) -> None:
    if not records:
        return

    body = "\n".join(
        json.dumps(record, ensure_ascii=False, default=json_default)
        for record in records
    ) + "\n"

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Uploaded {len(records)} records to s3://{bucket}/{key}")


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "transactions-s3-bronze-sink",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def create_deserializer() -> AvroDeserializer:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_str = load_schema(TRANSACTION_SCHEMA_PATH)

    return AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
    )


def run_sink(batch_size: int = 100, flush_interval_seconds: int = 10) -> None:
    if not FINSTREAM_BRONZE_BUCKET:
        raise ValueError("FINSTREAM_BRONZE_BUCKET is not set")

    consumer = create_consumer()
    deserializer = create_deserializer()
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    consumer.subscribe([TOPIC_TRANSACTIONS_RAW])

    buffer: List[Dict[str, Any]] = []
    last_flush = time.time()

    print(f"Consuming from Kafka topic: {TOPIC_TRANSACTIONS_RAW}")
    print(f"Writing to: s3://{FINSTREAM_BRONZE_BUCKET}/{S3_TRANSACTIONS_BRONZE_PREFIX}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                should_flush_by_time = (
                    buffer and time.time() - last_flush >= flush_interval_seconds
                )
                if should_flush_by_time:
                    key = build_s3_key(S3_TRANSACTIONS_BRONZE_PREFIX)
                    flush_to_s3(s3_client, FINSTREAM_BRONZE_BUCKET, key, buffer)
                    consumer.commit(asynchronous=False)
                    buffer.clear()
                    last_flush = time.time()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            record = deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )

            record["_kafka_topic"] = msg.topic()
            record["_kafka_partition"] = msg.partition()
            record["_kafka_offset"] = msg.offset()
            record["_s3_ingestion_time"] = utc_now().isoformat()

            buffer.append(record)

            if len(buffer) >= batch_size:
                key = build_s3_key(S3_TRANSACTIONS_BRONZE_PREFIX)
                flush_to_s3(s3_client, FINSTREAM_BRONZE_BUCKET, key, buffer)
                consumer.commit(asynchronous=False)
                buffer.clear()
                last_flush = time.time()

    except KeyboardInterrupt:
        print("Stopping sink...")

    finally:
        if buffer:
            key = build_s3_key(S3_TRANSACTIONS_BRONZE_PREFIX)
            flush_to_s3(s3_client, FINSTREAM_BRONZE_BUCKET, key, buffer)
            consumer.commit(asynchronous=False)

        consumer.close()
        print("Kafka to S3 sink stopped.")


if __name__ == "__main__":
    run_sink(batch_size=20, flush_interval_seconds=5)