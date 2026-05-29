import argparse
import json
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src.producers.config import (
    AWS_REGION,
    FINSTREAM_BRONZE_BUCKET,
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    S3_TRANSACTIONS_BRONZE_PREFIX,
    TOPIC_TRANSACTIONS_RAW,
    TRANSACTION_SCHEMA_PATH,
)
from src.producers.producer_utils import load_schema


SHOULD_STOP = False


def handle_shutdown(signum, frame) -> None:
    global SHOULD_STOP
    SHOULD_STOP = True
    print("Shutdown signal received. Finishing current batch safely...")


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def build_s3_key(prefix: str, run_id: str, file_sequence: int) -> str:
    now = utc_now()
    return (
        f"{prefix}/"
        f"event_date={now.date().isoformat()}/"
        f"hour={now.hour:02d}/"
        f"run_id={run_id}/"
        f"part-{file_sequence:06d}.jsonl"
    )


def create_s3_client():
    boto_config = Config(
        region_name=AWS_REGION,
        retries={
            "max_attempts": 10,
            "mode": "adaptive",
        },
        connect_timeout=60,
        read_timeout=300,
        s3={
            "addressing_style": "path",
        },
    )

    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        config=boto_config,
        endpoint_url=f"https://s3.{AWS_REGION}.amazonaws.com",
    )


def create_consumer(group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 900000,
            "session.timeout.ms": 45000,
        }
    )


def create_deserializer() -> AvroDeserializer:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_str = load_schema(TRANSACTION_SCHEMA_PATH)

    return AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
    )


def serialize_records(records: List[Dict[str, Any]]) -> bytes:
    body = "\n".join(
        json.dumps(record, ensure_ascii=False, default=json_default)
        for record in records
    ) + "\n"

    return body.encode("utf-8")


def put_object_with_retry(
    s3_client,
    bucket: str,
    key: str,
    body: bytes,
    max_attempts: int,
    base_sleep_seconds: int,
) -> None:
    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType="application/x-ndjson",
            )
            return

        except (EndpointConnectionError, BotoCoreError, ClientError) as error:
            last_error = error
            sleep_seconds = min(base_sleep_seconds * (2 ** (attempt - 1)), 120)

            print(
                f"S3 upload failed attempt={attempt}/{max_attempts}. "
                f"key=s3://{bucket}/{key}. "
                f"retrying_in={sleep_seconds}s. "
                f"error={error}"
            )

            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"Failed to upload s3://{bucket}/{key} after {max_attempts} attempts"
    ) from last_error


def flush_to_s3(
    s3_client,
    bucket: str,
    key: str,
    records: List[Dict[str, Any]],
    max_s3_attempts: int,
) -> None:
    if not records:
        return

    body = serialize_records(records)

    put_object_with_retry(
        s3_client=s3_client,
        bucket=bucket,
        key=key,
        body=body,
        max_attempts=max_s3_attempts,
        base_sleep_seconds=5,
    )

    print(f"Uploaded {len(records)} records to s3://{bucket}/{key}")


def run_sink(
    batch_size: int,
    flush_interval_seconds: int,
    max_s3_attempts: int,
    group_id: str,
) -> None:
    if not FINSTREAM_BRONZE_BUCKET:
        raise ValueError("FINSTREAM_BRONZE_BUCKET is not set")

    run_id = utc_now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    file_sequence = 0

    consumer = create_consumer(group_id=group_id)
    deserializer = create_deserializer()
    s3_client = create_s3_client()

    consumer.subscribe([TOPIC_TRANSACTIONS_RAW])

    buffer: List[Dict[str, Any]] = []
    last_flush = time.time()
    total_uploaded = 0

    print(f"Consuming from Kafka topic: {TOPIC_TRANSACTIONS_RAW}")
    print(f"Consumer group: {group_id}")
    print(f"Writing to: s3://{FINSTREAM_BRONZE_BUCKET}/{S3_TRANSACTIONS_BRONZE_PREFIX}")
    print(f"Run ID: {run_id}")
    print(f"Batch size: {batch_size}")

    try:
        while not SHOULD_STOP:
            msg = consumer.poll(1.0)

            if msg is None:
                should_flush_by_time = (
                    buffer and time.time() - last_flush >= flush_interval_seconds
                )

                if should_flush_by_time:
                    file_sequence += 1
                    key = build_s3_key(
                        prefix=S3_TRANSACTIONS_BRONZE_PREFIX,
                        run_id=run_id,
                        file_sequence=file_sequence,
                    )

                    flush_to_s3(
                        s3_client=s3_client,
                        bucket=FINSTREAM_BRONZE_BUCKET,
                        key=key,
                        records=buffer,
                        max_s3_attempts=max_s3_attempts,
                    )

                    consumer.commit(asynchronous=False)
                    total_uploaded += len(buffer)
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
            record["_sink_run_id"] = run_id

            buffer.append(record)

            if len(buffer) >= batch_size:
                file_sequence += 1
                key = build_s3_key(
                    prefix=S3_TRANSACTIONS_BRONZE_PREFIX,
                    run_id=run_id,
                    file_sequence=file_sequence,
                )

                flush_to_s3(
                    s3_client=s3_client,
                    bucket=FINSTREAM_BRONZE_BUCKET,
                    key=key,
                    records=buffer,
                    max_s3_attempts=max_s3_attempts,
                )

                consumer.commit(asynchronous=False)
                total_uploaded += len(buffer)
                print(f"Total uploaded records: {total_uploaded}")

                buffer.clear()
                last_flush = time.time()

    finally:
        if buffer:
            file_sequence += 1
            key = build_s3_key(
                prefix=S3_TRANSACTIONS_BRONZE_PREFIX,
                run_id=run_id,
                file_sequence=file_sequence,
            )

            flush_to_s3(
                s3_client=s3_client,
                bucket=FINSTREAM_BRONZE_BUCKET,
                key=key,
                records=buffer,
                max_s3_attempts=max_s3_attempts,
            )

            consumer.commit(asynchronous=False)
            total_uploaded += len(buffer)
            print(f"Final upload completed. Total uploaded records: {total_uploaded}")

        consumer.close()
        print("Kafka to S3 sink stopped safely.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume transaction events from Kafka and write Bronze JSONL files to S3."
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Number of Kafka records per S3 JSONL file.",
    )

    parser.add_argument(
        "--flush-interval-seconds",
        type=int,
        default=30,
        help="Flush partial batch after this many seconds.",
    )

    parser.add_argument(
        "--max-s3-attempts",
        type=int,
        default=20,
        help="Maximum retry attempts per S3 object upload.",
    )

    parser.add_argument(
        "--group-id",
        type=str,
        default="transactions-s3-bronze-sink",
        help="Kafka consumer group ID.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_sink(
        batch_size=args.batch_size,
        flush_interval_seconds=args.flush_interval_seconds,
        max_s3_attempts=args.max_s3_attempts,
        group_id=args.group_id,
    )


if __name__ == "__main__":
    main()