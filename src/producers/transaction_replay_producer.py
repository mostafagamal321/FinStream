import argparse
import time
from pathlib import Path
from typing import Optional

import pandas as pd

from src.producers.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TEST_IDENTITY_PATH,
    TEST_TRANSACTION_PATH,
    TOPIC_TRANSACTIONS_RAW,
    TRAIN_IDENTITY_PATH,
    TRAIN_TRANSACTION_PATH,
    TRANSACTION_SCHEMA_PATH,
)
from src.producers.producer_utils import create_avro_producer, delivery_report
from src.producers.transaction_mapper import map_ieee_row_to_transaction_event


def validate_file_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


def get_paths(split: str) -> tuple[Path, Path]:
    if split == "train":
        return TRAIN_TRANSACTION_PATH, TRAIN_IDENTITY_PATH

    if split == "test":
        return TEST_TRANSACTION_PATH, TEST_IDENTITY_PATH

    raise ValueError("split must be either 'train' or 'test'")


def load_identity_data(identity_path: Path) -> pd.DataFrame:
    validate_file_exists(identity_path)

    identity = pd.read_csv(identity_path)

    if "TransactionID" not in identity.columns:
        raise ValueError(f"TransactionID column not found in {identity_path}")

    return identity


def replay_transactions(
    split: str,
    rate: int,
    limit: Optional[int],
    chunk_size: int,
) -> None:
    if rate <= 0:
        raise ValueError("rate must be greater than zero")

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

    transaction_path, identity_path = get_paths(split)

    validate_file_exists(transaction_path)
    validate_file_exists(identity_path)

    identity = load_identity_data(identity_path)

    producer = create_avro_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        schema_path=TRANSACTION_SCHEMA_PATH,
    )

    sleep_seconds = 1 / rate
    produced_count = 0

    print(f"Reading transactions from: {transaction_path}")
    print(f"Reading identity from: {identity_path}")
    print(f"Publishing to Kafka topic: {TOPIC_TRANSACTIONS_RAW}")
    print(f"Split: {split}")
    print(f"Rate: {rate} events/second")
    print(f"Chunk size: {chunk_size}")
    print(f"Limit: {limit if limit is not None else 'no limit'}")

    for chunk in pd.read_csv(transaction_path, chunksize=chunk_size):
        if limit is not None:
            remaining = limit - produced_count
            if remaining <= 0:
                break

            chunk = chunk.head(remaining)

        merged = chunk.merge(
            identity,
            on="TransactionID",
            how="left",
        )

        for _, row in merged.iterrows():
            event = map_ieee_row_to_transaction_event(
                row=row.to_dict(),
                split=split,
            )

            producer.produce(
                topic=TOPIC_TRANSACTIONS_RAW,
                key=event["transaction_id"],
                value=event,
                on_delivery=delivery_report,
            )

            producer.poll(0)
            produced_count += 1

            if produced_count % 1000 == 0:
                print(f"Produced {produced_count} events")

            time.sleep(sleep_seconds)

            if limit is not None and produced_count >= limit:
                break

        producer.flush()

        if limit is not None and produced_count >= limit:
            break

    print(f"Finished publishing {produced_count} transaction events.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay IEEE-CIS transaction CSV rows into Kafka."
    )

    parser.add_argument(
        "--split",
        choices=["train", "test"],
        default="train",
        help="Dataset split to replay.",
    )

    parser.add_argument(
        "--rate",
        type=int,
        default=10,
        help="Events per second.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of rows to replay. If omitted, replay all rows.",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Number of CSV rows to read per chunk.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    replay_transactions(
        split=args.split,
        rate=args.rate,
        limit=args.limit,
        chunk_size=args.chunk_size,
    )


if __name__ == "__main__":
    main()

    