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


def load_ieee_data(split: str, limit: Optional[int]) -> pd.DataFrame:
    transaction_path, identity_path = get_paths(split)

    validate_file_exists(transaction_path)
    validate_file_exists(identity_path)

    transactions = pd.read_csv(transaction_path, nrows=limit)
    identity = pd.read_csv(identity_path)

    merged = transactions.merge(
        identity,
        on="TransactionID",
        how="left",
    )

    return merged


def replay_transactions(split: str, rate: int, limit: Optional[int]) -> None:
    if rate <= 0:
        raise ValueError("rate must be greater than zero")

    data = load_ieee_data(split=split, limit=limit)

    producer = create_avro_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        schema_path=TRANSACTION_SCHEMA_PATH,
    )

    sleep_seconds = 1 / rate

    print(f"Publishing {len(data)} {split} transactions to {TOPIC_TRANSACTIONS_RAW}")
    print(f"Rate: {rate} events/second")

    for _, row in data.iterrows():
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
        time.sleep(sleep_seconds)

    producer.flush()
    print("Finished publishing transaction events.")


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
        default=1000,
        help="Maximum number of rows to replay.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    replay_transactions(
        split=args.split,
        rate=args.rate,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()