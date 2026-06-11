import argparse
import os
import time
from pathlib import Path

import pandas as pd

from src.producers.producer_utils import (
    create_avro_producer,
    delivery_report,
    new_event_id,
    utc_now_iso,
)


def get_env(name: str, default: str) -> str:
    return os.getenv(name, default)


def normalize_value(value):
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value


def to_nullable_string(value):
    value = normalize_value(value)
    if value is None:
        return None
    return str(value)


def to_required_string(value, default: str) -> str:
    value = normalize_value(value)
    if value is None or str(value).strip() == "":
        return default
    return str(value)


def to_nullable_double(value):
    value = normalize_value(value)
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_required_double(value, default: float = 0.0) -> float:
    parsed = to_nullable_double(value)
    if parsed is None:
        return default
    return parsed


def to_required_long(value, default: int = 0) -> int:
    value = normalize_value(value)
    if value is None:
        return default

    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def to_nullable_int(value):
    value = normalize_value(value)
    if value is None:
        return None

    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def build_card_id(row: pd.Series, transaction_id: str) -> str:
    card_parts = [
        normalize_value(row.get("card1")),
        normalize_value(row.get("card2")),
        normalize_value(row.get("card3")),
        normalize_value(row.get("card4")),
        normalize_value(row.get("card5")),
        normalize_value(row.get("card6")),
    ]

    usable_parts = [str(part) for part in card_parts if part is not None]

    if not usable_parts:
        return f"card_unknown_{transaction_id}"

    return "card_" + "_".join(usable_parts)


def build_customer_id(row: pd.Series, transaction_id: str) -> str:
    # IEEE-CIS identity fields are not always present for every transaction.
    identity_value = normalize_value(row.get("id_01"))

    if identity_value is not None:
        return f"cust_{identity_value}"

    # Stable fallback so every transaction has a customer_id.
    return f"cust_{transaction_id}"


def build_merchant_id(row: pd.Series) -> str:
    addr1 = normalize_value(row.get("addr1"))

    if addr1 is not None:
        return f"merch_{addr1}"

    return "merch_unknown"


def build_transaction_event(row: pd.Series, split: str) -> dict:
    transaction_id = to_required_string(row.get("TransactionID"), "unknown_transaction")

    now_iso = utc_now_iso()

    event = {
        "event_id": new_event_id(),
        "transaction_id": transaction_id,

        "event_time": now_iso,
        "ingestion_time": now_iso,
        "source": "ieee_cis",

        "split": split,
        "transaction_dt": to_required_long(row.get("TransactionDT"), 0),

        "amount": to_required_double(row.get("TransactionAmt"), 0.0),
        "currency": "USD",
        "product_cd": to_nullable_string(row.get("ProductCD")),

        "customer_id": build_customer_id(row, transaction_id),
        "card_id": build_card_id(row, transaction_id),
        "merchant_id": build_merchant_id(row),

        "card_brand": to_nullable_string(row.get("card4")),
        "card_type": to_nullable_string(row.get("card6")),
        "addr1": to_nullable_double(row.get("addr1")),
        "addr2": to_nullable_double(row.get("addr2")),

        "payer_email_domain": to_nullable_string(row.get("P_emaildomain")),
        "receiver_email_domain": to_nullable_string(row.get("R_emaildomain")),

        "device_type": to_nullable_string(row.get("DeviceType")),
        "device_info": to_nullable_string(row.get("DeviceInfo")),

        "is_fraud": to_nullable_int(row.get("isFraud")),
    }

    return event


def load_paths(split: str) -> tuple[Path, Path | None]:
    base_dir = Path("data/raw/ieee_fraud")

    if split == "train":
        transaction_path = base_dir / "train_transaction.csv"
        identity_path = base_dir / "train_identity.csv"
    else:
        transaction_path = base_dir / "test_transaction.csv"
        identity_path = base_dir / "test_identity.csv"

    if not transaction_path.exists():
        raise FileNotFoundError(f"Transaction file not found: {transaction_path}")

    if not identity_path.exists():
        identity_path = None

    return transaction_path, identity_path


def resolve_schema_path() -> Path:
    configured = get_env(
        "TRANSACTION_SCHEMA_PATH",
        "schemas/transaction/transaction_event.avsc",
    )

    schema_path = Path(configured)

    if schema_path.exists():
        return schema_path

    fallback_candidates = [
        Path("schemas/transaction/transaction_event.avsc"),
        Path("schemas/transactions.avsc"),
        Path("schemas/transaction.avsc"),
    ]

    for candidate in fallback_candidates:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "Could not find transaction Avro schema. "
        "Set TRANSACTION_SCHEMA_PATH in .env or rename schema to schemas/transactions_event.avsc"
    )

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay IEEE-CIS transaction CSV rows into Kafka using Avro + Schema Registry."
    )

    parser.add_argument(
        "--split",
        choices=["train", "test"],
        default="train",
        help="Dataset split to replay.",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Events per second. Use 0 for no sleep.",
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

    parser.add_argument(
        "--start-row",
        type=int,
        default=0,
        help="Number of rows to skip before publishing.",
    )

    args = parser.parse_args()

    if args.start_row < 0:
        raise ValueError("--start-row must be >= 0")

    if args.limit is not None and args.limit < 0:
        raise ValueError("--limit must be >= 0")

    topic = get_env("TOPIC_TRANSACTIONS_RAW", "transactions_raw")
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    schema_registry_url = get_env("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    schema_path = resolve_schema_path()

    transaction_path, identity_path = load_paths(args.split)

    print(f"Reading transactions from: {transaction_path}")

    if identity_path is not None:
        print(f"Reading identity from: {identity_path}")
    else:
        print("Identity file not found. Continuing without identity join.")

    print(f"Publishing to Kafka topic: {topic}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Schema Registry URL: {schema_registry_url}")
    print(f"Schema path: {schema_path}")
    print(f"Split: {args.split}")
    print(f"Rate: {args.rate} events/second")
    print(f"Chunk size: {args.chunk_size}")
    print(f"Start row: {args.start_row}")
    print(f"Limit: {args.limit if args.limit is not None else 'ALL'}")

    identity_df = None

    if identity_path is not None:
        identity_df = pd.read_csv(identity_path)
        identity_df = identity_df.set_index("TransactionID")

    producer = create_avro_producer(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        schema_path=schema_path,
    )

    sleep_seconds = 0.0
    if args.rate and args.rate > 0:
        sleep_seconds = 1.0 / args.rate

    published = 0
    absolute_row_index = 0

    for chunk in pd.read_csv(transaction_path, chunksize=args.chunk_size):
        chunk_start = absolute_row_index
        chunk_end = absolute_row_index + len(chunk)
        absolute_row_index = chunk_end

        if chunk_end <= args.start_row:
            continue

        if chunk_start < args.start_row:
            rows_to_skip_inside_chunk = args.start_row - chunk_start
            chunk = chunk.iloc[rows_to_skip_inside_chunk:]

        if args.limit is not None:
            remaining = args.limit - published
            if remaining <= 0:
                break

            chunk = chunk.iloc[:remaining]

        if identity_df is not None:
            chunk = chunk.join(identity_df, on="TransactionID", how="left", rsuffix="_identity")

        for _, row in chunk.iterrows():
            event = build_transaction_event(row, args.split)

            producer.produce(
                topic=topic,
                key=event["transaction_id"],
                value=event,
                on_delivery=delivery_report,
            )

            producer.poll(0)
            published += 1

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

            if args.limit is not None and published >= args.limit:
                break

        producer.flush()

        if args.limit is not None and published >= args.limit:
            break

    print(f"Finished publishing {published} transaction events.")

if __name__ == "__main__":
    main()



    