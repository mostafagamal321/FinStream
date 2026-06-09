
import os
import sys
import argparse
import logging
import boto3
import pandas as pd
import clickhouse_connect
from io import BytesIO
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("silver_to_clickhouse_market_ticks")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",        type=str, default=None)
    parser.add_argument("--full-reload", action="store_true")
    return parser.parse_args()


def get_config():
    return {
        "silver_bucket": os.environ.get("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev"),
        "ch_host"      : os.environ.get("CLICKHOUSE_HOST",          "finstream-clickhouse"),
        "ch_user"      : os.environ.get("CLICKHOUSE_USER",          "finstream"),
        "ch_password"  : os.environ.get("CLICKHOUSE_PASSWORD",      "finstream123"),
        "ch_db"        : os.environ.get("CLICKHOUSE_DB",            "finstream"),
        "ch_table"     : "silver_market_ticks",
    }


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id     = os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name           = os.environ.get("AWS_REGION", "us-east-1"),
    )


def list_partitions(s3, bucket, prefix):
    """List event_day= partitions under a prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
    partitions = sorted([
        p["Prefix"].split("event_day=")[1].rstrip("/")
        for page in pages
        for p in page.get("CommonPrefixes", [])
        if "event_day=" in p["Prefix"]
    ])
    return partitions


def list_parquet_files(s3, bucket, prefix):
    """List all parquet files under a prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=bucket, Prefix=prefix)
    return [
        o["Key"] for page in pages
        for o in page.get("Contents", [])
        if o["Key"].endswith(".parquet")
    ]


def fix_dtypes(df, event_day_str):
   
    df["event_day"] = pd.to_datetime(event_day_str).date()

    
    for col in ["event_id", "event_time", "symbol", "source", "anomaly_flag",
                "raw_payload", "price_band", "silver_version"]:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    # Timestamp
    if "silver_processed_at" in df.columns:
        df["silver_processed_at"] = pd.to_datetime(df["silver_processed_at"])

    # Int columns
    for col in ["is_anomaly", "is_high_volume"]:
        if col in df.columns:
            df[col] = df[col].astype("Int32")

    # Float columns
    for col in ["price", "open_price", "high_price", "low_price", "close_price",
                "volume", "price_change_pct", "source_lag_seconds"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df
    """Fix dtypes and inject event_day from partition name."""
    
    df["event_day"] = pd.to_datetime(event_day_str).date()

    # Timestamp
    if "silver_processed_at" in df.columns:
        df["silver_processed_at"] = pd.to_datetime(df["silver_processed_at"])

    # Int columns
    for col in ["is_anomaly", "is_high_volume"]:
        if col in df.columns:
            df[col] = df[col].astype("Int32")

    # Float columns
    for col in ["price", "open_price", "high_price", "low_price", "close_price",
                "volume", "price_change_pct", "source_lag_seconds"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def load_partition(s3, bucket, prefix, event_day_str, client, ch_table, batch_size=5000):
    """Load one partition to ClickHouse."""
    files = list_parquet_files(s3, bucket, prefix)
    total = 0
    for f in files:
        obj = s3.get_object(Bucket=bucket, Key=f)
        df  = pd.read_parquet(BytesIO(obj["Body"].read()))
        df  = fix_dtypes(df, event_day_str)

        # Keep only ClickHouse columns
        ch_cols = [
            "event_id", "event_time", "event_time_ms", "ingestion_time_ms",
            "source_lag_ms", "symbol", "price", "open_price", "high_price",
            "low_price", "close_price", "volume", "source", "anomaly_flag",
            "raw_payload", "price_band", "price_change_pct",
            "is_anomaly", "is_high_volume", "source_lag_seconds",
            "event_day", "silver_processed_at", "silver_version",
        ]
        df = df[[c for c in ch_cols if c in df.columns]]

        for i in range(0, len(df), batch_size):
            client.insert_df(ch_table, df.iloc[i:i+batch_size])
            total += len(df.iloc[i:i+batch_size])

    return total


def main():
    args = parse_args()
    cfg  = get_config()
    s3   = get_s3_client()

    bucket            = cfg["silver_bucket"]
    historical_prefix = "silver/market_ticks/historical/"
    daily_prefix      = "silver/market_ticks/daily/"

    client = clickhouse_connect.get_client(
        host=cfg["ch_host"], port=8123,
        username=cfg["ch_user"], password=cfg["ch_password"],
        database=cfg["ch_db"],
    )

    if args.full_reload:
        log.info("Mode: FULL RELOAD")
        client.command(f"TRUNCATE TABLE {cfg['ch_table']}")
        log.info("Table truncated ✅")

        # Historical partitions
        hist_parts = list_partitions(s3, bucket, historical_prefix)
        log.info("Found %s historical partitions", len(hist_parts))

        total = 0
        for day in hist_parts:
            prefix = f"{historical_prefix}event_day={day}/"
            n = load_partition(s3, bucket, prefix, day, client, cfg["ch_table"])
            total += n
            if total % 50000 == 0 or day == hist_parts[-1]:
                log.info("Progress: %s rows loaded — current: %s", f"{total:,}", day)

        # Daily partitions
        daily_parts = list_partitions(s3, bucket, daily_prefix)
        for day in daily_parts:
            prefix = f"{daily_prefix}event_day={day}/"
            n = load_partition(s3, bucket, prefix, day, client, cfg["ch_table"])
            total += n
            log.info("Daily partition %s loaded — %s rows", day, n)

        log.info("Full reload complete — total: %s rows ", f"{total:,}")

    elif args.date:
        log.info("Mode: SINGLE DATE — %s", args.date)
        prefix = f"{daily_prefix}event_day={args.date}/"
        n = load_partition(s3, bucket, prefix, args.date, client, cfg["ch_table"])
        log.info("Done — %s rows", n)

    else:
        log.info("Mode: INCREMENTAL")
        result    = client.query(f"SELECT max(event_day) FROM {cfg['ch_table']}")
        last_date = str(result.first_row[0]) if result.first_row[0] else None
        log.info("Last loaded date: %s", last_date)

        daily_parts = list_partitions(s3, bucket, daily_prefix)
        new_parts   = [p for p in daily_parts if last_date is None or p > last_date]

        if not new_parts:
            log.info("No new partitions — ClickHouse is up to date")
            client.close()
            return

        total = 0
        for day in new_parts:
            prefix = f"{daily_prefix}event_day={day}/"
            n = load_partition(s3, bucket, prefix, day, client, cfg["ch_table"])
            total += n
            log.info("Loaded %s — %s rows", day, n)

        log.info("Incremental complete — %s rows", total)

    client.close()


if __name__ == "__main__":
    main()
