"""
FinStream — Silver Transactions → ClickHouse
=============================================
Owner   : Saleh (E3)
Source  : s3a://<silver-bucket>/silver/transactions/
Target  : ClickHouse — finstream.silver_transactions

Run:
    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0 \
        spark/jobs/silver_to_clickhouse_transactions.py

    # Single partition:
    spark-submit ... silver_to_clickhouse_transactions.py --date 2020-01-02
"""

import os
import sys
import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("silver_to_clickhouse_transactions")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Args
# ─────────────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Silver Transactions → ClickHouse")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Load single event_day partition only (YYYY-MM-DD). Omit for full load."
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# 2. Spark Session
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session():
    aws_key    = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]

    spark = (
        SparkSession.builder
        .appName("finstream-silver-to-clickhouse-transactions")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",        aws_key)
        .config("spark.hadoop.fs.s3a.secret.key",        aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint",          "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.shuffle.partitions",          "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created — version: %s", spark.version)
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# 3. Config
# ─────────────────────────────────────────────────────────────────────────────
def get_config(date_filter=None):
    silver_bucket = os.environ.get("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev")
    ch_host       = os.environ.get("CLICKHOUSE_HOST",     "localhost")
    ch_user       = os.environ.get("CLICKHOUSE_USER",     "finstream")
    ch_password   = os.environ.get("CLICKHOUSE_PASSWORD", "finstream123")
    ch_db         = os.environ.get("CLICKHOUSE_DB",       "finstream")

    silver_root = f"s3a://{silver_bucket}/silver/transactions/"

    if date_filter:
        read_path = f"{silver_root}event_day={date_filter}/"
        log.info("Mode: SINGLE PARTITION — %s", date_filter)
    else:
        read_path = silver_root
        log.info("Mode: FULL LOAD — all partitions")

    log.info("Silver source : %s", read_path)
    log.info("ClickHouse    : %s@%s/%s", ch_user, ch_host, ch_db)

    return read_path, ch_host, ch_user, ch_password, ch_db


# ─────────────────────────────────────────────────────────────────────────────
# 4. Read Silver
# ─────────────────────────────────────────────────────────────────────────────
def read_silver(spark, read_path):
    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(read_path)
    )
    count = df.count()
    log.info("Silver read complete — rows: %s", f"{count:,}")
    return df, count


# ─────────────────────────────────────────────────────────────────────────────
# 5. Prepare for ClickHouse
# ─────────────────────────────────────────────────────────────────────────────
def prepare_for_clickhouse(df):
    # Select only columns that exist in ClickHouse silver_transactions table
    return df.select(
        "transaction_id",
        "customer_id",
        "card_id",
        "merchant_id",
        "amount",
        "currency",
        "amount_band",
        "is_high_amount",
        "card_brand",
        "card_type",
        "addr1",
        "addr2",
        "device_type",
        "device_info",
        "has_device_info",
        "payer_email_domain",
        "receiver_email_domain",
        "email_domain_type",
        "product_cd",
        "is_fraud",
        "event_ts",
        "ingestion_ts",
        "s3_ingestion_ts",
        "event_day",
        "event_hour",
        "processing_lag_seconds",
        "silver_processed_at",
        "silver_version",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 6. Write to ClickHouse
# ─────────────────────────────────────────────────────────────────────────────
def write_to_clickhouse(df, ch_host, ch_user, ch_password, ch_db):
    ch_url = f"jdbc:clickhouse://{ch_host}:8123/{ch_db}"

    log.info("Writing to ClickHouse — %s.silver_transactions ...", ch_db)

    (
        df.write
        .format("jdbc")
        .option("url",      ch_url)
        .option("dbtable",  "silver_transactions")
        .option("user",     ch_user)
        .option("password", ch_password)
        .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", "50000")
        .mode("append")
        .save()
    )

    log.info("ClickHouse write complete")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    args  = parse_args()
    spark = create_spark_session()

    read_path, ch_host, ch_user, ch_password, ch_db = get_config(args.date)

    silver_df, count = read_silver(spark, read_path)
    ch_df            = prepare_for_clickhouse(silver_df)

    write_to_clickhouse(ch_df, ch_host, ch_user, ch_password, ch_db)

    log.info("Job complete — %s rows loaded to ClickHouse", f"{count:,}")
    spark.stop()


if __name__ == "__main__":
    main()
