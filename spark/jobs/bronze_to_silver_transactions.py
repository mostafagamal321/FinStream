"""
FinStream — Bronze to Silver Transactions
==========================================
Owner   : Saleh (E3)
Source  : s3a://<bronze-bucket>/bronze/transactions_raw/
Target  : s3a://<silver-bucket>/silver/transactions/

Run:
    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        spark/jobs/bronze_to_silver_transactions.py

    # Single partition (dev/test):
    spark-submit ... bronze_to_silver_transactions.py --date 2020-01-02
"""

import os
import sys
import datetime
import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("bronze_to_silver_transactions")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Args
# ─────────────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Bronze → Silver Transactions")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Process single event_date partition only (YYYY-MM-DD). "
             "Omit to process all partitions (full run)."
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
        .appName("finstream-bronze-to-silver-transactions")
        # 
        .config("spark.driver.memory",                 "4g")
        .config("spark.driver.maxResultSize",          "2g")
        .config("spark.memory.fraction",               "0.8")
        .config("spark.memory.storageFraction",        "0.3")
        # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
        .config("spark.jars.packages",                 "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.impl",            "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",      aws_key)
        .config("spark.hadoop.fs.s3a.secret.key",      aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint",        "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.shuffle.partitions",        "16")  # زودتها من 8 لـ 16 عشان الـ Shuffle
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created — version: %s", spark.version)
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# 3. Config
# ─────────────────────────────────────────────────────────────────────────────
def get_config(date_filter=None):
    bronze_bucket = os.environ.get("FINSTREAM_BRONZE_BUCKET", "finstream-bronze-mostafa-dev")
    silver_bucket = os.environ.get("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev")

    bronze_root   = f"s3a://{bronze_bucket}/bronze/transactions_raw/"
    silver_target = f"s3a://{silver_bucket}/silver/transactions/"

    if date_filter:
        read_path = f"{bronze_root}event_date={date_filter}/"
        log.info("Mode: SINGLE PARTITION — %s", date_filter)
    else:
        read_path = bronze_root
        log.info("Mode: FULL RUN — all partitions")

    log.info("Bronze source : %s", read_path)
    log.info("Silver target : %s", silver_target)

    return read_path, silver_target


# ─────────────────────────────────────────────────────────────────────────────
# 4. Read Bronze
# ─────────────────────────────────────────────────────────────────────────────
def read_bronze(spark, read_path):
    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .json(read_path)
    )
    raw_count = df.count()
    log.info("Bronze read complete — raw rows: %s", f"{raw_count:,}")
    return df, raw_count


# ─────────────────────────────────────────────────────────────────────────────
# 5. Clean
# ─────────────────────────────────────────────────────────────────────────────
def clean(df, raw_count):
    base_df = (
        df
        .withColumn("event_ts",        F.to_timestamp("event_time"))
        .withColumn("ingestion_ts",    F.to_timestamp("ingestion_time"))
        .withColumn("s3_ingestion_ts", F.to_timestamp("_s3_ingestion_time"))
        .withColumn("amount",          F.col("amount").cast("double"))
        .withColumn("is_fraud",        F.col("is_fraud").cast("int"))
        .filter(
            F.col("transaction_id").isNotNull() &
            F.col("amount").isNotNull() &
            (F.col("amount") >= 0)
        )
    )
    clean_count = base_df.count()
    log.info("After cleaning : %s (dropped %s)", f"{clean_count:,}", f"{raw_count - clean_count:,}")
    return base_df, clean_count


# ─────────────────────────────────────────────────────────────────────────────
# 6. Deduplicate
# ─────────────────────────────────────────────────────────────────────────────
def deduplicate(df, clean_count):
    window = (
        Window
        .partitionBy("transaction_id")
        .orderBy(
            F.col("s3_ingestion_ts").desc_nulls_last(),
            F.col("_kafka_offset").desc_nulls_last()
        )
    )
    dedup_df = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    dedup_count = dedup_df.count()
    log.info("After dedup    : %s (removed %s duplicates)", f"{dedup_count:,}", f"{clean_count - dedup_count:,}")
    return dedup_df, dedup_count


# ─────────────────────────────────────────────────────────────────────────────
# 7. Feature Engineering
# ─────────────────────────────────────────────────────────────────────────────
def add_silver_features(df):
    return (
        df
        # Amount
        .withColumn(
            "amount_band",
            F.when(F.col("amount") <   50, "LOW")
             .when(F.col("amount") <  250, "MEDIUM")
             .when(F.col("amount") < 1000, "HIGH")
             .otherwise("VERY_HIGH")
        )
        .withColumn("is_high_amount",     (F.col("amount") >= 1000).cast("int"))
        # Device
        .withColumn("has_device_info",    F.col("device_info").isNotNull().cast("int"))
        # Email
        .withColumn(
            "email_domain_type",
            F.when(F.col("payer_email_domain").isNull(), "MISSING").otherwise("PRESENT")
        )
        # Time
        .withColumn("event_day",          F.to_date("event_ts"))
        .withColumn("event_hour",         F.hour("event_ts"))
        .withColumn(
            "processing_lag_seconds",
            F.unix_timestamp("s3_ingestion_ts") - F.unix_timestamp("event_ts")
        )
        # Metadata
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("silver_version",      F.lit("1.0"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# 8. Quality Checks
# ─────────────────────────────────────────────────────────────────────────────
def run_quality_checks(df):
    checks = {
        "null_transaction_id" : df.filter(F.col("transaction_id").isNull()).count(),
        "null_amount"         : df.filter(F.col("amount").isNull()).count(),
        "negative_amount"     : df.filter(F.col("amount") < 0).count(),
        "null_event_ts"       : df.filter(F.col("event_ts").isNull()).count(),
        "null_customer_id"    : df.filter(F.col("customer_id").isNull()).count(),
        "null_merchant_id"    : df.filter(F.col("merchant_id").isNull()).count(),
        "null_is_fraud"       : df.filter(F.col("is_fraud").isNull()).count(),
        "invalid_is_fraud"    : df.filter(~F.col("is_fraud").isin(0, 1)).count(),
    }

    all_passed = True
    for name, result in checks.items():
        status = "PASS" if result == 0 else "FAIL"
        if result != 0:
            all_passed = False
        log.info("Quality [%s] %s = %s", status, name, result)

    if not all_passed:
        log.error("Quality checks FAILED — aborting write")
        sys.exit(1)

    log.info("All quality checks passed ")


# ─────────────────────────────────────────────────────────────────────────────
# 9. Write Silver
# ─────────────────────────────────────────────────────────────────────────────
def write_silver(df, silver_target):
    silver_final = df.drop(
        "_kafka_offset", "_kafka_partition", "_kafka_topic",
        "_sink_run_id", "run_id"
    )

    log.info("Writing Silver to S3...")

    (
        silver_final
        .coalesce(4) 
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_day")
        .parquet(silver_target)
    )

    log.info("Silver written to: %s ", silver_target)
    log.info("Timestamp: %s", datetime.datetime.utcnow().isoformat())


# ─────────────────────────────────────────────────────────────────────────────
# 10. Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    args        = parse_args()
    spark       = create_spark_session()
    read_path, silver_target = get_config(args.date)

    raw_df, raw_count        = read_bronze(spark, read_path)
    clean_df, clean_count    = clean(raw_df, raw_count)
    dedup_df, dedup_count    = deduplicate(clean_df, clean_count)
    silver_df                = add_silver_features(dedup_df)

    run_quality_checks(silver_df)
    write_silver(silver_df, silver_target)

    log.info("Job complete — raw: %s | clean: %s | dedup: %s",
             f"{raw_count:,}", f"{clean_count:,}", f"{dedup_count:,}")

    spark.stop()


if __name__ == "__main__":
    main()
