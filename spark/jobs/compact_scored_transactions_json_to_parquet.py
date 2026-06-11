import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def get_env(name: str, default: str) -> str:
    return os.getenv(name, default)


def log_step(message: str) -> None:
    print(f"\n[{datetime.now(timezone.utc).isoformat()}] {message}", flush=True)


def log_elapsed(step_name: str, start_time: float) -> None:
    elapsed = time.time() - start_time
    print(f"[TIMER] {step_name}: {elapsed:.2f} seconds", flush=True)


def main() -> None:
    bronze_bucket = get_env("FINSTREAM_BRONZE_BUCKET", "finstream-bronze-mostafa-dev")
    silver_bucket = get_env("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev")

    input_path = get_env(
        "S3_SCORED_TRANSACTIONS_JSON_INPUT",
        f"s3a://{bronze_bucket}/bronze/transactions_scored_flink/*/*/part-*",
    )

    output_path = get_env(
        "S3_SCORED_TRANSACTIONS_PARQUET_OUTPUT",
        f"s3a://{silver_bucket}/silver/transactions_scored_parquet/",
    )

    output_partitions = int(get_env("SCORED_PARQUET_OUTPUT_PARTITIONS", "8"))
    write_mode = get_env("SCORED_PARQUET_WRITE_MODE", "overwrite")

    spark = (
        SparkSession.builder
        .appName("FinStream Compact Scored Transactions JSON To Parquet")
        .config("spark.sql.shuffle.partitions", get_env("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .config("spark.default.parallelism", get_env("SPARK_DEFAULT_PARALLELISM", "8"))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    total_start = time.time()

    log_step(f"Reading JSON scored transactions from: {input_path}")
    step_start = time.time()

    df = spark.read.json(input_path)

    log_elapsed("read_json_with_schema_inference", step_start)

    required_columns = [
        "transaction_id",
        "customer_id",
        "card_id",
        "merchant_id",
        "event_time",
        "scored_at",
        "amount",
        "currency",
        "product_cd",
        "rule_score",
        "ml_score",
        "fraud_score",
        "scoring_method",
        "risk_level",
        "decision",
        "reason_codes",
        "actual_is_fraud",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    ]

    existing_columns = [c for c in required_columns if c in df.columns]
    missing_columns = [c for c in required_columns if c not in df.columns]

    if missing_columns:
        print(f"Warning: missing columns: {missing_columns}", flush=True)

    log_step("Selecting and normalizing columns")
    step_start = time.time()

    compact_df = (
        df
        .select(*existing_columns)
        .filter(col("transaction_id").isNotNull())
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("actual_is_fraud", col("actual_is_fraud").cast("int"))
        .withColumn("kafka_partition", col("kafka_partition").cast("int"))
        .withColumn("kafka_offset", col("kafka_offset").cast("long"))
    )

    for score_col in ["rule_score", "ml_score", "fraud_score"]:
        if score_col in compact_df.columns:
            compact_df = compact_df.withColumn(score_col, col(score_col).cast("double"))

    row_count = compact_df.count()

    print(f"Rows to write: {row_count}", flush=True)

    if row_count == 0:
        raise RuntimeError("No rows found to compact.")

    log_elapsed("select_normalize_count", step_start)

    log_step(f"Writing Parquet to: {output_path}")
    print(f"Write mode: {write_mode}", flush=True)
    print(f"Output partitions: {output_partitions}", flush=True)

    step_start = time.time()

    (
        compact_df
        .repartition(output_partitions)
        .write
        .mode(write_mode)
        .option("compression", "snappy")
        .parquet(output_path)
    )

    log_elapsed("write_parquet", step_start)
    log_elapsed("total_compaction_job", total_start)

    print("Compaction finished successfully.", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
