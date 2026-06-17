"""
FinStream — Bronze to Silver Market News
==========================================
Owner   : Saleh (E3)
Source  : s3a://<bronze-bucket>/bronze/market_news/
Target  : s3a://<silver-bucket>/silver/market_news/

Run:
    python spark/jobs/bronze_to_silver_market_news.py --date 2026-06-08
"""

import os
import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bronze_to_silver_market_news")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None, help="Single date YYYY-MM-DD")
    return parser.parse_args()


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("finstream-bronze-to-silver-market-news")
        .config("spark.jars.packages",                      "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.impl",                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",           os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key",           os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint",             "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access",    "false")
        .config("spark.sql.shuffle.partitions",             "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.memory",                      "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created")
    return spark


def transform(df, partition_date):
    window = Window.partitionBy("event_id").orderBy(F.col("ingestion_time_ms").desc_nulls_last())

    return (
        df
        .drop("dt")
        # dedup
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        # clean nulls on core columns
        .filter(F.col("event_id").isNotNull())
        # cast types
        .withColumn("event_time",       F.col("event_time").cast("string"))
        .withColumn("event_time_ms",    F.col("event_time_ms").cast("long"))
        .withColumn("ingestion_time_ms",F.col("ingestion_time_ms").cast("long"))
        .withColumn("source_lag_ms",    F.col("source_lag_ms").cast("long"))
        .withColumn("content_length",   F.col("content_length").cast("int"))
        .withColumn("negation_flag",    F.col("negation_flag").cast("boolean"))
        # partition date
        .withColumn("event_day",        F.to_date(F.lit(partition_date)))
        # silver metadata
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("silver_version",      F.lit("1.0"))
        # select final columns
        .select(
            "event_id", "event_time", "event_time_ms",
            "published_at", "source_name", "author",
            "title", "description", "url",
            "symbol_query", "source",
            "content_text", "sentiment_label",
            "risk_category", "severity",
            "negation_flag", "matched_keywords", "content_length",
            "ingestion_time_ms", "source_lag_ms",
            "raw_payload",
            "event_day", "silver_processed_at", "silver_version",
        )
    )


def main():
    args  = parse_args()
    spark = create_spark_session()

    bronze_bucket = os.environ.get("FINSTREAM_BRONZE_BUCKET", "finstream-bronze-mostafa-dev")
    silver_bucket = os.environ.get("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev")
    bronze_root   = f"s3a://{bronze_bucket}/bronze/market_news/"
    silver_target = f"s3a://{silver_bucket}/silver/market_news/"

    if args.date:
        log.info("Mode: INCREMENTAL — %s", args.date)
        bronze_path = f"{bronze_root}dt={args.date}/"
        df     = spark.read.option("mergeSchema", "true").parquet(bronze_path)
        silver = transform(df, args.date)
        (
            silver.write
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("event_day")
            .parquet(silver_target)
        )
        log.info("Done— date: %s", args.date)
    else:
        log.info("Mode: FULL LOAD")
        df     = spark.read.option("mergeSchema", "true").parquet(bronze_root)
        df     = df.withColumn("_partition_date", F.col("dt").cast("string"))
        silver = (
            df
            .drop("dt")
            .withColumn("_rn", F.row_number().over(
                Window.partitionBy("event_id").orderBy(F.col("ingestion_time_ms").desc_nulls_last())
            ))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .filter(F.col("event_id").isNotNull())
            .withColumn("event_time",        F.col("event_time").cast("string"))
            .withColumn("event_time_ms",     F.col("event_time_ms").cast("long"))
            .withColumn("ingestion_time_ms", F.col("ingestion_time_ms").cast("long"))
            .withColumn("source_lag_ms",     F.col("source_lag_ms").cast("long"))
            .withColumn("content_length",    F.col("content_length").cast("int"))
            .withColumn("negation_flag",     F.col("negation_flag").cast("boolean"))
            .withColumn("event_day",         F.to_date(F.col("_partition_date")))
            .withColumn("silver_processed_at", F.current_timestamp())
            .withColumn("silver_version",      F.lit("1.0"))
            .drop("_partition_date")
            .select(
                "event_id", "event_time", "event_time_ms",
                "published_at", "source_name", "author",
                "title", "description", "url",
                "symbol_query", "source",
                "content_text", "sentiment_label",
                "risk_category", "severity",
                "negation_flag", "matched_keywords", "content_length",
                "ingestion_time_ms", "source_lag_ms",
                "raw_payload",
                "event_day", "silver_processed_at", "silver_version",
            )
        )
        (
            silver.write
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("event_day")
            .parquet(silver_target)
        )
        log.info("Full load complete")

    spark.stop()


if __name__ == "__main__":
    main()
