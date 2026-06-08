
import os
import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bronze_to_silver_market_ticks")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None, help="Single date YYYY-MM-DD (incremental)")
    return parser.parse_args()


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("finstream-bronze-to-silver-market-ticks")
        .config("spark.jars.packages",                      "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.impl",                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",           os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key",           os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint",             "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access",    "false")
        .config("spark.sql.shuffle.partitions",             "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.memory",                      "3g")
        .config("spark.hadoop.fs.s3a.connection.maximum",   "100")
        .config("spark.hadoop.fs.s3a.threads.max",          "20")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created — version: %s", spark.version)
    return spark


def transform(df):
    """Apply all transformations on the dataframe."""
    window = Window.partitionBy("event_id").orderBy(F.col("ingestion_time_ms").desc_nulls_last())

    return (
        df
        .drop("dt")
        .withColumn("price",       F.col("price").cast("double"))
        .withColumn("open_price",  F.col("open_price").cast("double"))
        .withColumn("high_price",  F.col("high_price").cast("double"))
        .withColumn("low_price",   F.col("low_price").cast("double"))
        .withColumn("close_price", F.col("close_price").cast("double"))
        .withColumn("volume",      F.col("volume").cast("double"))
        .filter(
            F.col("event_id").isNotNull() &
            F.col("symbol").isNotNull() &
            F.col("price").isNotNull() &
            (F.col("price") > 0)
        )
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("price_band",
            F.when(F.col("price") <  50,  "LOW")
             .when(F.col("price") < 200,  "MEDIUM")
             .when(F.col("price") < 500,  "HIGH")
             .otherwise("VERY_HIGH"))
        .withColumn("price_change_pct",
            F.when(F.col("open_price").isNotNull() & (F.col("open_price") > 0),
                F.round((F.col("close_price") - F.col("open_price")) / F.col("open_price") * 100, 4)
            ).otherwise(None))
        .withColumn("is_anomaly",
            F.when(F.col("anomaly_flag") != "NORMAL", 1).otherwise(0).cast("int"))
        .withColumn("is_high_volume",
            F.when(F.col("volume") >= 10000, 1).otherwise(0).cast("int"))
        .withColumn("source_lag_seconds",
            F.when(F.col("source_lag_ms").isNotNull(),
                F.round(F.col("source_lag_ms") / 1000, 2)).otherwise(None))
        .withColumn("event_day",
            F.to_date(F.from_unixtime(F.col("event_time_ms") / 1000)))
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("silver_version",      F.lit("1.0"))
    )


def main():
    args  = parse_args()
    spark = create_spark_session()

    bronze_bucket    = os.environ.get("FINSTREAM_BRONZE_BUCKET", "finstream-bronze-mostafa-dev")
    silver_bucket    = os.environ.get("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev")
    bronze_root      = f"s3a://{bronze_bucket}/bronze/market_ticks/"
    silver_historical = f"s3a://{silver_bucket}/silver/market_ticks/historical/"
    silver_daily      = f"s3a://{silver_bucket}/silver/market_ticks/daily/"

    if args.date:
        #Incremental
        log.info("Mode: INCREMENTAL — %s", args.date)
        bronze_path = f"{bronze_root}dt={args.date}/"
        log.info("Reading: %s", bronze_path)

        df     = spark.read.option("mergeSchema", "true").parquet(bronze_path)
        silver = transform(df)

        log.info("Writing to daily: %s", silver_daily)
        (
            silver.write
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("event_day")
            .parquet(silver_daily)
        )
        log.info("Done ✅ — date: %s", args.date)

    else:
        # Full load
        log.info("Mode: FULL LOAD — reading ALL Bronze partitions at once")
        log.info("Reading: %s", bronze_root)

        df     = spark.read.option("mergeSchema", "true").parquet(bronze_root)
        log.info("All Bronze data loaded into Spark")

        silver = transform(df)

        log.info("Writing to historical: %s (coalesce 10)", silver_historical)
        (
            silver
            .coalesce(10)   
            .write
            .mode("overwrite")
            .parquet(silver_historical)
        )
        log.info("Silver historical written to S3")
        log.info("Full load complete")

    spark.stop()


if __name__ == "__main__":
    main()
