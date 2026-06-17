import os
import sys
import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("silver_to_clickhouse_transactions")


def parse_args():
    parser = argparse.ArgumentParser(description="Silver Transactions → ClickHouse (Incremental)")
    parser.add_argument("--date",        type=str, default=None,  help="Force single date YYYY-MM-DD")
    parser.add_argument("--full-reload", action="store_true",     help="Reload everything from S3 Silver")
    return parser.parse_args()


def get_config():
    return {
        "silver_bucket" : os.environ.get("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev"),
        "ch_host"       : os.environ.get("CLICKHOUSE_HOST",         "localhost"),
        "ch_user"       : os.environ.get("CLICKHOUSE_USER",         "finstream"),
        "ch_password"   : os.environ.get("CLICKHOUSE_PASSWORD",     "finstream123"),
        "ch_db"         : os.environ.get("CLICKHOUSE_DB",           "finstream"),
        "ch_table"      : "silver_transactions",
    }


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("finstream-silver-to-clickhouse-incremental")
        .config("spark.jars.packages",                   "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",        os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key",        os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint",          "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.shuffle.partitions",          "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created")
    return spark


def get_last_loaded_date(cfg):
    try:
        client = clickhouse_connect.get_client(
            host=cfg["ch_host"], port=8123,
            username=cfg["ch_user"], password=cfg["ch_password"],
            database=cfg["ch_db"],
        )
        result    = client.query(f"SELECT max(event_day) FROM {cfg['ch_table']}")
        last_date = result.first_row[0]
        client.close()

        if last_date is None:
            log.info("ClickHouse table is empty → will do full load")
            return None

        log.info("Last loaded date in ClickHouse: %s", last_date)
        return str(last_date)

    except Exception as e:
        log.warning("Could not query ClickHouse: %s — will do full load", e)
        return None


def get_s3_partitions(spark, silver_root):
    try:
        jvm      = spark.sparkContext._jvm
        fs       = jvm.org.apache.hadoop.fs.FileSystem.get(
                       jvm.java.net.URI.create(silver_root),
                       jvm.org.apache.hadoop.conf.Configuration()
                   )
        statuses = fs.listStatus(jvm.org.apache.hadoop.fs.Path(silver_root))

        partitions = sorted([
            s.getPath().getName().replace("event_day=", "")
            for s in statuses
            if s.getPath().getName().startswith("event_day=")
        ])

        log.info("Found %s partitions in S3 Silver", len(partitions))
        return partitions

    except Exception as e:
        log.error("Failed to list S3 partitions: %s", e)
        sys.exit(1)


def get_partitions_to_load(all_partitions, last_loaded_date, full_reload):
    if full_reload or last_loaded_date is None:
        log.info("Loading ALL %s partitions", len(all_partitions))
        return all_partitions

    new_partitions = [p for p in all_partitions if p > last_loaded_date]

    if not new_partitions:
        log.info("No new partitions — ClickHouse is up to date")
        return []

    log.info("New partitions to load: %s", new_partitions)
    return new_partitions


def read_silver_partitions(spark, silver_root, partitions):
    paths = [f"{silver_root}event_day={p}/" for p in partitions]
    log.info("Reading %s partition(s)...", len(paths))
    df    = spark.read.option("mergeSchema", "true").parquet(*paths)
    count = df.count()
    log.info("Rows to load: %s", f"{count:,}")
    return df, count


def prepare_for_clickhouse(df):
    if "event_day" not in df.columns:
        df = df.withColumn("event_day", F.to_date("event_ts"))
    return df.select(
        "transaction_id", "customer_id", "card_id", "merchant_id",
        "amount", "currency", "amount_band", "is_high_amount",
        "card_brand", "card_type", "addr1", "addr2",
        "device_type", "device_info", "has_device_info",
        "payer_email_domain", "receiver_email_domain", "email_domain_type",
        "product_cd", "is_fraud",
        "event_ts", "ingestion_ts", "s3_ingestion_ts",
        "event_day", "event_hour", "processing_lag_seconds",
        "silver_processed_at", "silver_version",
    )


def write_to_clickhouse(df, cfg, total_count):
    ch_url = f"jdbc:clickhouse://{cfg['ch_host']}:8123/{cfg['ch_db']}"

    
    num_partitions = max(1, total_count // 10000)
    log.info("Repartitioning into %s chunks (~10k rows each)", num_partitions)
    df = df.repartition(num_partitions)

    log.info("Writing to ClickHouse %s.%s ...", cfg["ch_db"], cfg["ch_table"])
    (
        df.write
        .format("jdbc")
        .option("url",       ch_url)
        .option("dbtable",   cfg["ch_table"])
        .option("user",      cfg["ch_user"])
        .option("password",  cfg["ch_password"])
        .option("driver",    "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", "5000")
        .mode("append")
        .save()
    )
    log.info("ClickHouse write complete")


def main():
    args  = parse_args()
    cfg   = get_config()
    spark = create_spark_session()

    silver_root = f"s3a://{cfg['silver_bucket']}/silver/transactions/"

    if args.date:
        log.info("Manual override — date: %s", args.date)
        partitions_to_load = [args.date]
    else:
        all_partitions     = get_s3_partitions(spark, silver_root)
        last_loaded_date   = None if args.full_reload else get_last_loaded_date(cfg)
        partitions_to_load = get_partitions_to_load(all_partitions, last_loaded_date, args.full_reload)

    if not partitions_to_load:
        log.info("Nothing to load — exiting.")
        spark.stop()
        return

    silver_df, count = read_silver_partitions(spark, silver_root, partitions_to_load)
    ch_df            = prepare_for_clickhouse(silver_df)

    
    if args.full_reload or (not args.date and get_last_loaded_date(cfg) is None):
        log.info("Truncating ClickHouse table before full reload...")
        client = clickhouse_connect.get_client(
            host=cfg["ch_host"], port=8123,
            username=cfg["ch_user"], password=cfg["ch_password"],
            database=cfg["ch_db"],
        )
        client.command(f"TRUNCATE TABLE {cfg['ch_table']}")
        client.close()
        log.info("Table truncated")

    write_to_clickhouse(ch_df, cfg, count)

    log.info("Job complete — %s rows | partitions: %s", f"{count:,}", partitions_to_load)
    spark.stop()


if __name__ == "__main__":
    main()
