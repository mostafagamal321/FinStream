
import os
import sys
import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import clickhouse_connect

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
        "ch_host"      : os.environ.get("CLICKHOUSE_HOST",          "finstream-clickhouse"),  # FIX
        "ch_user"      : os.environ.get("CLICKHOUSE_USER",          "finstream"),
        "ch_password"  : os.environ.get("CLICKHOUSE_PASSWORD",      "finstream123"),
        "ch_db"        : os.environ.get("CLICKHOUSE_DB",            "finstream"),
        "ch_table"     : "silver_market_ticks",
    }


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("finstream-silver-to-clickhouse-market-ticks")
        .config("spark.jars.packages",                   "org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.6.0")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",        os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key",        os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint",          "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.shuffle.partitions",          "8")
        .config("spark.driver.memory",                   "3g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created")
    return spark


def get_last_loaded_date(cfg):
    try:
        client    = clickhouse_connect.get_client(
            host=cfg["ch_host"], port=8123,
            username=cfg["ch_user"], password=cfg["ch_password"],
            database=cfg["ch_db"],
        )
        result    = client.query(f"SELECT max(event_day) FROM {cfg['ch_table']}")
        last_date = result.first_row[0]
        client.close()
        if last_date is None:
            log.info("ClickHouse empty → full load")
            return None
        log.info("Last loaded date: %s", last_date)
        return str(last_date)
    except Exception as e:
        log.warning("Could not query ClickHouse: %s — full load", e)
        return None


def get_daily_partitions(spark, daily_root):
    """List event_day= partitions in daily folder."""
    try:
        jvm      = spark.sparkContext._jvm
        fs       = jvm.org.apache.hadoop.fs.FileSystem.get(
                       jvm.java.net.URI.create(daily_root),
                       jvm.org.apache.hadoop.conf.Configuration())
        statuses = fs.listStatus(jvm.org.apache.hadoop.fs.Path(daily_root))
        partitions = sorted([
            s.getPath().getName().replace("event_day=", "")
            for s in statuses
            if s.getPath().getName().startswith("event_day=")
        ])
        log.info("Found %s daily partitions", len(partitions))
        return partitions
    except Exception:
        log.info("No daily partitions found")
        return []


def truncate_table(cfg):
    client = clickhouse_connect.get_client(
        host=cfg["ch_host"], port=8123,
        username=cfg["ch_user"], password=cfg["ch_password"],
        database=cfg["ch_db"],
    )
    client.command(f"TRUNCATE TABLE {cfg['ch_table']}")
    client.close()
    log.info("Table truncated")


def write_to_clickhouse(df, cfg, total_count):
    ch_url         = f"jdbc:clickhouse://{cfg['ch_host']}:8123/{cfg['ch_db']}"
    num_partitions = max(1, total_count // 10000)
    log.info("Repartitioning into %s chunks", num_partitions)
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
    args = parse_args()
    cfg  = get_config()
    spark = create_spark_session()

    silver_bucket     = cfg["silver_bucket"]
    silver_historical = f"s3a://{silver_bucket}/silver/market_ticks/historical/"
    silver_daily      = f"s3a://{silver_bucket}/silver/market_ticks/daily/"

    if args.full_reload:
        # ── Full reload: historical + all daily ──────────────────────────
        log.info("Mode: FULL RELOAD")
        truncate_table(cfg)

       
        log.info("Reading historical: %s", silver_historical)
        df_hist = spark.read.option("mergeSchema", "true").parquet(silver_historical)

       
        daily_parts = get_daily_partitions(spark, silver_daily)
        if daily_parts:
            log.info("Reading daily partitions: %s", daily_parts)
            df_daily = spark.read.option("mergeSchema", "true").parquet(silver_daily)
            df       = df_hist.unionByName(df_daily, allowMissingColumns=True)
        else:
            df = df_hist

        count = df.count()
        log.info("Total rows to load: %s", f"{count:,}")
        write_to_clickhouse(df, cfg, count)

    elif args.date:
        # ── Single date from daily ───────────────────────────────────────
        log.info("Mode: SINGLE DATE — %s", args.date)
        path  = f"{silver_daily}event_day={args.date}/"
        df    = spark.read.option("mergeSchema", "true").parquet(path)
        count = df.count()
        log.info("Rows to load: %s", f"{count:,}")
        write_to_clickhouse(df, cfg, count)

    else:
        # ── Incremental: new daily partitions only ───────────────────────
        log.info("Mode: INCREMENTAL")
        last_date  = get_last_loaded_date(cfg)
        daily_parts = get_daily_partitions(spark, silver_daily)
        new_parts   = [p for p in daily_parts if last_date is None or p > last_date]

        if not new_parts:
            log.info("No new partitions — ClickHouse is up to date")
            spark.stop()
            return

        log.info("New partitions: %s", new_parts)
        paths = [f"{silver_daily}event_day={p}/" for p in new_parts]
        df    = spark.read.option("mergeSchema", "true").parquet(*paths)
        count = df.count()
        log.info("Rows to load: %s", f"{count:,}")
        write_to_clickhouse(df, cfg, count)

    log.info("Job complete")
    spark.stop()


if __name__ == "__main__":
    main()
