from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, max, min, avg, sum as _sum

# =========================
# 1. Spark Session
# =========================
spark = SparkSession.builder \
    .appName("StockBatchSilverJob") \
    .getOrCreate()

# =========================
# 2. Read RAW STOCK DATA (CSV)
# =========================
df = spark.read.csv(
    "walmart_stock.csv",
    header=True,
    inferSchema=True
)

# =========================
# 3. Data Cleaning
# =========================
df_clean = df \
    .filter(col("Volume") > 0) \
    .dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume", "Adj Close"]) \
    .withColumn("Date", to_date(col("Date")))

# =========================
# 4. STATE (incremental processing)
# =========================
try:
    state_df = spark.read.parquet("state/stock/")
    last_date = state_df.agg(max("last_date")).collect()[0][0]
except:
    last_date = None

# =========================
# 5. Incremental Filtering
# =========================
if last_date:
    df_clean = df_clean.filter(col("Date") > last_date)

# =========================
# 6. Feature Engineering
# =========================
df_features = df_clean \
    .withColumn("daily_range", col("High") - col("Low")) \
    .withColumn("price_change", col("Close") - col("Open")) \
    .withColumn("pct_change", ((col("Close") - col("Open")) / col("Open")) * 100)

# =========================
# 7. Silver Aggregation
# =========================
silver_df = df_features.groupBy("Date").agg(
    avg("Close").alias("avg_close"),
    avg("Adj Close").alias("avg_adj_close"),
    max("High").alias("max_high"),
    min("Low").alias("min_low"),
    _sum("Volume").alias("total_volume"),
    avg("pct_change").alias("avg_pct_change")
)

# =========================
# 8. Write Silver Output
# =========================
silver_df.write \
    .mode("append") \
    .parquet("output/silver/stock/")

# =========================
# 9. Update STATE
# =========================
new_last_date = df_clean.agg(max("Date")).collect()[0][0]

if new_last_date:
    state_update = spark.createDataFrame(
        [(new_last_date,)],
        ["last_date"]
    )

    state_update.write \
        .mode("overwrite") \
        .parquet("state/stock/")

# =========================
# 10. Stop Spark
# =========================
spark.stop() 