import json
import os
import tempfile
import time
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, lit, rand

from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.preprocessing import StandardScaler


def now_ts() -> float:
    return time.time()


def log_step(message: str) -> None:
    print(f"\n[{datetime.now(timezone.utc).isoformat()}] {message}", flush=True)


def log_elapsed(step_name: str, start_time: float) -> None:
    elapsed = time.time() - start_time
    print(f"[TIMER] {step_name}: {elapsed:.2f} seconds", flush=True)


def get_env(name: str, default: str) -> str:
    return os.getenv(name, default)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")

    path = uri.replace("s3://", "", 1)
    bucket, key = path.split("/", 1)
    return bucket, key


def upload_file_to_s3(local_path: str, s3_uri: str) -> None:
    bucket, key = parse_s3_uri(s3_uri)
    client = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    client.upload_file(local_path, bucket, key)


def add_group_features(df, train_df, key_col: str, prefix: str):
    stats = (
        train_df
        .groupBy(key_col)
        .agg(
            count("*").alias(f"{prefix}_txn_count"),
            count(when(col("label") == 1, True)).alias(f"{prefix}_fraud_count"),
            avg("amount").alias(f"{prefix}_avg_amount"),
        )
        .withColumn(
            f"{prefix}_fraud_rate_smoothed",
            (col(f"{prefix}_fraud_count") + lit(1.0)) / (col(f"{prefix}_txn_count") + lit(100.0))
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # materialize stats once
    stats.count()

    return df.join(stats, on=key_col, how="left")


def evaluate_thresholds(y_true: np.ndarray, probabilities: np.ndarray, thresholds: list[float]) -> list[dict]:
    rows = []

    for threshold in thresholds:
        preds = (probabilities >= threshold).astype(int)

        tp = int(((preds == 1) & (y_true == 1)).sum())
        fp = int(((preds == 1) & (y_true == 0)).sum())
        fn = int(((preds == 0) & (y_true == 1)).sum())
        tn = int(((preds == 0) & (y_true == 0)).sum())

        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        false_positive_rate = fp / (fp + tn) if (fp + tn) else 0.0

        rows.append(
            {
                "threshold": threshold,
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "tn": tn,
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "false_positive_rate": false_positive_rate,
            }
        )

    return rows


def main() -> None:
    silver_bucket = get_env("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev")

    input_path = get_env(
        "S3_SCORED_TRANSACTIONS_INPUT",
        f"s3a://{silver_bucket}/silver/transactions_scored_parquet/",
    )

    input_format = get_env("S3_SCORED_TRANSACTIONS_FORMAT", "parquet").lower()

    scorecard_s3_uri = get_env(
        "MODEL_SCORECARD_S3_URI",
        "s3://finstream-silver-mostafa-dev/ml/models/fraud_scorecard/latest/model_scorecard.json",
    )

    review_threshold = float(get_env("ML_REVIEW_THRESHOLD", "0.30"))
    block_threshold = float(get_env("ML_BLOCK_THRESHOLD", "0.70"))

    # 0 means no limit
    max_training_rows = int(get_env("MAX_TRAINING_ROWS", "0"))

    spark = (
        SparkSession.builder
        .appName("FinStream Fraud Scorecard Training From S3")
        .config("spark.sql.shuffle.partitions", get_env("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .config("spark.default.parallelism", get_env("SPARK_DEFAULT_PARALLELISM", "8"))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    total_start = now_ts()

    log_step(f"Reading S3 training data from: {input_path} (format={input_format})")
    step_start = now_ts()

    if input_format == "parquet":
        df = spark.read.parquet(input_path)
    elif input_format == "json":
        df = spark.read.json(input_path)
    else:
        raise ValueError(f"Unsupported input format: {input_format}")

    log_elapsed(f"read_{input_format}_lazy", step_start)

    required_columns = [
        "transaction_id",
        "customer_id",
        "card_id",
        "merchant_id",
        "amount",
        "currency",
        "product_cd",
        "actual_is_fraud",
    ]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns in S3 input: {missing}")

    base_df = (
        df
        .select(*required_columns)
        .filter(col("actual_is_fraud").isNotNull())
        .withColumn("label", col("actual_is_fraud").cast("int"))
        .withColumn("amount", col("amount").cast("double"))
        .filter(col("transaction_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("card_id").isNotNull())
        .filter(col("merchant_id").isNotNull())
    )

    log_step("Counting base dataset and label distribution")
    step_start = now_ts()

    base_df = base_df.persist(StorageLevel.MEMORY_AND_DISK)

    total_rows = base_df.count()

    print(f"Base rows after filtering: {total_rows}", flush=True)
    print("Label distribution:", flush=True)
    base_df.groupBy("label").count().orderBy("label").show(truncate=False)

    log_elapsed("count_base_and_label_distribution", step_start)

    if total_rows == 0:
        raise RuntimeError("No rows available for training after filtering.")

    if max_training_rows > 0 and total_rows > max_training_rows:
        fraction = max_training_rows / total_rows
        log_step(f"Sampling dataset: total_rows={total_rows}, max_training_rows={max_training_rows}, fraction={fraction:.6f}")
        step_start = now_ts()

        base_df = (
            base_df
            .sample(withReplacement=False, fraction=fraction, seed=42)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        sampled_count = base_df.count()
        print(f"Sampled rows: {sampled_count}", flush=True)

        log_elapsed("sample_dataset", step_start)
    else:
        print(f"Using all available rows. max_training_rows={max_training_rows}", flush=True)

    log_step("Creating train/test split inside Spark")
    step_start = now_ts()

    # Split inside Spark instead of collecting ids to Pandas.
    # This is not perfectly stratified, but it avoids a heavy driver-side ids collection.
    split_df = base_df.withColumn("split_rand", rand(seed=42)).persist(StorageLevel.MEMORY_AND_DISK)

    train_df = (
        split_df
        .filter(col("split_rand") < 0.8)
        .drop("split_rand")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    test_df = (
        split_df
        .filter(col("split_rand") >= 0.8)
        .drop("split_rand")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    train_count = train_df.count()
    test_count = test_df.count()

    print(f"Train rows: {train_count}", flush=True)
    print(f"Test rows:  {test_count}", flush=True)

    print("Train label distribution:", flush=True)
    train_df.groupBy("label").count().orderBy("label").show(truncate=False)

    print("Test label distribution:", flush=True)
    test_df.groupBy("label").count().orderBy("label").show(truncate=False)

    if train_count == 0 or test_count == 0:
        raise RuntimeError("Train/test split produced empty dataset.")

    log_elapsed("spark_train_test_split", step_start)

    log_step("Building group features")
    step_start = now_ts()

    train_features_df = train_df
    test_features_df = test_df

    for key_col, prefix in [
        ("merchant_id", "merchant"),
        ("customer_id", "customer"),
        ("card_id", "card"),
    ]:
        print(f"Adding group features for {key_col} -> {prefix}", flush=True)
        train_features_df = add_group_features(train_features_df, train_df, key_col, prefix)
        test_features_df = add_group_features(test_features_df, train_df, key_col, prefix)

    log_elapsed("build_group_features", step_start)

    fill_cols = [
        "merchant_txn_count",
        "merchant_fraud_count",
        "merchant_avg_amount",
        "merchant_fraud_rate_smoothed",
        "customer_txn_count",
        "customer_fraud_count",
        "customer_avg_amount",
        "customer_fraud_rate_smoothed",
        "card_txn_count",
        "card_fraud_count",
        "card_avg_amount",
        "card_fraud_rate_smoothed",
    ]

    def finalize_features(feature_df):
        return (
            feature_df
            .fillna(0.0, subset=fill_cols)
            .withColumn(
                "amount_to_customer_avg",
                when(col("customer_avg_amount") > 0, col("amount") / col("customer_avg_amount")).otherwise(lit(0.0))
            )
            .withColumn(
                "amount_to_card_avg",
                when(col("card_avg_amount") > 0, col("amount") / col("card_avg_amount")).otherwise(lit(0.0))
            )
            .withColumn(
                "amount_to_merchant_avg",
                when(col("merchant_avg_amount") > 0, col("amount") / col("merchant_avg_amount")).otherwise(lit(0.0))
            )
        )

    train_features_df = finalize_features(train_features_df).persist(StorageLevel.MEMORY_AND_DISK)
    test_features_df = finalize_features(test_features_df).persist(StorageLevel.MEMORY_AND_DISK)

    feature_columns = [
        "amount",
        "merchant_fraud_rate_smoothed",
        "merchant_fraud_count",
        "merchant_txn_count",
        "merchant_avg_amount",
        "customer_fraud_rate_smoothed",
        "customer_fraud_count",
        "customer_txn_count",
        "customer_avg_amount",
        "card_fraud_rate_smoothed",
        "card_fraud_count",
        "card_txn_count",
        "card_avg_amount",
        "amount_to_customer_avg",
        "amount_to_card_avg",
        "amount_to_merchant_avg",
    ]

    log_step("Converting final feature frames to Pandas for sklearn")
    step_start = now_ts()

    train_pd = train_features_df.select(*(feature_columns + ["label"])).toPandas()
    test_pd = test_features_df.select(*(feature_columns + ["label"])).toPandas()

    print(f"train_pd shape: {train_pd.shape}", flush=True)
    print(f"test_pd shape:  {test_pd.shape}", flush=True)

    log_elapsed("to_pandas_final_features", step_start)

    x_train_raw = train_pd[feature_columns].astype("float64").to_numpy()
    y_train = train_pd["label"].astype("int32").to_numpy()

    x_test_raw = test_pd[feature_columns].astype("float64").to_numpy()
    y_test = test_pd["label"].astype("int32").to_numpy()

    if len(np.unique(y_train)) < 2:
        raise RuntimeError("Training labels contain only one class. Cannot train fraud model.")

    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()

    x_train_imputed = imputer.fit_transform(x_train_raw)
    x_test_imputed = imputer.transform(x_test_raw)

    x_train_scaled = scaler.fit_transform(x_train_imputed)
    x_test_scaled = scaler.transform(x_test_imputed)

    model = LogisticRegression(
        class_weight="balanced",
        max_iter=300,
        solver="lbfgs",
        n_jobs=1,
        random_state=42,
        tol=1e-4,
    )

    log_step("Training LogisticRegression fraud scorecard")
    step_start = now_ts()

    model.fit(x_train_scaled, y_train)

    log_elapsed("sklearn_logistic_regression_fit", step_start)

    probabilities = model.predict_proba(x_test_scaled)[:, 1]

    roc_auc = roc_auc_score(y_test, probabilities)
    pr_auc = average_precision_score(y_test, probabilities)

    thresholds = [0.70, 0.50, 0.30, 0.20, 0.10, 0.05, 0.02, 0.01]
    threshold_metrics = evaluate_thresholds(y_test, probabilities, thresholds)

    print(f"ROC_AUC={roc_auc}", flush=True)
    print(f"PR_AUC={pr_auc}", flush=True)
    print("Threshold metrics:", flush=True)
    print(json.dumps(threshold_metrics, indent=2), flush=True)

    imputer_medians = {
        feature_name: float(value)
        for feature_name, value in zip(feature_columns, imputer.statistics_)
    }

    scaler_means = {
        feature_name: float(value)
        for feature_name, value in zip(feature_columns, scaler.mean_)
    }

    scaler_scales = {
        feature_name: float(value)
        for feature_name, value in zip(feature_columns, scaler.scale_)
    }

    weights = {
        feature_name: float(value)
        for feature_name, value in zip(feature_columns, model.coef_[0])
    }

    model_version = datetime.now(timezone.utc).strftime("fraud_scorecard_%Y%m%dT%H%M%SZ")

    artifact = {
        "model_version": model_version,
        "model_type": "logistic_scorecard",
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": input_path,
        "max_training_rows": max_training_rows,
        "train_rows": int(len(train_pd)),
        "test_rows": int(len(test_pd)),
        "feature_columns": feature_columns,
        "intercept": float(model.intercept_[0]),
        "weights": weights,
        "imputer_strategy": "median",
        "imputer_medians": imputer_medians,
        "scaler_type": "standard",
        "scaler_means": scaler_means,
        "scaler_scales": scaler_scales,
        "review_threshold": review_threshold,
        "block_threshold": block_threshold,
        "metrics": {
            "roc_auc": float(roc_auc),
            "pr_auc": float(pr_auc),
            "threshold_metrics": threshold_metrics,
        },
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, "model_scorecard.json")

        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(artifact, f, indent=2)

        print(f"Uploading scorecard model to: {scorecard_s3_uri}", flush=True)
        upload_file_to_s3(local_path, scorecard_s3_uri)

    log_elapsed("total_training_job", total_start)

    print("Scorecard training finished successfully.", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()