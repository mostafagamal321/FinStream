import json
import os
import tempfile
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, when, lit

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split

from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType


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


def safe_division_column(numerator_col: str, denominator_col: str):
    return when(col(denominator_col) > 0, col(numerator_col) / col(denominator_col)).otherwise(lit(0.0))


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
            f"{prefix}_fraud_rate",
            when(col(f"{prefix}_txn_count") > 0, col(f"{prefix}_fraud_count") / col(f"{prefix}_txn_count"))
            .otherwise(lit(0.0))
        )
        .withColumn(
            f"{prefix}_fraud_rate_smoothed",
            (col(f"{prefix}_fraud_count") + lit(1.0)) / (col(f"{prefix}_txn_count") + lit(100.0))
        )
    )

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
    bronze_bucket = get_env("FINSTREAM_BRONZE_BUCKET", "finstream-bronze-mostafa-dev")

    input_path = get_env(
        "S3_SCORED_TRANSACTIONS_INPUT",
        f"s3a://{bronze_bucket}/bronze/transactions_scored_flink/*/*/part-*",
    )

    model_onnx_s3_uri = get_env(
        "MODEL_ONNX_S3_URI",
        "s3://finstream-silver-mostafa-dev/ml/models/fraud_gbdt_onnx/latest/fraud_model.onnx",
    )

    metadata_s3_uri = get_env(
        "MODEL_METADATA_S3_URI",
        "s3://finstream-silver-mostafa-dev/ml/models/fraud_gbdt_onnx/latest/metadata.json",
    )

    review_threshold = float(get_env("ML_REVIEW_THRESHOLD", "0.10"))
    block_threshold = float(get_env("ML_BLOCK_THRESHOLD", "0.30"))
    max_training_rows = int(get_env("MAX_TRAINING_ROWS", "200000"))

    spark = (
        SparkSession.builder
        .appName("FinStream Fraud ONNX Training From S3")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading S3 training data from: {input_path}")

    df = spark.read.json(input_path)

    required_columns = [
        "transaction_id",
        "customer_id",
        "card_id",
        "merchant_id",
        "amount",
        "currency",
        "product_cd",
        "actual_is_fraud",
        "event_time",
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
        .withColumn("currency", when(col("currency").isNull(), "UNKNOWN").otherwise(col("currency")))
        .withColumn("product_cd", when(col("product_cd").isNull(), "UNKNOWN").otherwise(col("product_cd")))
    )

    print("Label distribution:")
    base_df.groupBy("label").count().orderBy("label").show(truncate=False)

    total_rows = base_df.count()
    if total_rows > max_training_rows:
        fraction = max_training_rows / total_rows
        print(f"Sampling dataset: total_rows={total_rows}, fraction={fraction}")
        base_df = base_df.sample(withReplacement=False, fraction=fraction, seed=42)

    pandas_base = base_df.select("transaction_id", "label").toPandas()

    train_ids, test_ids = train_test_split(
        pandas_base["transaction_id"],
        test_size=0.2,
        random_state=42,
        stratify=pandas_base["label"],
    )

    train_ids_df = spark.createDataFrame(pd.DataFrame({"transaction_id": train_ids}))
    test_ids_df = spark.createDataFrame(pd.DataFrame({"transaction_id": test_ids}))

    train_df = base_df.join(train_ids_df, on="transaction_id", how="inner")
    test_df = base_df.join(test_ids_df, on="transaction_id", how="inner")

    train_features_df = train_df
    test_features_df = test_df

    for key_col, prefix in [
        ("merchant_id", "merchant"),
        ("customer_id", "customer"),
        ("card_id", "card"),
    ]:
        train_features_df = add_group_features(train_features_df, train_df, key_col, prefix)
        test_features_df = add_group_features(test_features_df, train_df, key_col, prefix)

    for feature_df_name, feature_df in [
        ("train", train_features_df),
        ("test", test_features_df),
    ]:
        feature_df = (
            feature_df
            .fillna(0.0, subset=[
                "merchant_txn_count",
                "merchant_fraud_count",
                "merchant_avg_amount",
                "merchant_fraud_rate",
                "merchant_fraud_rate_smoothed",
                "customer_txn_count",
                "customer_fraud_count",
                "customer_avg_amount",
                "customer_fraud_rate",
                "customer_fraud_rate_smoothed",
                "card_txn_count",
                "card_fraud_count",
                "card_avg_amount",
                "card_fraud_rate",
                "card_fraud_rate_smoothed",
            ])
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

        if feature_df_name == "train":
            train_features_df = feature_df
        else:
            test_features_df = feature_df

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

    train_pd = train_features_df.select(*(feature_columns + ["label"])).toPandas()
    test_pd = test_features_df.select(*(feature_columns + ["label"])).toPandas()

    x_train = train_pd[feature_columns].astype("float32").to_numpy()
    y_train = train_pd["label"].astype("int32").to_numpy()

    x_test = test_pd[feature_columns].astype("float32").to_numpy()
    y_test = test_pd["label"].astype("int32").to_numpy()

    imputer = SimpleImputer(strategy="median")

    x_train_imputed = imputer.fit_transform(x_train).astype("float32")
    x_test_imputed = imputer.transform(x_test).astype("float32")

    model = GradientBoostingClassifier(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=3,
        random_state=42,
    )

    print("Training GradientBoostingClassifier...")
    model.fit(x_train_imputed, y_train)

    probabilities = model.predict_proba(x_test_imputed)[:, 1]

    roc_auc = roc_auc_score(y_test, probabilities)
    pr_auc = average_precision_score(y_test, probabilities)

    thresholds = [0.50, 0.30, 0.20, 0.10, 0.05, 0.02, 0.01]
    threshold_metrics = evaluate_thresholds(y_test, probabilities, thresholds)

    print(f"ROC_AUC={roc_auc}")
    print(f"PR_AUC={pr_auc}")
    print("Threshold metrics:")
    print(json.dumps(threshold_metrics, indent=2))

    initial_type = [
        ("float_input", FloatTensorType([None, len(feature_columns)]))
    ]

    onnx_model = convert_sklearn(
        model,
        name="finstream_fraud_gbdt",
        initial_types=initial_type,
        target_opset={"": 17, "ai.onnx.ml": 3},
        options={id(model): {"zipmap": False}},
    )

    model_version = datetime.now(timezone.utc).strftime("fraud_gbdt_onnx_%Y%m%dT%H%M%SZ")

    imputer_medians = {
        feature_name: float(value)
        for feature_name, value in zip(feature_columns, imputer.statistics_)
    }

    metadata = {
        "model_version": model_version,
        "model_type": "sklearn_gradient_boosting_classifier_onnx",
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": input_path,
        "feature_columns": feature_columns,
        "onnx_input_name": "float_input",
        "review_threshold": review_threshold,
        "block_threshold": block_threshold,
        "imputer_strategy": "median",
        "imputer_medians": imputer_medians,
        "metrics": {
            "roc_auc": float(roc_auc),
            "pr_auc": float(pr_auc),
            "threshold_metrics": threshold_metrics,
        },
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        local_onnx_path = os.path.join(tmpdir, "fraud_model.onnx")
        local_metadata_path = os.path.join(tmpdir, "metadata.json")

        with open(local_onnx_path, "wb") as f:
            f.write(onnx_model.SerializeToString())

        with open(local_metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        print(f"Uploading ONNX model to: {model_onnx_s3_uri}")
        upload_file_to_s3(local_onnx_path, model_onnx_s3_uri)

        print(f"Uploading metadata to: {metadata_s3_uri}")
        upload_file_to_s3(local_metadata_path, metadata_s3_uri)

    print("ONNX training finished successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
