import json
import os
import tempfile
from datetime import datetime, timezone

import boto3
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


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
    boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1")).upload_file(
        local_path,
        bucket,
        key,
    )


def main() -> None:
    bronze_bucket = get_env("FINSTREAM_BRONZE_BUCKET", "finstream-bronze-mostafa-dev")
    silver_bucket = get_env("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev")

    input_path = get_env(
        "S3_SCORED_TRANSACTIONS_INPUT",
        f"s3a://{bronze_bucket}/bronze/transactions_scored_flink/*/*/part-*",
    )

    model_s3_uri = get_env(
        "MODEL_S3_URI",
        f"s3://{silver_bucket}/ml/serving/fraud_model_latest/model.joblib",
    )

    metadata_s3_uri = get_env(
        "MODEL_METADATA_S3_URI",
        f"s3://{silver_bucket}/ml/serving/fraud_model_latest/metadata.json",
    )

    model_threshold = float(get_env("MODEL_THRESHOLD", "0.20"))

    max_training_rows = int(get_env("MAX_TRAINING_ROWS", "200000"))

    spark = (
        SparkSession.builder
        .appName("FinStream Serving Fraud Model Training")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading training data from: {input_path}")

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
    ]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns in S3 input: {missing}")

    df = (
        df
        .select(*required_columns)
        .filter(col("actual_is_fraud").isNotNull())
        .withColumn("label", col("actual_is_fraud").cast("int"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("currency", when(col("currency").isNull(), "UNKNOWN").otherwise(col("currency")))
        .withColumn("product_cd", when(col("product_cd").isNull(), "UNKNOWN").otherwise(col("product_cd")))
    )

    label_counts = df.groupBy("label").count().orderBy("label")
    print("Label distribution:")
    label_counts.show(truncate=False)

    total_rows = df.count()
    if total_rows > max_training_rows:
        fraction = max_training_rows / total_rows
        print(f"Sampling training data: total={total_rows}, fraction={fraction}")
        df = df.sample(withReplacement=False, fraction=fraction, seed=42)

    pandas_df = df.toPandas()

    feature_columns = [
        "amount",
        "currency",
        "product_cd",
    ]

    target_column = "label"

    x = pandas_df[feature_columns]
    y = pandas_df[target_column]

    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    numeric_features = ["amount"]
    categorical_features = ["currency", "product_cd"]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                    ]
                ),
                numeric_features,
            ),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_features,
            ),
        ]
    )

    model = HistGradientBoostingClassifier(
        max_iter=200,
        learning_rate=0.05,
        max_leaf_nodes=31,
        l2_regularization=0.1,
        random_state=42,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    print("Training serving model...")
    pipeline.fit(x_train, y_train)

    probabilities = pipeline.predict_proba(x_test)[:, 1]

    roc_auc = roc_auc_score(y_test, probabilities)
    pr_auc = average_precision_score(y_test, probabilities)

    threshold_predictions = (probabilities >= model_threshold).astype(int)

    tp = int(((threshold_predictions == 1) & (y_test.to_numpy() == 1)).sum())
    fp = int(((threshold_predictions == 1) & (y_test.to_numpy() == 0)).sum())
    fn = int(((threshold_predictions == 0) & (y_test.to_numpy() == 1)).sum())
    tn = int(((threshold_predictions == 0) & (y_test.to_numpy() == 0)).sum())

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0

    model_version = datetime.now(timezone.utc).strftime("fraud_model_%Y%m%dT%H%M%SZ")

    metadata = {
        "model_version": model_version,
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_type": "sklearn_hist_gradient_boosting",
        "input_path": input_path,
        "feature_columns": feature_columns,
        "threshold": model_threshold,
        "metrics": {
            "roc_auc": float(roc_auc),
            "pr_auc": float(pr_auc),
            "threshold": float(model_threshold),
            "precision_at_threshold": float(precision),
            "recall_at_threshold": float(recall),
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
        },
    }

    print("Metrics:")
    print(json.dumps(metadata["metrics"], indent=2))

    with tempfile.TemporaryDirectory() as tmpdir:
        local_model_path = os.path.join(tmpdir, "model.joblib")
        local_metadata_path = os.path.join(tmpdir, "metadata.json")

        joblib.dump(pipeline, local_model_path)

        with open(local_metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        print(f"Uploading model to: {model_s3_uri}")
        upload_file_to_s3(local_model_path, model_s3_uri)

        print(f"Uploading metadata to: {metadata_s3_uri}")
        upload_file_to_s3(local_metadata_path, metadata_s3_uri)

    print("Serving model training finished successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
