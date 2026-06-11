import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


def get_env(name: str, default: str) -> str:
    return os.getenv(name, default)


def main() -> None:
    bronze_bucket = get_env("FINSTREAM_BRONZE_BUCKET", "finstream-bronze-mostafa-dev")
    silver_bucket = get_env("FINSTREAM_SILVER_BUCKET", "finstream-silver-mostafa-dev")

    input_path = get_env(
        "S3_SCORED_TRANSACTIONS_INPUT",
        f"s3a://{bronze_bucket}/bronze/transactions_scored_flink/*/*/part-*",
    )

    model_output_path = get_env(
        "S3_FRAUD_MODEL_OUTPUT",
        f"s3a://{silver_bucket}/ml/models/fraud_random_forest_baseline",
    )

    metrics_output_path = get_env(
        "S3_FRAUD_METRICS_OUTPUT",
        f"s3a://{silver_bucket}/ml/metrics/fraud_random_forest_baseline",
    )

    spark = (
        SparkSession.builder
        .appName("FinStream Fraud ML Training From S3")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading scored transactions from: {input_path}")

    df = spark.read.json(input_path)

    required_columns = [
        "transaction_id",
        "customer_id",
        "card_id",
        "merchant_id",
        "amount",
        "currency",
        "product_cd",
        "rule_score",
        "fraud_score",
        "risk_level",
        "decision",
        "actual_is_fraud",
    ]

    existing_columns = set(df.columns)
    missing = [c for c in required_columns if c not in existing_columns]

    if missing:
        raise RuntimeError(f"Missing required columns in S3 input: {missing}")

    df = (
        df
        .select(*required_columns)
        .filter(col("actual_is_fraud").isNotNull())
        .withColumn("label", col("actual_is_fraud").cast("double"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("rule_score", col("rule_score").cast("double"))
        .withColumn("fraud_score", col("fraud_score").cast("double"))
        .withColumn("product_cd", when(col("product_cd").isNull(), "UNKNOWN").otherwise(col("product_cd")))
        .withColumn("currency", when(col("currency").isNull(), "UNKNOWN").otherwise(col("currency")))
        .withColumn("risk_level", when(col("risk_level").isNull(), "UNKNOWN").otherwise(col("risk_level")))
        .withColumn("decision", when(col("decision").isNull(), "UNKNOWN").otherwise(col("decision")))
    )

    print("Dataset label distribution:")
    df.groupBy("label").count().orderBy("label").show(truncate=False)

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    numeric_cols = [
        "amount",
        "rule_score",
        "fraud_score",
    ]

    categorical_cols = [
        "currency",
        "product_cd",
        "risk_level",
        "decision",
    ]

    imputed_numeric_cols = [f"{c}_imputed" for c in numeric_cols]

    imputer = Imputer(
        inputCols=numeric_cols,
        outputCols=imputed_numeric_cols,
    )

    indexers = [
        StringIndexer(
            inputCol=c,
            outputCol=f"{c}_idx",
            handleInvalid="keep",
        )
        for c in categorical_cols
    ]

    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in categorical_cols],
        outputCols=[f"{c}_ohe" for c in categorical_cols],
        handleInvalid="keep",
    )

    assembler = VectorAssembler(
        inputCols=imputed_numeric_cols + [f"{c}_ohe" for c in categorical_cols],
        outputCol="features",
    )

    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="rawPrediction",
        numTrees=80,
        maxDepth=8,
        seed=42,
    )

    pipeline = Pipeline(
        stages=indexers + [encoder, imputer, assembler, rf]
    )

    print("Training RandomForest model...")
    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)

    auc_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )

    pr_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )

    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy",
    )

    auc = auc_evaluator.evaluate(predictions)
    pr_auc = pr_evaluator.evaluate(predictions)
    accuracy = accuracy_evaluator.evaluate(predictions)

    confusion = (
        predictions
        .groupBy("label", "prediction")
        .count()
        .orderBy("label", "prediction")
    )

    print("Confusion matrix:")
    confusion.show(truncate=False)

    print(f"ROC_AUC={auc}")
    print(f"PR_AUC={pr_auc}")
    print(f"ACCURACY={accuracy}")

    print(f"Saving model to: {model_output_path}")
    model.write().overwrite().save(model_output_path)

    metrics_df = spark.createDataFrame(
        [
            ("roc_auc", float(auc)),
            ("pr_auc", float(pr_auc)),
            ("accuracy", float(accuracy)),
        ],
        ["metric", "value"],
    )

    print(f"Saving metrics to: {metrics_output_path}")
    metrics_df.coalesce(1).write.mode("overwrite").json(metrics_output_path)

    print("ML training finished successfully.")

    spark.stop()


if __name__ == "__main__":
    main()