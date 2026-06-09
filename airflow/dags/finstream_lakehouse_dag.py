"""
FinStream — Lakehouse Batch DAG
================================
Owner : Saleh (E3)
Flow  : Bronze → Silver (S3) → ClickHouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import boto3
import os

default_args = {
    "owner"            : "saleh-e3",
    "retries"          : 1,
    "retry_delay"      : timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

def get_latest_bronze_partition(**context):
    s3 = boto3.client(
        "s3",
        aws_access_key_id     = os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name           = os.environ.get("AWS_REGION", "us-east-1"),
    )
    bucket    = os.environ.get("FINSTREAM_BRONZE_BUCKET", "finstream-bronze-mostafa-dev")
    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=bucket, Prefix="bronze/transactions_raw/", Delimiter="/")

    dates = []
    for page in pages:
        for p in page.get("CommonPrefixes", []):
            prefix = p["Prefix"]
            if "event_date=" in prefix:
                dates.append(prefix.split("event_date=")[1].rstrip("/"))

    if not dates:
        raise ValueError("No Bronze partitions found in S3!")

    latest = sorted(dates)[-1]
    print(f"Latest Bronze partition: {latest}")
    context["ti"].xcom_push(key="bronze_date", value=latest)
    return latest


with DAG(
    dag_id            = "finstream_lakehouse_transactions",
    default_args      = default_args,
    description       = "Bronze → Silver → ClickHouse (incremental)",
    schedule_interval = "0 2 * * *",
    start_date        = datetime(2026, 1, 1),
    catchup           = False,
    tags              = ["finstream", "lakehouse", "e3"],
) as dag:

    detect_partition = PythonOperator(
        task_id         = "detect_latest_bronze_partition",
        python_callable = get_latest_bronze_partition,
    )

    # ── FIX: بدل Jinja env() بناخد القيم من os.environ مباشرة في Python ──
    bronze_to_silver = BashOperator(
        task_id      = "bronze_to_silver_transactions",
        bash_command = (
            "python /opt/airflow/spark/jobs/bronze_to_silver_transactions.py "
            "--date {{ ti.xcom_pull(task_ids='detect_latest_bronze_partition', key='bronze_date') }} "
        ),
        append_env = True,  # بياخد كل الـ env variables من الـ container تلقائياً
    )

    silver_to_clickhouse = BashOperator(
        task_id      = "silver_to_clickhouse_transactions",
        bash_command = (
            "python /opt/airflow/spark/jobs/silver_to_clickhouse_transactions.py "
            "--date {{ ti.xcom_pull(task_ids='detect_latest_bronze_partition', key='bronze_date') }} "
        ),
        env        = {"CLICKHOUSE_HOST": "finstream-clickhouse"},
        append_env = True,  # بياخد AWS credentials والباقي من الـ container
    )

    detect_partition >> bronze_to_silver >> silver_to_clickhouse
