# Storage Architecture: Amazon S3 + Apache Iceberg

FinStream uses Amazon S3 as the cloud object storage layer for the lakehouse. S3 stores raw Bronze events, cleaned Silver datasets, curated Gold tables, checkpoints/artifacts where needed, and historical data used for replay and model training.

Apache Iceberg sits on top of S3 to provide table-level capabilities such as schema evolution, partition evolution, snapshots, and time travel. This means S3 remains the durable storage layer, while Iceberg provides governed lakehouse table management.

## Medallion Layout

```text
s3://finstream-lakehouse/bronze/
s3://finstream-lakehouse/silver/
s3://finstream-lakehouse/gold/
```

## Why S3 Instead of MinIO

S3 is the production cloud storage target used in many enterprise data platforms. MinIO is useful for local S3-compatible development, but FinStream is now positioned as a cloud-ready lakehouse using S3 directly.
