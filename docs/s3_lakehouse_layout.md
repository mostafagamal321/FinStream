# S3 Lakehouse Layout

FinStream currently uses AWS S3-compatible bucket configuration from `.env`.

## Buckets

| Bucket Variable | Purpose |
|---|---|
| `FINSTREAM_BRONZE_BUCKET` | Raw immutable events |
| `FINSTREAM_SILVER_BUCKET` | Cleaned and conformed datasets |
| `FINSTREAM_GOLD_BUCKET` | ML features, risk profiles, and marts |
| `FINSTREAM_CHECKPOINT_BUCKET` | Flink and Spark checkpoints |
| `MLFLOW_ARTIFACT_BUCKET` | MLflow artifacts and model files |

## Bronze Layer

```text
s3://<FINSTREAM_BRONZE_BUCKET>/
├── transactions_raw/event_date=YYYY-MM-DD/
├── market_ticks_raw/event_date=YYYY-MM-DD/
├── news_raw/event_date=YYYY-MM-DD/
├── fx_rates_raw/event_date=YYYY-MM-DD/
└── raw_audit_events/event_date=YYYY-MM-DD/