# FinStream — Real-Time Financial Intelligence Platform




![Project Status](https://img.shields.io/badge/status-in%20progress-yellow)
![Build](https://img.shields.io/badge/build-in%20development-orange)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![Kafka](https://img.shields.io/badge/kafka-streaming-black)
![Flink](https://img.shields.io/badge/flink-real--time-orange)
![ClickHouse](https://img.shields.io/badge/clickhouse-analytics-yellow)
![Machine Learning](https://img.shields.io/badge/ML-fraud%20detection-purple)

# project architecture
<img width="3020" height="2444" alt="FinStream_Diagram_page-0001 (2)" src="https://github.com/user-attachments/assets/98c2c610-8297-4187-b5e8-c3ab45656344" />

FinStream is a real-time financial intelligence platform that simulates how a modern bank, fintech, or investment-risk team turns fast-moving financial data into business decisions. The platform processes transaction data, stock market data, and company news data to support three use cases: real-time fraud detection, dynamic risk scoring, and market intelligence.

## Business Case

Financial institutions need to react to financial events as they happen, not after daily batch processing. FinStream uses transaction events to detect suspicious payment behavior, historical and real-time transaction patterns to update risk indicators, and stock/news streams to detect market anomalies and sentiment-driven market signals.

## Data Domains

| Data Domain | Main Use Case | Output |
|---|---|---|
| Transaction data | Fraud detection and transaction risk | Fraud score, risk level, fraud alert |
| Historical transactions | Model training and risk baselines | Fraud model, customer/card risk features |
| Real-time stock data | Market monitoring | Price anomaly, volume spike, volatility signal |
| Historical stock data | Market baseline | Normal price/volume behavior |
| Live news data | Real-time sentiment monitoring | Sentiment signal, news event alert |
| Historical news data | Sentiment baseline/training | Sentiment trend, correlation with price movement |

## Target Architecture

```text
Sources
  -> Kafka Topics + Schema Registry
  -> Flink Real-Time Processing
  -> Outputs:
       1. Amazon S3 Bronze raw audit events
       2. ClickHouse real-time scores and signals
       3. Kafka alert topics
  -> Amazon S3 + Iceberg Medallion Lakehouse: Bronze -> Silver -> Gold
  -> Spark/dbt/Great Expectations Batch Processing
  -> MLflow Model Training + Registry
  -> Redis Feature Store Updates
  -> Superset/Grafana Dashboards and Alerts
```

## Repository Structure

```text
finstream/
├── configs/              # Service and tool configuration files
├── data/                 # Local sample/raw data placeholders
├── docker/               # Docker build assets
├── docs/                 # Architecture and project documentation
├── notebooks/            # Exploration and modeling notebooks
├── schemas/              # Avro/JSON schemas for event contracts
├── src/                  # Application source code
│   ├── alerting/          # Alert consumers and notification logic
│   ├── batch/             # Spark/dbt batch jobs
│   ├── common/            # Shared utilities
│   ├── flink_jobs/        # Real-time stream processing jobs
│   ├── ml/                # Training and model serving code
│   └── producers/         # Data producers for transactions/stocks/news
└── tests/                # Unit and integration tests
```

## First Implementation Phases


## Status

Initial repository 
