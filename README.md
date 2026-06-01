# FinStream — Real-Time Financial Intelligence Platform

![Project Status](https://img.shields.io/badge/status-in%20progress-yellow)
![Build](https://img.shields.io/badge/build-in%20development-orange)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![Kafka](https://img.shields.io/badge/kafka-streaming-grey)
![Static Badge](https://img.shields.io/badge/Spark-Batch%20Processing-Brightgreen)
![Flink](https://img.shields.io/badge/flink-real--time-orange)
![ClickHouse](https://img.shields.io/badge/clickhouse-analytics-yellow)
![Machine Learning](https://img.shields.io/badge/ML-fraud%20detection-purple)

# FinStream architecture

FinStream is a real-time financial intelligence platform that simulates how a modern bank, fintech, or investment-risk team turns fast-moving financial data into business decisions. The platform processes transaction data, stock market data, and company news data to support three use cases: real-time fraud detection, dynamic risk scoring, and market intelligence.

<img width="1405" height="1082" alt="FinStream_Architecture_page-0001 (2)" src="https://github.com/user-attachments/assets/a8cd6f40-df4b-4c80-b18c-5408c9c8b76b" />

## Business Case

Financial institutions need to react to financial events as they happen, not after daily batch processing. FinStream uses transaction events to detect **suspicious payment behavior**, historical and real-time transaction patterns to update **risk indicators**, and **stock/news streams** to detect market anomalies and sentiment-driven market signals By.

## Problem We trying to solve: 
- Delayed fraud detection , In many systems fraud is detected after the transaction has already happened. 
- Outdated risk scores , Traditional risk scores are often updated daily or weekly. That means the customer risk profile can be stale.
- Slow market awareness , Financial exposure can change quickly because of price movement, volume spikes, or negative news.


---
## Data Sources:
| Data source                    | Source URL                                                                  | 
| ------------------------------ | ----------------------------------------------------------------------------| 
| **IEEE-CIS Fraud Transcation** | https://www.kaggle.com/competitions/ieee-fraud-detection/data               |
| **Historical market data**     | [https://finnhub.io/docs/api](https://finnhub.io/dashboard)                 |
| **Live market ticks**          | [Real-time market movement and signals](https://finnhub.io/dashboard)       |                        
| **Historical financial news**  | [Batch news ingestion, sentiment/history analysis](https://newsapi.org/)    |             
| **Live/recent financial news** | [Real-time market intelligence and exposure alerts](https://newsapi.org/)   |            


## Data Domains

| Data Domain             | Main Use Case                        | Output                                           |
| ----------------------- | ------------------------------------ | ------------------------------------------------ |
| Transaction data        | Fraud detection and transaction risk | Fraud score, risk level, fraud alert             |
| Historical transactions | Model training and risk baselines    | Fraud model, customer/card risk features         |
| Real-time stock data    | Market monitoring                    | Price anomaly, volume spike, volatility signal   |
| Historical stock data   | Market baseline                      | Normal price/volume behavior                     |
| Live news data          | Real-time sentiment monitoring       | Sentiment signal, news event alert               |
| Historical news data    | Sentiment baseline/training          | Sentiment trend, correlation with price movement |

## Target Architecture

```text
Sources
  -> Kafka Topics + Schema Registry
  -> Flink Real-Time Processing
  -> Outputs:
       1. Amazon S3 Bronze raw audit events
       2. ClickHouse real-time scores and signals
       3. Kafka alert topics
  -> Amazon S3  Lakehouse: Bronze -> Silver 
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

Current completed work:

- Dockerized Python producer layer.
- Kafka and Schema Registry are running through Docker Compose.
- Producers now run inside Docker instead of local Python.
- Added Avro serialization through Confluent Schema Registry.
- Added live market producer using Finnhub REST API.
- Added real-time market stream producer using Finnhub WebSocket.
- Added live news producer using NewsAPI.
- Added historical market CSV replay producer.
- Added historical news CSV replay producer.
- Added mapper layer to normalize live and historical data into the same Kafka event contracts.
- Added Dead Letter Queue support for failed records.
- Added support for simulating historical CSV data as streaming events.
---

## 1.Market & news pipline setup: 
#### Start the core services:
```bash
docker compose up -d kafka schema-registry
docker compose up -d flink-jobmanager flink-taskmanager
```
#### submit Flink jobs:
```bash
docker exec -it finstream-flink-jobmanager flink run `
  -c com.finstream.flink.MarketBronzeAndSignalsJob `
  /opt/flink/jobs/flink-jobs.jar

docker exec -it finstream-flink-jobmanager flink run `
  -c com.finstream.flink.NewsBronzeAndSignalsJob `
  /opt/flink/jobs/flink-jobs.jar
```
#### Running Producers:
```bash
docker compose up -d --force-recreate historical-market-producer
docker compose up -d --force-recreate finnhub-stream-producer
docker compose up -d --force-recreate historical-news-producer
docker compose up -d --force-recreate news-producer
```
#### Check Proudcers Logs:
```bash
docker compose logs -f historical-market-producer
docker compose logs -f finnhub-stream-producer
docker compose logs -f historical-news-producer
docker compose logs -f news-producer
```

## Service URLs:
| Service           | URL                     |
| ----------------- | ----------------------- |
| Kafka Broker      | `localhost:29092`       |
| Schema Registry   | `http://localhost:8081` |
| Flink Dashboard   | `http://localhost:8082` |
| ClickHouse HTTP   | `http://localhost:8123` |
| ClickHouse Native | `localhost:9000`        |
| Airflow           | `http://localhost:8080` |
| Superset          | `http://localhost:8088` |
| Prometheus        | `http://localhost:9090` |
| Grafana           | `http://localhost:3000` |
---
## Recent Data Transformation

The market data was transformed from a wide historical dataframe format into normalized event records.

### Before

The raw historical market data came in a wide format:

```text
Date | Close | Close.1 | Close.2 | High | High.1 | Open | Open.1 | Volume | Volume.1 ...
```
<img width="1773" height="306" alt="Screenshot 2026-05-31 205453" src="https://github.com/user-attachments/assets/a60de76c-f835-4860-a480-2e799c73b3c1" />

This format is not ideal for Kafka, Flink, or S3 partitioned event storage.

### After

The data is converted into one event per symbol per timestamp:

```text
event_id
event_time
event_time_ms
ingestion_time_ms
source_lag_ms
symbol
price
open_price
high_price
low_price
close_price
volume
source
anomaly_flag
raw_payload
```

Example:

```text
hist-MSFT-1519862400000 | MSFT | 74.672501 | historical_market_replay | LATE_SOURCE_EVENT
hist-AMZN-1519862400000 | AMZN | 174.570053 | historical_market_replay | LATE_SOURCE_EVENT
hist-NVDA-1519862400000 | NVDA | 29.039000 | historical_market_replay | LATE_SOURCE_EVENT
```
<img width="1708" height="274" alt="Screenshot 2026-05-31 195145" src="https://github.com/user-attachments/assets/49da72a6-94cc-4df0-bfc6-042b3ab18d6a" />

---

## Creating IAM Roles and AWS Access

FinStream needs permission to read from and write to the project S3 buckets. For local development, the simplest setup is to create an IAM user with limited S3 permissions and place its access keys in the `.env` file.

For a more production-style setup, use an IAM role instead of long-lived access keys. IAM roles are safer because credentials are temporary and managed by AWS.

<img width="691" height="585" alt="Screenshot 2026-05-30 150521" src="https://github.com/user-attachments/assets/efacb647-a73e-4347-8321-9b2819cbb09b" />


### S3 Buckets

The project uses three S3 buckets:

```text
finstream-bronze-mostafa-dev
finstream-silver-mostafa-dev
finstream-mlflow-mostafa-dev
```
