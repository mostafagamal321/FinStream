CREATE TABLE IF NOT EXISTS finstream.fraud_scores_rt
(
    event_time DateTime64(3),
    ingestion_time DateTime64(3),
    transaction_id String,
    customer_id String,
    card_id String,
    merchant_id String,
    amount Float64,
    currency String,
    fraud_score Float64,
    risk_level String,
    decision String,
    reason_codes Array(String)
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (event_time, transaction_id);

CREATE TABLE IF NOT EXISTS finstream.fraud_alerts
(
    alert_time DateTime64(3),
    transaction_id String,
    customer_id String,
    card_id String,
    merchant_id String,
    amount Float64,
    currency String,
    fraud_score Float64,
    risk_level String,
    decision String,
    reason_codes Array(String)
)
ENGINE = MergeTree
PARTITION BY toDate(alert_time)
ORDER BY (alert_time, fraud_score, transaction_id);