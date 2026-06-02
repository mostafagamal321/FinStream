CREATE DATABASE IF NOT EXISTS finstream;

CREATE TABLE IF NOT EXISTS finstream.fraud_scores_rt
(
    event_time String,
    scored_at DateTime64(3),
    transaction_id String,
    customer_id String,
    card_id String,
    merchant_id String,
    amount Float64,
    currency String,
    product_cd Nullable(String),
    rule_score Float64,
    ml_score Nullable(Float64),
    fraud_score Float64,
    scoring_method String,
    risk_level LowCardinality(String),
    decision LowCardinality(String),
    reason_codes String,
    actual_is_fraud Nullable(Int32),
    kafka_topic String,
    kafka_partition Int32,
    kafka_offset Int64,
    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toDate(inserted_at)
ORDER BY transaction_id;

CREATE OR REPLACE VIEW finstream.fraud_scores_rt_latest AS
SELECT *
FROM finstream.fraud_scores_rt
FINAL;

CREATE OR REPLACE VIEW finstream.v_fraud_risk_distribution AS
SELECT
    risk_level,
    decision,
    count() AS total_transactions,
    round(avg(fraud_score), 4) AS avg_fraud_score,
    round(sum(amount), 2) AS total_amount
FROM finstream.fraud_scores_rt_latest
GROUP BY risk_level, decision;

CREATE OR REPLACE VIEW finstream.v_high_risk_transactions AS
SELECT
    transaction_id,
    customer_id,
    card_id,
    merchant_id,
    amount,
    fraud_score,
    risk_level,
    decision,
    reason_codes,
    scored_at,
    inserted_at
FROM finstream.fraud_scores_rt_latest
WHERE risk_level = 'HIGH'
   OR decision = 'BLOCK';

CREATE OR REPLACE VIEW finstream.v_hourly_fraud_summary AS
SELECT
    toStartOfHour(inserted_at) AS hour,
    risk_level,
    decision,
    count() AS transactions,
    round(avg(fraud_score), 4) AS avg_score,
    round(sum(amount), 2) AS total_amount
FROM finstream.fraud_scores_rt_latest
GROUP BY hour, risk_level, decision
ORDER BY hour DESC;