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

CREATE TABLE IF NOT EXISTS finstream.fraud_scores_rt_enriched
(
    inserted_at DateTime('UTC') DEFAULT now(),

    event_id String,
    transaction_id String,

    event_time DateTime64(3, 'UTC'),
    ingestion_time DateTime64(3, 'UTC'),
    source LowCardinality(String),
    split LowCardinality(String),
    transaction_dt Int64,

    amount Float64,
    currency LowCardinality(String),
    product_cd Nullable(String),

    customer_id String,
    card_id String,
    merchant_id String,

    card_brand Nullable(String),
    card_type Nullable(String),
    addr1 Nullable(Float64),
    addr2 Nullable(Float64),

    payer_email_domain Nullable(String),
    receiver_email_domain Nullable(String),
    device_type Nullable(String),
    device_info Nullable(String),

    actual_is_fraud Nullable(Int32),

    rule_score Float64,
    ml_score Nullable(Float64),
    fraud_score Float64,

    scoring_method LowCardinality(String),
    risk_level LowCardinality(String),
    decision LowCardinality(String),
    reason_codes String,

    source_topic String,
    source_partition Int32,
    source_offset Int64
)
ENGINE = MergeTree
PARTITION BY toDate(inserted_at)
ORDER BY (inserted_at, decision, risk_level, transaction_id)
TTL inserted_at + INTERVAL 14 DAY;

