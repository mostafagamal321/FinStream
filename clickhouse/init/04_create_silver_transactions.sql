-- FinStream — Silver Transactions Table
-- Owner  : Saleh 
-- Layer  : Silver (Batch — Spark)
-- Source : s3a://<silver-bucket>/silver/transactions/
-- Load   : spark/jobs/silver_to_clickhouse_transactions.py

CREATE TABLE IF NOT EXISTS finstream.silver_transactions
(
    -- Core identifiers
    transaction_id          String,
    customer_id             String,
    card_id                 String,
    merchant_id             String,

    -- Amount
    amount                  Float64,
    currency                String,
    amount_band             String,
    is_high_amount          Int8,

    -- Card info
    card_brand              Nullable(String),
    card_type               Nullable(String),

    -- Location / address
    addr1                   Nullable(String),
    addr2                   Nullable(String),

    -- Device
    device_type             Nullable(String),
    device_info             Nullable(String),
    has_device_info         Int8,

    -- Email
    payer_email_domain      Nullable(String),
    receiver_email_domain   Nullable(String),
    email_domain_type       String,

    -- Product
    product_cd              Nullable(String),

    -- Fraud label
    is_fraud                Int8,

    -- Time
    event_ts                DateTime64(3),
    ingestion_ts            Nullable(DateTime64(3)),
    s3_ingestion_ts         Nullable(DateTime64(3)),
    event_day               Date,
    event_hour              Int8,
    processing_lag_seconds  Nullable(Float64),

    -- Silver metadata
    silver_processed_at     DateTime64(3),
    silver_version          String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_day)
ORDER BY (event_day, customer_id, transaction_id)
SETTINGS index_granularity = 8192;
