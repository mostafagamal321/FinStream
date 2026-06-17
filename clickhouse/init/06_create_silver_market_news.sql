CREATE TABLE IF NOT EXISTS finstream.silver_market_news
(
    event_id                String,
    event_time              Nullable(String),
    event_time_ms           Nullable(Int64),
    published_at            Nullable(String),
    source_name             Nullable(String),
    author                  Nullable(String),
    title                   Nullable(String),
    description             Nullable(String),
    url                     Nullable(String),
    symbol_query            Nullable(String),
    source                  Nullable(String),
    content_text            Nullable(String),
    sentiment_label         Nullable(String),
    risk_category           Nullable(String),
    severity                Nullable(String),
    negation_flag           Nullable(Bool),
    matched_keywords        Nullable(String),
    content_length          Nullable(Int32),
    ingestion_time_ms       Nullable(Int64),
    source_lag_ms           Nullable(Int64),
    raw_payload             Nullable(String),
    event_day               Date,
    silver_processed_at     Nullable(DateTime64(3)),
    silver_version          Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_day)
ORDER BY (event_day, event_id)
SETTINGS index_granularity = 8192
