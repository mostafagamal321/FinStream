-- FinStream — Silver Market Ticks Table
-- Owner  : Saleh (E3)
-- Layer  : Silver (Batch — Spark)
-- Source : s3a://<silver-bucket>/silver/market_ticks/
-- Load   : spark/jobs/silver_to_clickhouse_market_ticks.py

CREATE TABLE IF NOT EXISTS finstream.silver_market_ticks
(
    -- Core identifiers
    event_id                String,
    event_time              Nullable(String),
    event_time_ms           Nullable(Int64),
    ingestion_time_ms       Nullable(Int64),
    source_lag_ms           Nullable(Float64),
    -- Market data
    symbol                  String,
    price                   Nullable(Float64),
    open_price              Nullable(Float64),
    high_price              Nullable(Float64),
    low_price               Nullable(Float64),
    close_price             Nullable(Float64),
    volume                  Nullable(Float64),
    -- Metadata
    source                  Nullable(String),
    anomaly_flag            Nullable(String),
    raw_payload             Nullable(String),
    -- Partitioning
    dt                      Date,
    silver_processed_at     Nullable(DateTime64(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, symbol, event_id)
SETTINGS index_granularity = 8192;


-- FinStream — Silver Market News Table
-- Owner  : Saleh (E3)
-- Layer  : Silver (Batch — Spark)
-- Source : s3a://<silver-bucket>/silver/market_news/
-- Load   : spark/jobs/silver_to_clickhouse_market_news.py

CREATE TABLE IF NOT EXISTS finstream.silver_market_news
(
    -- Core identifiers
    event_id                String,
    event_time              Nullable(String),
    published_at            Nullable(String),
    -- News content
    source_name             Nullable(String),
    author                  Nullable(String),
    title                   Nullable(String),
    description             Nullable(String),
    url                     Nullable(String),
    -- Market relevance
    symbol_query            Nullable(String),
    source                  Nullable(String),
    company                 Nullable(String),
    -- Sentiment
    sentiment               Nullable(String),
    sentiment_score         Nullable(Float64),
    -- Partitioning
    dt                      Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, company, event_id)
SETTINGS index_granularity = 8192;
