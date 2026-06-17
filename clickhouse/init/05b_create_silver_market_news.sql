CREATE TABLE IF NOT EXISTS finstream.silver_market_news
(
    event_id                String,
    event_time              Nullable(String),
    published_at            Nullable(String),
    source_name             Nullable(String),
    author                  Nullable(String),
    title                   Nullable(String),
    description             Nullable(String),
    url                     Nullable(String),
    symbol_query            Nullable(String),
    source                  Nullable(String),
    company                 Nullable(String),
    sentiment               Nullable(String),
    sentiment_score         Nullable(Float64),
    dt                      Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, event_id)
SETTINGS index_granularity = 8192
