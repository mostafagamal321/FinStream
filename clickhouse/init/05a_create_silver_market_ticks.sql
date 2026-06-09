CREATE TABLE IF NOT EXISTS finstream.silver_market_ticks
(
    event_id                String,
    event_time              Nullable(String),
    event_time_ms           Nullable(Int64),
    ingestion_time_ms       Nullable(Int64),
    source_lag_ms           Nullable(Float64),
    symbol                  String,
    price                   Nullable(Float64),
    open_price              Nullable(Float64),
    high_price              Nullable(Float64),
    low_price               Nullable(Float64),
    close_price             Nullable(Float64),
    volume                  Nullable(Float64),
    source                  Nullable(String),
    anomaly_flag            Nullable(String),
    raw_payload             Nullable(String),
    dt                      Date,
    silver_processed_at     Nullable(DateTime64(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, symbol, event_id)
SETTINGS index_granularity = 8192
