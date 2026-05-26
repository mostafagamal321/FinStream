CREATE TABLE IF NOT EXISTS finstream.market_signals_rt
(
    event_time DateTime64(3),
    symbol String,
    price Float64,
    volume Nullable(Int64),
    price_change_pct Nullable(Float64),
    volume_spike_score Nullable(Float64),
    volatility_score Nullable(Float64),
    signal_type String,
    signal_strength String
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (event_time, symbol);