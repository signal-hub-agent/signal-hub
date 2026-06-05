-- [Silver DWD] Standardized Kline Time Series
CREATE TABLE IF NOT EXISTS signal_hub.clean_klines (
    token_symbol String,
    interval String,
    open_time DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    turnover Float64,
    source String DEFAULT 'bybit',
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(open_time)
ORDER BY (token_symbol, interval, open_time);