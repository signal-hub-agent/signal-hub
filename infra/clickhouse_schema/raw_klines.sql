-- [Bronze ODS] Raw Bybit Klines
CREATE TABLE IF NOT EXISTS signal_hub.raw_klines (
    token_symbol String,
    interval String,
    raw_json String,
    fetch_timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(fetch_timestamp)
-- Partition by Month since Kline data volume per symbol is relatively small
PARTITION BY toYYYYMM(fetch_timestamp)
ORDER BY (token_symbol, interval, fetch_timestamp);