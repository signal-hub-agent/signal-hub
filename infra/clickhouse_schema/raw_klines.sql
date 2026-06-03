-- 【铜层 ODS】存储 Bybit API 原始 K 线 JSON
CREATE TABLE IF NOT EXISTS signal_hub.raw_klines (
    token_symbol String,
    interval String,
    raw_json String,
    fetch_timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (token_symbol, interval, fetch_timestamp);