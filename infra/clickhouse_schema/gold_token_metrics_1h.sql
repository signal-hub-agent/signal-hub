-- [Gold DWS] Token Signal: Multi-dimensional Metrics Wide Table
CREATE TABLE IF NOT EXISTS signal_hub.gold_token_metrics_1h (
    token_symbol String,
    calc_time DateTime,

    -- Volume Trends
    volume_24h_usd Float64,
    volume_7d_avg_usd Float64,
    volume_ratio Float64,
    tx_count_24h UInt32,

    -- MEV Toxicity
    mev_total_txs UInt32,
    mev_suspicious_txs UInt32,
    mev_toxicity_pct Float64,

    -- Technical Basics & Trend Indicators
    current_price Float64,
    ema7 Float64,
    ema25 Float64,
    ema99 Float64,
    rsi_14 Float64,

    -- MACD Indicators
    macd_dif Float64,
    macd_dea Float64,
    macd_hist Float64,

    -- Bollinger Bands
    boll_mid Float64,
    boll_up Float64,
    boll_low Float64,

    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(calc_time)
ORDER BY (token_symbol, calc_time);