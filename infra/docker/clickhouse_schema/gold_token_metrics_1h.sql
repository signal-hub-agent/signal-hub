-- 【金层 DWS】信号参谋：代币多维指标宽表
CREATE TABLE IF NOT EXISTS signal_hub.gold_token_metrics_1h (
    token_symbol String,
    calc_time DateTime,

    -- 成交量趋势
    volume_24h_usd Float64,
    volume_7d_avg_usd Float64,
    volume_ratio Float64,
    tx_count_24h UInt32,

    -- MEV污染度
    mev_total_txs UInt32,
    mev_suspicious_txs UInt32,
    mev_toxicity_pct Float64,

    -- 技术面基础
    current_price Float64,
    ema7 Float64,
    ema25 Float64,
    ema99 Float64,
    rsi_14 Float64,

    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (token_symbol, calc_time);