-- 【金层 DWS】地址侦探：地址财务指标宽表
CREATE TABLE IF NOT EXISTS signal_hub.gold_address_financials_daily (
    trader_address String,
    calc_date Date,

    -- 财务核心指标
    total_trades_30d UInt32,
    win_rate Float64,
    profit_loss_ratio Float64,
    sharpe_ratio Float64,
    max_drawdown Float64,

    -- 规模与活跃度
    total_volume_usd Float64,
    active_days_30d UInt8,

    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (trader_address, calc_date);