-- [Gold DWS] Address Detective: Financial Metrics Wide Table
CREATE TABLE IF NOT EXISTS signal_hub.gold_address_financials_daily (
    trader_address String,
    calc_date Date,

    -- Core Financial Metrics
    total_trades_30d UInt32,
    win_rate Float64,
    profit_loss_ratio Float64,
    sharpe_ratio Float64,
    max_drawdown Float64,

    -- Scale and Activity
    total_volume_usd Float64,
    active_days_30d UInt8,

    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
-- Partition by Month based on the calculation date
PARTITION BY toYYYYMM(calc_date)
ORDER BY (trader_address, calc_date);