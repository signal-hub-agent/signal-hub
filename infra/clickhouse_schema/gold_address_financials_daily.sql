CREATE TABLE IF NOT EXISTS signal_hub.gold_address_financials_daily (
    trader_address String COMMENT 'Lowercase wallet address',
    calc_date Date COMMENT 'The reporting snapshot date',

    -- 1. Hard Core Financial Metrics
    total_trades_30d UInt32 DEFAULT 0 COMMENT 'Total swap executions within 30-day window',
    win_rate Float32 DEFAULT 0.0 COMMENT 'Calculated win rate percentage based on token PnL realizations',
    profit_loss_ratio Float32 DEFAULT 0.0 COMMENT 'Total profit divided by total loss',
    sharpe_ratio Float32 DEFAULT 0.0 COMMENT 'Approximated Sharpe Ratio representing risk-adjusted return profile',
    max_drawdown Float32 DEFAULT 0.0 COMMENT 'Maximum historical equity peak-to-trough drop percentage',
    total_volume_usd Float32 DEFAULT 0.0 COMMENT 'Total accumulated swap volume in USD over 30 days',
    active_days_30d UInt8 DEFAULT 0 COMMENT 'Distinct days with at least one transaction',
    account_growth_30d Float32 DEFAULT 0.0 COMMENT 'Estimated account compound growth rate over 30 days',

    -- 2. Timing & Entry Quality (Intersecting address trades with historical token SFS)
    avg_entry_sfs Float32 DEFAULT 0.0 COMMENT 'Average Technical Score (SFS) of tokens at the exact time of buy entry',
    high_sfs_ratio Float32 DEFAULT 0.0 COMMENT 'Ratio of buy orders placed when token SFS was in prime zone (>= 70)',

    -- 3. Business Labels & Risk Intermediate Classifications
    style_tags Array(String) COMMENT 'System assigned behavioural tags, e.g., ["Whale", "Mid-Frequency", "High Win Rate"]',
    risk_level String COMMENT 'Aggregated traffic-light risk tier: GREEN, YELLOW, RED',
    risk_score UInt8 DEFAULT 0 COMMENT 'Risk scale score from 0 (Safe) to 10 (Dangerous)',
    risk_flags Array(String) COMMENT 'Array of triggered micro risk items for UI lists',
    composite_score UInt8 DEFAULT 0 COMMENT 'Final master copy-trade valuation score (0-100)',

    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (trader_address, calc_date);