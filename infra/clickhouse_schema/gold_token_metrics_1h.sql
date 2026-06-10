CREATE TABLE IF NOT EXISTS signal_hub.gold_token_metrics_1h (
    token_address String COMMENT 'Token contract address',
    token_symbol String COMMENT 'Token ticker symbol',
    calc_time DateTime COMMENT 'Execution timestamp of the offline job',

    -- 1. On-Chain Volume & MEV Metrics (From Silver clean_swaps)
    volume_24h_usd Float32 DEFAULT 0.0 COMMENT '24-hour trading volume in USD',
    volume_7d_avg_usd Float32 DEFAULT 0.0 COMMENT '7-day rolling average daily volume',
    volume_ratio Float32 DEFAULT 1.0 COMMENT 'Volume ratio: 24h_volume / 7d_avg',
    tx_count_24h UInt32 DEFAULT 0 COMMENT 'Total count of swap transactions in 24h',
    mev_total_txs UInt32 DEFAULT 0 COMMENT 'Total sampled transactions for MEV detection',
    mev_suspicious_txs UInt32 DEFAULT 0 COMMENT 'Identified sandwich or bot transactions',
    mev_toxicity_pct Float32 DEFAULT 0.0 COMMENT 'MEV toxicity ratio percentage',

    -- 2. Technical Indicator States (From offline technical analysis calculations)
    current_price Float32 DEFAULT 0.0 COMMENT 'Latest token price derived from exchange feeds',
    ma_trend_type String COMMENT 'EMA structure: bullish_trending, bearish_trending, range_bound, compressing',
    ma_alignment String COMMENT 'Full sorted alignment string, e.g., EMA7 > EMA25 > EMA99',
    macd_position String COMMENT 'MACD status: zero_above_golden_cross, zero_below_bearish, etc.',
    macd_histogram_trend String COMMENT 'Histogram expansion state: red_expanding, green_shrinking',
    macd_divergence String COMMENT 'Divergence signal: bullish_divergence, bearish_divergence, or None',
    rsi_value Float32 DEFAULT 50.0 COMMENT 'RSI 14 absolute value',
    rsi_zone String COMMENT 'RSI categorization: overbought, oversold, bullish, bearish, neutral',
    bollinger_pattern String COMMENT 'BB band pattern: bullish_channel, bearish_channel, potential_top/bottom',
    bollinger_support Float32 DEFAULT 0.0 COMMENT 'Dynamic support level calculated from lower/mid band',
    bollinger_resistance Float32 DEFAULT 0.0 COMMENT 'Dynamic resistance level calculated from upper/mid band',
    kdj_zone String COMMENT 'KDJ location state: overbought, oversold, normal',

    -- 3. Composite Scoring System (Consolidated scoring results)
    tech_score UInt8 DEFAULT 0 COMMENT 'Technical analysis score component (0-30)',
    volume_score UInt8 DEFAULT 0 COMMENT 'Volume trend score component (0-15)',
    mev_score UInt8 DEFAULT 0 COMMENT 'MEV wind-control score component (0-25)',
    contract_score UInt8 DEFAULT 28 COMMENT 'Smart contract risk score component (0-30)',
    composite_score UInt8 DEFAULT 0 COMMENT 'Unified aggregate signal score (0-100)',
    signal_color String COMMENT 'Actionable system signal flag: green, yellow, red',

    updated_at DateTime DEFAULT now() COMMENT 'Row modification time for ReplacingMergeTree'
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (token_address, calc_time);