-- Address Dimension Profile (Caches LLM insights)
CREATE TABLE IF NOT EXISTS signal_hub.dim_address_profile (
    trader_address String,
    tags Array(String),
    llm_summary String,
    trade_style String,
    risk_level String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY trader_address;