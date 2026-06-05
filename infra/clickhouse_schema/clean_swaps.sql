-- [Silver DWD] Cleaned Swap Events (CRITICAL FIX: Added MEV & PnL fields, aligned Int64 timestamp)
CREATE TABLE IF NOT EXISTS signal_hub.clean_swaps (
    block_timestamp Int64,
    block_number UInt64,
    tx_hash String,
    log_index UInt32,
    trader_address String,
    pool_address String,     -- 🌟 Added for MEV Sandwich Detection
    router_address String,   -- 🌟 Added for Traceability
    dex_name String,
    token_in_address String,
    token_in_symbol String,
    token_in_amount Float64,
    token_out_address String,
    token_out_symbol String,
    token_out_amount Float64,
    amount_usd Float64,
    tx_fee_mnt Float64,      -- 🌟 Added for exact Profit/Loss calculation
    source String DEFAULT 'rpc',
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
-- Partition by Day enables the 'DROP PARTITION' idempotency logic in your Python script
PARTITION BY toYYYYMMDD(toDateTime(block_timestamp / 1000))
ORDER BY (block_number, tx_hash, log_index);