-- 【银层 DWD】清洗后的标准化 Swap 事件表
CREATE TABLE IF NOT EXISTS signal_hub.clean_swaps (
    block_timestamp DateTime,
    block_number UInt64,
    tx_hash String,
    log_index UInt32,
    trader_address String,
    dex_name String,
    token_in_address String,
    token_in_symbol String,
    token_in_amount Float64,
    token_out_address String,
    token_out_symbol String,
    token_out_amount Float64,
    amount_usd Float64,
    source String DEFAULT 'rpc',
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (trader_address, block_timestamp, tx_hash, log_index);