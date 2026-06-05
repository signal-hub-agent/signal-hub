-- [Bronze ODS] Raw RPC Logs
CREATE TABLE IF NOT EXISTS signal_hub.raw_logs (
    chain_name String DEFAULT 'mantle',
    block_number UInt64,
    block_timestamp Int64,
    tx_hash String,
    log_index UInt32,
    contract_address String,
    topic0 String,
    topic1 String,
    topic2 String,
    topic3 String,
    data String,
    ingestion_timestamp Int64
) ENGINE = ReplacingMergeTree(ingestion_timestamp)
-- Partition by Day to align with offline daily batch processing
PARTITION BY toYYYYMMDD(toDateTime(block_timestamp / 1000))
-- Strict on-chain uniqueness for accurate deduplication
ORDER BY (block_number, tx_hash, log_index);