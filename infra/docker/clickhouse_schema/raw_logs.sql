-- 【铜层 ODS】存储 RPC 实时解析的原始日志
CREATE TABLE IF NOT EXISTS signal_hub.raw_logs (
    chain_name String DEFAULT 'mantle',
    block_number UInt64,
    block_timestamp DateTime,
    tx_hash String,
    log_index UInt32,
    contract_address String,
    topic0 String,
    topic1 String,
    topic2 String,
    topic3 String,
    data String,
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (block_number, tx_hash, log_index);