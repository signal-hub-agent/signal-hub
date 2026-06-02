-- 库初始化 (可选)
CREATE DATABASE IF NOT EXISTS signal_forge;
USE signal_forge;

-- ==========================================
-- ODS Layer: 存储 RPC 实时解析的原始日志
-- ==========================================
CREATE TABLE IF NOT EXISTS raw_logs (
    chain_name String DEFAULT 'mantle',
    block_number UInt64,
    block_timestamp DateTime,  -- 统一命名规范
    tx_hash String,
    log_index UInt32,          -- 🌟 核心字段：同一笔交易可能产生多个 swap log
    contract_address String,   -- 触发 log 的合约地址 (通常是 LP Pool 地址)
    topic0 String,
    topic1 String,
    topic2 String,
    topic3 String,
    data String,
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
-- 🌟 ReplacingMergeTree 依赖 ORDER BY 去重。
-- 同一个 block, 同一个 tx_hash 下的同一个 log_index，只会保留最新的一条。
ORDER BY (block_number, tx_hash, log_index);


-- ==========================================
-- DWD Layer: 清洗后的标准化 Swap 事件表
-- 融合了 Dune 的冷数据和 RPC 的热数据
-- ==========================================
CREATE TABLE IF NOT EXISTS clean_swaps (
    block_timestamp DateTime,
    block_number UInt64,
    tx_hash String,
    log_index UInt32,
    trader_address String,     -- 交易者地址 (Address Detective 的主角)
    dex_name String,           -- 枚举: agni, fusionx, merchant_moe 等

    -- 🌟 补充的 SignalForge 核心字段：精准计算盈亏和筹码分布必备
    token_in_address String,   -- 卖出的代币合约
    token_in_symbol String,
    token_in_amount Float64,   -- 卖出的具体数量

    token_out_address String,  -- 买入的代币合约
    token_out_symbol String,
    token_out_amount Float64,  -- 买入的具体数量

    amount_usd Float64,        -- 美元价值 (用于过滤灰尘交易)
    source String DEFAULT 'rpc', -- 标识数据来源: 'dune' 或 'rpc'
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
-- 按照 trader_address 排序，极大加速 Address Detective 的查询速度
ORDER BY (trader_address, block_timestamp, tx_hash, log_index);