CREATE TABLE IF NOT EXISTS signal_hub.dim_token_info (
    token_address String COMMENT 'Token contract hex address',
    symbol String COMMENT 'Uppercase ticker symbol, e.g., MNT',
    name String COMMENT 'Full human-readable asset name',
    decimals UInt8 DEFAULT 18 COMMENT 'Token precision standard unit decimal place',
    is_contract_verified UInt8 DEFAULT 0 COMMENT 'Boolean flag indicating code verification status on explorer',
    contract_safety_score UInt8 DEFAULT 28 COMMENT 'Static smart contract audit score component (0-30)',
    risk_items Array(String) COMMENT 'Static risk components list, e.g., ["Liquidity Unlocked"]',
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY token_address;