-- Token Dimension Profile
CREATE TABLE IF NOT EXISTS signal_hub.dim_token_profile (
    token_address String,
    symbol String,
    name String,
    deployed_at DateTime,
    tags Array(String),
    is_verified UInt8 DEFAULT 0,
    sfs_score Float32 DEFAULT 0.0,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY token_address;