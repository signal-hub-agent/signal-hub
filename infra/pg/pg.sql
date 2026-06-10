-- 1. Job Watermark and Status Monitoring Table
CREATE TABLE IF NOT EXISTS sys_job_watermarks (
    job_name VARCHAR(100) PRIMARY KEY,
    cron_expression VARCHAR(50),
    watermark TIMESTAMP,
    last_execution_time TIMESTAMP,
    next_execution_time TIMESTAMP,
    status VARCHAR(20),
    error_msg TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. System Alerts Table for monitoring triggers
CREATE TABLE IF NOT EXISTS sys_alerts (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50),
    alert_message TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User core table (Wallet based)
CREATE TABLE IF NOT EXISTS users (
    wallet_address VARCHAR(42) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'ACTIVE'
);

-- Subscription relationship table
CREATE TABLE IF NOT EXISTS user_subscriptions (
    id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(42) REFERENCES users(wallet_address) ON DELETE CASCADE,
    target_id VARCHAR(100) NOT NULL,
    target_type VARCHAR(20) NOT NULL, -- 'TOKEN' or 'ADDRESS'
    subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_address, target_id, target_type)
);

-- Real-time alerts table for Zero-Day Radar
CREATE TABLE IF NOT EXISTS zero_day_alerts (
    id VARCHAR(64) PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL, -- e.g., 'NEW_POOL', 'WHALE_ACCUMULATION'
    target_address VARCHAR(42) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- 'HIGH', 'MEDIUM', 'LOW'
    tags TEXT[],
    payload JSONB NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_zero_day_detected_at ON zero_day_alerts(detected_at DESC);