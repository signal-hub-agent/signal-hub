-- ==============================================================================
-- 1. Database 配置
-- 注意：如果是通过 Docker 环境变量 (POSTGRES_DB) 启动，数据库已经被自动创建。
-- 如果是手动在客户端执行，请先确保连接到了正确的数据库。
-- \c signal_hub;
-- ==============================================================================

-- ==============================================================================
-- 2. Schema 配置
-- 创建一个与业务同名的独立 Schema，避免数据堆积在默认的 public 中
-- ==============================================================================
CREATE SCHEMA IF NOT EXISTS signal_hub;

-- 将当前会话的默认搜索路径设置为新创建的 Schema
SET search_path TO signal_hub, public;

-- ==============================================================================
-- 3. 核心业务表创建 (以下所有表都会自动建在 signal_hub 下)
-- ==============================================================================

-- 3.1 任务水位线与状态监控表 (Job Watermark and Status)
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

-- 3.2 系统告警表 (System Alerts)
CREATE TABLE IF NOT EXISTS sys_alerts (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50),
    alert_message TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3.3 用户核心表 (Users - Wallet based)
CREATE TABLE IF NOT EXISTS users (
    wallet_address VARCHAR(42) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'ACTIVE'
);

-- 3.4 订阅关系表 (User Subscriptions)
CREATE TABLE IF NOT EXISTS user_subscriptions (
    id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(42) REFERENCES users(wallet_address) ON DELETE CASCADE,
    target_id VARCHAR(100) NOT NULL,
    target_type VARCHAR(20) NOT NULL, -- 'TOKEN' or 'ADDRESS'
    subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_address, target_id, target_type)
);

-- 3.5 零日雷达实时告警表 (Zero-Day Alerts)
CREATE TABLE IF NOT EXISTS zero_day_alerts (
    id VARCHAR(64) PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL, -- e.g., 'NEW_POOL', 'WHALE_ACCUMULATION'
    target_address VARCHAR(42) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- 'HIGH', 'MEDIUM', 'LOW'
    tags TEXT[],
    payload JSONB NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
-- 为告警时间创建索引，加快按时间倒序查询速度
CREATE INDEX IF NOT EXISTS idx_zero_day_detected_at ON zero_day_alerts(detected_at DESC);

-- ==============================================================================
-- 4. 实时同步组件状态表 (新增：解决断点续传失忆问题)
-- ==============================================================================

-- 4.1 区块链抓取进度状态表 (Chain Sync State)
CREATE TABLE IF NOT EXISTS chain_sync_state (
    chain_name VARCHAR(50) PRIMARY KEY,
    last_processed_block BIGINT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE signal_hub.users ADD COLUMN IF NOT EXISTS email VARCHAR(255);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON signal_hub.users(email);


ALTER TABLE signal_hub.user_subscriptions ADD COLUMN IF NOT EXISTS email VARCHAR(255);
ALTER TABLE signal_hub.user_subscriptions DROP CONSTRAINT IF EXISTS user_subscriptions_wallet_address_target_id_target_type_key;
ALTER TABLE signal_hub.user_subscriptions ADD CONSTRAINT uniq_email_target_type UNIQUE (email, target_id, target_type);

ALTER TABLE signal_hub.user_subscriptions ADD COLUMN IF NOT EXISTS name VARCHAR(100) DEFAULT 'My Address Alert';
ALTER TABLE signal_hub.user_subscriptions ADD COLUMN IF NOT EXISTS config JSONB DEFAULT '{}'::jsonb;

ALTER TABLE signal_hub.users ADD COLUMN IF NOT EXISTS telegram_chat_id VARCHAR(50);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_telegram_chat_id ON signal_hub.users(telegram_chat_id);
