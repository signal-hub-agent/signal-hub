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