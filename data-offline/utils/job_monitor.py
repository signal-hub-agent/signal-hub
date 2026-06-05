import logging
from datetime import datetime
from config.database import get_pg_client

logger = logging.getLogger(__name__)

def update_job_start(job_name: str, cron_expr: str, watermark: datetime, next_exec_time: datetime):
    """
    Marks a job as RUNNING and updates the execution physical time.
    """
    query = """
        INSERT INTO sys_job_watermarks 
            (job_name, cron_expression, watermark, last_execution_time, next_execution_time, status, updated_at)
        VALUES 
            (%s, %s, %s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP)
        ON CONFLICT (job_name) DO UPDATE SET
            cron_expression = EXCLUDED.cron_expression,
            watermark = EXCLUDED.watermark,
            last_execution_time = EXCLUDED.last_execution_time,
            next_execution_time = EXCLUDED.next_execution_time,
            status = 'RUNNING',
            updated_at = CURRENT_TIMESTAMP;
    """
    try:
        client = get_pg_client()
        with client.cursor() as cursor:
            cursor.execute(query, (job_name, cron_expr, watermark, datetime.utcnow(), next_exec_time))
        client.commit()
        logger.info("Job %s marked as RUNNING in database.", job_name)
    except Exception as e:
        logger.error("Failed to update job start status for %s: %s", job_name, str(e))

def update_job_end(job_name: str, status: str, error_msg: str = ""):
    """
    Marks a job as SUCCESS or FAILED.
    """
    query = """
        UPDATE sys_job_watermarks 
        SET status = %s, error_msg = %s, updated_at = CURRENT_TIMESTAMP
        WHERE job_name = %s;
    """
    try:
        client = get_pg_client()
        with client.cursor() as cursor:
            cursor.execute(query, (status, error_msg, job_name))
        client.commit()
        logger.info("Job %s marked as %s.", job_name, status)
    except Exception as e:
        logger.error("Failed to update job end status for %s: %s", job_name, str(e))

def record_system_alert(alert_type: str, message: str):
    """
    Records a critical system alert for the monitoring dashboard.
    """
    query = "INSERT INTO sys_alerts (alert_type, alert_message) VALUES (%s, %s);"
    try:
        client = get_pg_client()
        with client.cursor() as cursor:
            cursor.execute(query, (alert_type, message))
        client.commit()
        logger.error("SYSTEM ALERT [%s]: %s", alert_type, message)
    except Exception as e:
        logger.error("Failed to record system alert: %s", str(e))