from clickhouse_driver import Client
import logging
import os
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_clickhouse_client() -> Client:
    # In a production environment, load these from environment variables
    return Client(
        host='localhost',
        port=9000,
        database='signal_hub',
        user='default',
        password=''
    )


# Configure logger if not already done in this file
logger = logging.getLogger(__name__)

# ==========================================
# PostgreSQL Configuration (For Job Monitor)
# ==========================================
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")  # Change to your actual PG password
PG_DATABASE = os.getenv("PG_DATABASE", "signal_hub")     # Change to your actual PG database name

def get_pg_client():
    """
    Creates and returns a new PostgreSQL database connection.
    The connection is used for updating job watermarks and system alerts.
    """
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE
        )
        return conn
    except psycopg2.Error as e:
        logger.error("Failed to connect to PostgreSQL: %s", str(e))
        raise