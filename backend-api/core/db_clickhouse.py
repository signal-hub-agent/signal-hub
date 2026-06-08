import logging
from clickhouse_driver import Client
from core.config import settings

logger = logging.getLogger(__name__)

class ClickHouseManager:
    def __init__(self):
        self.client = None

    def connect(self):
        try:
            self.client = Client(
                host=settings.CH_HOST,
                port=settings.CH_PORT,
                user=settings.CH_USER,
                password=settings.CH_PASSWORD,
                database=settings.CH_DATABASE
            )
            # Execute a lightweight query to test connection
            self.client.execute('SELECT 1')
            logger.info("ClickHouse connection established successfully.")
        except Exception as e:
            logger.error("Failed to connect to ClickHouse: %s", str(e))
            raise

    def disconnect(self):
        if self.client:
            self.client.disconnect()
            logger.info("ClickHouse connection closed.")

    def get_client(self) -> Client:
        if not self.client:
            self.connect()
        return self.client

ch_manager = ClickHouseManager()