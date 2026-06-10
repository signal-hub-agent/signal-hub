import logging
import clickhouse_connect
from core.config import settings

logger = logging.getLogger(__name__)

class ClickHouseManager:
    def __init__(self):
        self.client = None

    def connect(self):
        try:
            self.client = clickhouse_connect.get_client(
                host=settings.CH_HOST,
                port=settings.CH_PORT,
                username=settings.CH_USER,  # clickhouse_connect 参数名为 username
                password=settings.CH_PASSWORD,
                database=settings.CH_DATABASE
            )
            # Execute a lightweight query to test connection
            self.client.command('SELECT 1')
            logger.info("ClickHouse connection established successfully via HTTP.")
        except Exception as e:
            logger.error("Failed to connect to ClickHouse: %s", str(e))
            raise

    def disconnect(self):
        if self.client:
            # clickhouse_connect 底层使用 requests Session，释放引用即可
            self.client = None
            logger.info("ClickHouse connection released.")

    def get_client(self):
        if not self.client:
            self.connect()
        return self.client

# 全局单例管理器
ch_manager = ClickHouseManager()

# 暴露给各个 Service 直接使用的快捷入口，兼容旧代码习惯
def get_ch_client():
    return ch_manager.get_client()