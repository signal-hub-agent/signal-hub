import logging
import asyncpg
from core.config import settings

logger = logging.getLogger(__name__)

class PostgresManager:
    def __init__(self):
        self.pool = None

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(
                host=settings.PG_HOST,
                port=settings.PG_PORT,
                user=settings.PG_USER,
                password=settings.PG_PASSWORD,
                database=settings.PG_DATABASE,
                min_size=2,
                max_size=20
            )
            logger.info("PostgreSQL async connection pool created successfully.")
        except Exception as e:
            logger.error("Failed to create PostgreSQL pool: %s", str(e))
            raise

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL async connection pool closed.")

    def get_pool(self):
        if not self.pool:
            logger.warning("PostgreSQL pool accessed before initialization.")
        return self.pool

pg_manager = PostgresManager()