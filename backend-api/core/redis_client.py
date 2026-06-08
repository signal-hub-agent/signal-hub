import logging
import redis.asyncio as redis
from core.config import settings

logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self):
        self.client = None

    async def connect(self):
        try:
            self.client = redis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
            # Ping to verify connection
            await self.client.ping()
            logger.info("Redis async connection established successfully.")
        except Exception as e:
            logger.error("Failed to connect to Redis: %s", str(e))
            raise

    async def disconnect(self):
        if self.client:
            await self.client.close()
            logger.info("Redis connection closed.")

    def get_client(self) -> redis.Redis:
        if not self.client:
            logger.warning("Redis client accessed before initialization.")
        return self.client

redis_manager = RedisManager()