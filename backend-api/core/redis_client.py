import logging
import redis.asyncio as redis
from core.config import settings # 假设你有配置文件，如果没有可以直接把 url 写死

logger = logging.getLogger(__name__)

# 全局 Redis 连接池对象
_redis_client = None

async def init_redis():
    """初始化 Redis 连接池"""
    global _redis_client
    try:
        # 这里替换成你真实的 Redis 地址，如果没设密码就用 "redis://localhost:6379"
        redis_url = getattr(settings, "REDIS_URL", "redis://localhost:6379")
        _redis_client = redis.from_url(redis_url, decode_responses=True)
        # 测试一下连接是否畅通
        await _redis_client.ping()
        logger.info("✅ Redis client successfully initialized.")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Redis: {e}")

async def close_redis():
    """关闭 Redis 连接池"""
    global _redis_client
    if _redis_client:
        await _redis_client.aclose() # 注意：高版本 redis.asyncio 使用 aclose()
        logger.info("Redis client closed.")

async def get_redis_client() -> redis.Redis:
    """获取 Redis 客户端，带有防呆机制"""
    global _redis_client
    if _redis_client is None:
        logger.warning("Redis client accessed before initialization. Initializing now...")
        await init_redis()

    return _redis_client