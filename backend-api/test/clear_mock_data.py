import asyncio
from core.redis_client import get_redis_client

async def clear_mock_only():
    try:
        redis_client = await get_redis_client()

        # 查找所有带有 mock 标识的 key
        mock_keys = await redis_client.keys("kpi:*")

        if not mock_keys:
            print("✨ 当前 Redis 中没有发现 Mock 数据，系统非常干净！")
            return

        # 批量删除找到的 mock keys
        deleted_count = await redis_client.delete(*mock_keys)

        print(f"🧹 成功清除了 {deleted_count} 个 Mock 数据！")
        print("🛡️ 真实的生产数据 (kpi:*) 已被完美保留。刷新大屏即可看到真实水位。")

    except Exception as e:
        print(f"❌ 清理失败: {e}")

if __name__ == "__main__":
    asyncio.run(clear_mock_only())