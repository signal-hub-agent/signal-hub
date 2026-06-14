import asyncio
from core.redis_client import get_redis_client

async def clear_all_llm_caches():
    try:
        redis_client = await get_redis_client()

        # 定义需要清理的缓存 Key 的匹配模式
        # 包含了首页批量缓存以及独立页面的单体缓存
        patterns = [
            "dashboard:top_tokens_v1:*",     # 首页左翼 Token 批量缓存
            "dashboard:top_addresses_v1:*",  # 首页右翼 Address 批量缓存
            "signalhub:llm:token:*",         # Signal 详情页单个 Token 缓存
            "signalhub:llm:address:*"        # Detective 详情页单个 Address 缓存
        ]

        total_deleted = 0

        print("🧹 准备清理所有包含中文的旧版 LLM 缓存...")
        print("-" * 50)

        for pattern in patterns:
            # 查找匹配的所有 keys
            keys = await redis_client.keys(pattern)

            if keys:
                # 批量删除找到的 keys
                deleted_count = await redis_client.delete(*keys)
                total_deleted += deleted_count
                print(f"✅ 成功清除 {deleted_count} 个匹配 [{pattern}] 的缓存")
            else:
                print(f"⚪️ 未发现匹配 [{pattern}] 的缓存")

        print("-" * 50)
        print(f"✨ 清理完成！共删除了 {total_deleted} 条旧缓存。")
        print("💡 现在去刷新前端页面，将自动触发全新的纯英文 LLM 报告！")

    except Exception as e:
        print(f"❌ 清理失败，错误原因: {e}")

if __name__ == "__main__":
    asyncio.run(clear_all_llm_caches())