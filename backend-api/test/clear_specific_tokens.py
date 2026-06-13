import asyncio
from core.redis_client import get_redis_client

async def clear_specific_old_tokens():
    try:
        redis_client = await get_redis_client()

        # 严格锁定需要被“洗牌”的旧代币
        old_tokens = ["ETH", "BTC", "SOL", "MNT"]

        print("🧹 准备定向清理旧版代币的 LLM 缓存...")
        print("-" * 50)

        deleted_total = 0
        for token in old_tokens:
            cache_key = f"signalhub:llm:token:{token}"

            # 执行删除操作 (delete 返回成功删除的数量)
            deleted = await redis_client.delete(cache_key)
            if deleted:
                print(f"✅ 成功粉碎 {token:<4} 的残缺缓存")
                deleted_total += 1
            else:
                print(f"⚪️ 未找到 {token:<4} 的缓存 (可能已过期)")

        print("-" * 50)
        print(f"✨ 靶向清理完毕！共清除了 {deleted_total} 个旧版本数据。")
        print("💡 请前往浏览器刷新 Signal 页面，这四个代币将重新请求 DeepSeek 并生成全字段报告。")

    except Exception as e:
        print(f"❌ 清理脚本执行失败: {e}")

if __name__ == "__main__":
    asyncio.run(clear_specific_old_tokens())