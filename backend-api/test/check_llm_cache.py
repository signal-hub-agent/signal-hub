import asyncio
import json
from core.redis_client import get_redis_client

async def inspect_llm_cache():
    try:
        redis_client = await get_redis_client()

        # 将你原先的4个旧代币，加上几个新代币作为对照组
        test_tokens = ["ETH", "BTC", "SOL", "MNT", "USDC", "CATI", "COOK", "FBTC", "PUFF", "USDE", "WBTC", "WETH", "METH", "WMNT"]

        print("🔍 正在诊断 Redis 中的 LLM 报告缓存...\n" + "-"*50)

        for token in test_tokens:
            cache_key = f"signalhub:llm:token:{token}"
            val = await redis_client.get(cache_key)

            if not val:
                print(f"⚪️ {token:<6} | 无缓存 (下次请求将触发 LLM 生成)")
                continue

            print(f"🟢 {token:<6} | 发现缓存数据")

            # 尝试解析 JSON，这就是前端报错的“案发现场”
            try:
                parsed = json.loads(val)
                keys_str = ", ".join(parsed.keys())
                print(f"   └─ JSON 解析: ✅ 成功! 包含字段: [{keys_str}]")
            except json.JSONDecodeError:
                print(f"   └─ JSON 解析: ❌ 失败! (这正是导致前端渲染为空的元凶)")
                print(f"   └─ 脏数据片段: {val[:80]}...")

        print("-" * 50)

    except Exception as e:
        print(f"❌ 脚本执行报错: {e}")

if __name__ == "__main__":
    asyncio.run(inspect_llm_cache())