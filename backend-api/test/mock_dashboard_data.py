import asyncio
import random
from datetime import datetime

# 如果你无法导入 get_redis_client，请取消下面原生 redis 的注释
from core.redis_client import get_redis_client
# import redis.asyncio as redis

async def inject_new_mock_data():
    try:
        # redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client = await get_redis_client()

        today_str = datetime.utcnow().strftime('%Y%m%d')
        print(f"🚀 准备为【全新架构大屏】注入模拟数据 (日期: {today_str})...")

        # ==========================================
        # 1. 制造逼真的假数据 (Count + Sum 双指标)
        # ==========================================
        # 聪明钱交易: 笔数多，金额中等
        smart_swaps_count = random.randint(150, 450)
        smart_swaps_sum = random.uniform(200000.0, 800000.0)

        # 巨鲸异动: 笔数少，金额巨大
        whale_moves_count = random.randint(10, 50)
        whale_moves_sum = random.uniform(1500000.0, 8500000.0)

        # 流动性异动: 笔数极少，金额极大
        liquidity_count = random.randint(5, 20)
        liquidity_sum = random.uniform(500000.0, 3000000.0)

        # 跨链桥: 笔数中等，金额中等
        bridges_count = random.randint(30, 100)
        bridges_sum = random.uniform(100000.0, 1500000.0)

        # 零日告警: 只有笔数 (新池子)
        zero_day_count = random.randint(1, 12)

        # ==========================================
        # 2. 写入 Redis (严格匹配新 Key)
        # ==========================================
        await redis_client.set(f"kpi:smart_swaps:count:{today_str}", smart_swaps_count)
        await redis_client.set(f"kpi:smart_swaps:sum:{today_str}", smart_swaps_sum)

        await redis_client.set(f"kpi:whale_moves:count:{today_str}", whale_moves_count)
        await redis_client.set(f"kpi:whale_moves:sum:{today_str}", whale_moves_sum)

        await redis_client.set(f"kpi:liquidity:count:{today_str}", liquidity_count)
        await redis_client.set(f"kpi:liquidity:sum:{today_str}", liquidity_sum)

        await redis_client.set(f"kpi:bridges:count:{today_str}", bridges_count)
        await redis_client.set(f"kpi:bridges:sum:{today_str}", bridges_sum)

        await redis_client.set(f"kpi:zero_day:count:{today_str}", zero_day_count)

        # ==========================================
        # 3. 华丽的终端输出
        # ==========================================
        print("-" * 45)
        print("✅ 注入成功！当前指标：")
        print(f"⚡ Smart Swaps    : ${smart_swaps_sum:,.2f} ({smart_swaps_count} Txns)")
        print(f"🐋 Whale Moves    : ${whale_moves_sum:,.2f} ({whale_moves_count} Txns)")
        print(f"💧 Liquidity Flow : ${liquidity_sum:,.2f} ({liquidity_count} Events)")
        print(f"🌉 Bridge Transfer: ${bridges_sum:,.2f} ({bridges_count} Txns)")
        print(f"🛡️ Zero-Day Alerts: {zero_day_count} Unknown Pools")
        print("-" * 45)
        print("💡 请立即去浏览器查看前端大屏，主副体数字应该已经跳动！")

    except Exception as e:
        print(f"❌ 注入失败: {e}")

if __name__ == "__main__":
    asyncio.run(inject_new_mock_data())