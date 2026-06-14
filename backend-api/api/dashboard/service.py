import json
import logging
from datetime import datetime
from core.redis_client import get_redis_client
from llm_engine.llm_client import request_llm_json

from .repository import DashboardRepository
from .calculation import DashboardCalculation
from .schemas import TokenRadarItem, SmartMoneyItem

logger = logging.getLogger(__name__)

class DashboardService:
    @staticmethod
    async def get_top_tokens_with_ai() -> list[TokenRadarItem]:
        redis = await get_redis_client()
        # 每天一个独立的 Cache Key，自然实现按天刷新
        today_str = datetime.utcnow().strftime('%Y%m%d')
        cache_key = f"dashboard:top_tokens_v1:{today_str}"

        # 1. 尝试读缓存 (命中则直接返回，0 LLM 消耗)
        cached = await redis.get(cache_key)
        if cached:
            try:
                data = json.loads(cached)
                return [TokenRadarItem(**item) for item in data]
            except Exception as e:
                logger.error(f"Failed to parse cached tokens: {e}")

        # 2. 缓存未命中：查库
        tokens_data = DashboardRepository.get_top_10_tokens()
        if not tokens_data:
            return []

        # 3. 批量调用大模型 (1 次请求解决 10 个代币)
        ai_insights = {}
        try:
            sys_p, usr_p = DashboardCalculation.build_tokens_batch_prompt(tokens_data)
            logger.info("Batch triggering LLM for Top 10 Tokens...")
            ai_insights = await request_llm_json(sys_p, usr_p, max_tokens=800)
        except Exception as e:
            logger.error(f"Batch LLM tokens failed: {e}")

        # 4. 拼装结果并写入缓存 (TTL: 24小时)
        result_list = []
        for t in tokens_data:
            symbol = t["symbol"]
            insight = ai_insights.get(symbol, "AI scanning: Active recent trading, recommend monitoring fund flows.")
            result_list.append(TokenRadarItem(
                symbol=symbol,
                name=f"{symbol} Token",
                volume_1h_usd=t["volume_1h_usd"],
                volume_change_pct=0.0, # 如果有真实字段可替换
                mev_toxicity_pct=t["mev_toxicity_pct"],
                ai_score=t["ai_score"],
                ai_insight=insight
            ))

        await redis.setex(cache_key, 86400, json.dumps([r.model_dump() for r in result_list]))
        return result_list

    @staticmethod
    async def get_top_smart_money_with_ai() -> list[SmartMoneyItem]:
        redis = await get_redis_client()
        today_str = datetime.utcnow().strftime('%Y%m%d')
        cache_key = f"dashboard:top_addresses_v1:{today_str}"

        # 1. 尝试读缓存
        cached = await redis.get(cache_key)
        if cached:
            try:
                data = json.loads(cached)
                return [SmartMoneyItem(**item) for item in data]
            except Exception as e:
                logger.error(f"Failed to parse cached addresses: {e}")

        # 2. 查库
        addresses_data = DashboardRepository.get_top_10_smart_money()
        if not addresses_data:
            return []

        # 3. 批量调用大模型
        ai_profiles = {}
        try:
            sys_p, usr_p = DashboardCalculation.build_addresses_batch_prompt(addresses_data)
            logger.info("Batch triggering LLM for Top 10 Addresses...")
            ai_profiles = await request_llm_json(sys_p, usr_p, max_tokens=800)
        except Exception as e:
            logger.error(f"Batch LLM addresses failed: {e}")

        # 4. 拼装结果并写入缓存
        result_list = []
        for a in addresses_data:
            addr = a["address"]
            profile = ai_profiles.get(addr, "AI profiling: Stable recent win rate, observing accumulation cycles.")
            result_list.append(SmartMoneyItem(
                address=addr,
                win_rate=a["win_rate"],
                pnl_ratio=a["pnl_ratio"],
                tags=a["tags"],
                ai_profiling=profile,
                score=a.get("score", 0)
            ))

        await redis.setex(cache_key, 86400, json.dumps([r.model_dump() for r in result_list]))
        return result_list