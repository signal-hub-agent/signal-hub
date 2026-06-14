import json
import logging
from datetime import datetime
from fastapi import HTTPException
from core.redis_client import get_redis_client
from llm_engine.llm_client import request_llm_json

from .repository import SignalRepository
from .calculation import SignalCalculation
from .schemas import TokenSignalResponse

logger = logging.getLogger(__name__)

class SignalService:
    @staticmethod
    def get_available_tokens() -> list:
        tokens = SignalRepository.get_tokens_from_gold()
        if not tokens:
            tokens = SignalRepository.get_tokens_from_dune()
        return tokens

    @staticmethod
    async def analyze_token(token: str, force_refresh: bool = False) -> TokenSignalResponse:
        redis = await get_redis_client()
        token_symbol = token.upper()
        llm_cache_key = f"signalhub:llm:token:{token_symbol}"

        row = SignalRepository.get_token_gold_metrics(token_symbol)
        if not row:
            raise HTTPException(status_code=404, detail=f"No signal data found for token {token_symbol}")

        components = SignalCalculation.build_components(row)

        llm_report = None
        if force_refresh:
            logger.info(f"Force refresh for {token_symbol}, generating new AI Brief...")
            try:
                system_prompt = SignalCalculation.TOKEN_SYSTEM_PROMPT
                user_prompt = SignalCalculation.build_llm_user_prompt(row)

                # 请求大模型，获取 JSON 字典
                llm_report_dict = await request_llm_json(system_prompt, user_prompt, max_tokens=500)

                # 注入时间戳
                llm_report_dict["generated_at"] = datetime.utcnow().isoformat()

                # 序列化后存入 Redis
                llm_report = json.dumps(llm_report_dict)
                await redis.setex(llm_cache_key, 86400, llm_report)
            except Exception as e:
                logger.error(f"Failed to generate LLM report: {e}")
                # 兜底保持为 None
        else:
            # 读缓存，读不到就是 None，绝不自动调用 LLM
            llm_report = await redis.get(llm_cache_key)

        return TokenSignalResponse(
            token_symbol=token_symbol,
            current_price=row.get('current_price', 0.0),
            total_score=int(row.get('composite_score', 0)),
            signal_color=row.get('signal_color', 'yellow'),
            llm_report=llm_report, # 现在这里可能是 None 了
            components=components,
            generated_at=datetime.utcnow()
        )