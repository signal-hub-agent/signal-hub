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

        # 1. Repo: 取数据
        row = SignalRepository.get_token_gold_metrics(token_symbol)
        if not row:
            raise HTTPException(status_code=404, detail=f"No signal data found for token {token_symbol}")

        # 2. Calc: 构建组件
        components = SignalCalculation.build_components(row)

        # 3. LLM / Cache
        llm_report = None
        if force_refresh:
            await redis.delete(llm_cache_key)
        else:
            cached = await redis.get(llm_cache_key)
            if cached:
                llm_report = json.loads(cached)

        if not llm_report:
            logger.info(f"LLM Cache miss for token {token_symbol}, generating...")
            try:
                system_prompt = SignalCalculation.TOKEN_SYSTEM_PROMPT
                user_prompt = SignalCalculation.build_llm_user_prompt(row)
                llm_report = await request_llm_json(system_prompt, user_prompt, max_tokens=500)
                await redis.setex(llm_cache_key, 86400, json.dumps(llm_report))
            except Exception as e:
                logger.error(f"Failed to generate LLM report: {e}")
                llm_report = {"summary": "Technical analysis temporarily unavailable.", "bias": "NEUTRAL"}

        # 4. Assembling Response
        return TokenSignalResponse(
            token_symbol=token_symbol,
            current_price=row.get('current_price', 0.0),
            total_score=int(row.get('composite_score', 0)),
            signal_color=row.get('signal_color', 'yellow'),
            llm_report=json.dumps(llm_report), # 前端需要 stringified JSON
            components=components,
            generated_at=datetime.utcnow()
        )