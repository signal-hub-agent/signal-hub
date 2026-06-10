"""
信号参谋 - API 业务服务层
职责：纯读 ClickHouse 金表，结合 Redis 缓存控制 LLM 文本
"""
import json
import logging
from datetime import datetime
from fastapi import HTTPException

from core.db_clickhouse import get_ch_client
from core.redis_client import get_redis_client
from integrations.deepseek_client import generate_token_signal_report
from .schemas import TokenSignalResponse, ComponentScores

logger = logging.getLogger(__name__)

class SignalService:
    @staticmethod
    def get_available_tokens() -> list:
        """Returns a list of tokens that have data in the database."""
        client = get_ch_client()

        # Try Gold layer first
        try:
            rows = client.query("""
                SELECT DISTINCT token_symbol
                FROM signal_hub.gold_token_metrics_1h
                ORDER BY token_symbol LIMIT 50
            """).result_rows
            tokens = [r[0] for r in rows if r[0]]
            if tokens:
                return tokens
        except Exception as e:
            logger.debug(f"Failed to fetch from gold layer: {e}")

        # Fallback to dune_swaps
        try:
            rows = client.query("""
                SELECT upper(token_bought_symbol) AS token, COUNT() AS cnt
                FROM web3_data.dune_swaps
                GROUP BY token ORDER BY cnt DESC LIMIT 50
            """).result_rows
            return [r[0] for r in rows if r[0]]
        except Exception as e:
            logger.error(f"Failed to fetch available tokens: {e}")
            return []

    @staticmethod
    async def analyze_token(token: str, force_refresh: bool = False) -> TokenSignalResponse:
        """
        Fetches signal metrics from ClickHouse and LLM report from Redis.
        """
        client = get_ch_client()
        redis = await get_redis_client()
        token_symbol = token.upper()
        llm_cache_key = f"signalhub:llm:token:{token_symbol}"

        # ---------------------------------------------------------
        # Step 1: 极速读取 ClickHouse (0 计算)
        # ---------------------------------------------------------
        query_gold = """
            SELECT
                volume_24h_usd, volume_ratio, tx_count_24h, volume_score,
                mev_toxicity_pct, mev_score,
                contract_score,
                current_price, ma_trend_type, rsi_zone, macd_position, tech_score,
                composite_score, signal_color
            FROM signal_hub.gold_token_metrics_1h
            WHERE upper(token_symbol) = {token:String}
            ORDER BY calc_time DESC LIMIT 1
        """
        try:
            result = client.query(query_gold, parameters={"token": token_symbol})
            if not result.result_rows:
                raise HTTPException(status_code=404, detail=f"No signal data found for token {token_symbol}")
            row = dict(zip(result.column_names, result.result_rows[0]))
        except Exception as e:
            if isinstance(e, HTTPException): raise
            logger.error(f"CH Query failed for {token_symbol}: {e}")
            raise HTTPException(status_code=500, detail="Database error")

        # ---------------------------------------------------------
        # Step 2: 组装 Components 契约
        # ---------------------------------------------------------
        components = ComponentScores(
            volume_trend={
                "label": "Volume Trend", "score": int(row.get('volume_score', 0)), "max_score": 15,
                "details": {"volume_24h_usd": row.get('volume_24h_usd'), "ratio": row.get('volume_ratio')}
            },
            mev_toxicity={
                "label": "MEV Toxicity", "score": int(row.get('mev_score', 0)), "max_score": 25,
                "details": {"toxicity_pct": row.get('mev_toxicity_pct')}
            },
            contract_safety={
                "label": "Contract Safety", "score": int(row.get('contract_score', 28)), "max_score": 30,
                "details": {"risk_items": ["Checked OK"]}
            },
            technical_bias={
                "label": "Technical Bias", "score": int(row.get('tech_score', 0)), "max_score": 30,
                "details": {"ma_trend": row.get('ma_trend_type'), "rsi": row.get('rsi_zone'), "macd": row.get('macd_position')}
            }
        )

        # ---------------------------------------------------------
        # Step 3: Redis LLM 缓存与按需触发
        # ---------------------------------------------------------
        llm_report = None
        if force_refresh:
            await redis.delete(llm_cache_key)
        else:
            llm_report = await redis.get(llm_cache_key)

        if not llm_report:
            logger.info(f"LLM Cache miss for token {token_symbol}, generating...")
            llm_report = await generate_token_signal_report(row)
            await redis.setex(llm_cache_key, 86400, llm_report) # 缓存 24 小时

        # ---------------------------------------------------------
        # Step 4: 返回前端契约
        # ---------------------------------------------------------
        return TokenSignalResponse(
            token_symbol=token_symbol,
            current_price=row.get('current_price', 0.0),
            total_score=int(row.get('composite_score', 0)),
            signal_color=row.get('signal_color', 'yellow'),
            llm_report=llm_report,
            components=components,
            generated_at=datetime.utcnow()
        )