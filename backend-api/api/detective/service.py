import json
import logging
from datetime import datetime

from core.redis_client import get_redis_client
from llm_engine.llm_client import request_llm_json

from .repository import DetectiveRepository
from .calculation import DetectiveCalculation
from .probe import FallbackProbe
from .schemas import (
    AddressDetailResponse, Top100Response, FinancialMetrics, RiskAssessment
)

logger = logging.getLogger(__name__)

class DetectiveService:
    @staticmethod
    async def get_top_100_from_redis() -> Top100Response:
        redis = await get_redis_client()
        cache_key = "detective:top100"

        cached = await redis.get(cache_key)
        if cached:
            return Top100Response(**json.loads(cached))

        # Repo
        traders = DetectiveRepository.get_top_100_financials()
        response = Top100Response(updated_at=datetime.utcnow(), traders=traders)
        await redis.setex(cache_key, 3600, response.json())
        return response

    @staticmethod
    async def analyze_address(address: str, force_refresh: bool = False) -> AddressDetailResponse:
        addr = address.lower()
        redis = await get_redis_client()
        llm_cache_key = f"signalhub:llm:address:{addr}"

        # 1. Repo: 获取金表指标
        metrics_data = DetectiveRepository.get_address_gold_metrics(addr)

        # 2. 降级处理: Probe
        if not metrics_data:
            probe = FallbackProbe()
            probe_result = await probe.probe_address(addr)
            return AddressDetailResponse(**probe_result)

        # 3. LLM 与 Trade 流水处理 (Cache Check)
        llm_report = None
        trade_quality = {}
        recent_trades = []

        if force_refresh:
            await redis.delete(llm_cache_key)
        else:
            cached = await redis.get(llm_cache_key)
            if cached:
                cached_data = json.loads(cached)
                llm_report = cached_data.get("llm_analysis")
                trade_quality = cached_data.get("trade_quality", {})
                recent_trades = cached_data.get("recent_trades", [])

        if not llm_report:
            logger.info(f"LLM Cache miss for {addr}, generating pipeline...")
            # Repo: 拉取交易并查询相关的 Token SFS
            raw_trades = DetectiveRepository.get_recent_swaps(addr, limit=15)
            unique_tokens = list(set(t["token"] for t in raw_trades))
            sfs_dict = DetectiveRepository.get_tokens_sfs_metrics(unique_tokens)

            # Calc: 数据组装与计算
            recent_trades = DetectiveCalculation.enrich_trades_with_sfs(raw_trades, sfs_dict)
            trade_quality = DetectiveCalculation.calc_trade_quality_stats(recent_trades)

            # Calc -> LLM_Client
            user_prompt = DetectiveCalculation.build_llm_payload(addr, metrics_data, recent_trades, trade_quality)
            try:
                llm_report = await request_llm_json(DetectiveCalculation.ADDRESS_SYSTEM_PROMPT, user_prompt)
                # 存入 Cache
                cache_payload = {
                    "llm_analysis": llm_report,
                    "trade_quality": trade_quality,
                    "recent_trades": recent_trades[:5]
                }
                await redis.setex(llm_cache_key, 86400, json.dumps(cache_payload))
            except Exception as e:
                logger.error(f"LLM fail for {addr}: {e}")
                llm_report = {"profile_report": "Analysis temporarily unavailable.", "style_label": "Unknown"}

        # 4. 组装返回数据
        return AddressDetailResponse(
            address=addr,
            tags=metrics_data.get('style_tags', []),
            composite_score=metrics_data.get('composite_score', 0),
            is_dex_trader=True,
            metrics=FinancialMetrics(
                win_rate=metrics_data.get('win_rate', 0.0),
                profit_loss_ratio=metrics_data.get('profit_loss_ratio', 0.0),
                sharpe_ratio=metrics_data.get('sharpe_ratio', 0.0),
                max_drawdown=metrics_data.get('max_drawdown', 0.0),
                account_growth_30d=metrics_data.get('account_growth_30d', 0.0),
                total_trades_30d=metrics_data.get('total_trades_30d', 0),
                total_volume_usd=metrics_data.get('total_volume_usd', 0.0),
                active_days_30d=metrics_data.get('active_days_30d', 0)
            ),
            risk=RiskAssessment(
                risk_level=metrics_data.get('risk_level', 'UNKNOWN'),
                risk_score=metrics_data.get('risk_score', 0),
                flags=metrics_data.get('risk_flags', []),
            ),
            portfolio=[],
            llm_analysis={ # 兼容前端的嵌套层结构包装
                "llm_analysis": llm_report,
                "recent_trades": recent_trades,
                "trade_quality": trade_quality
            },
            data_source="gold_precomputed",
            last_active=datetime.utcnow()
        )