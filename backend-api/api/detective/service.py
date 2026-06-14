import json
import logging
from datetime import datetime

from core.redis_client import get_redis_client
from llm_engine.llm_client import request_llm_json

from .repository import DetectiveRepository
from .calculation import DetectiveCalculation
from .probe import FallbackProbe
from .schemas import (
    AddressDetailResponse, Top100Response, FinancialMetrics, RiskAssessment,
    TopologyGraphResponse, TopologyNode, TopologyEdge
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

        traders = DetectiveRepository.get_top_100_financials()
        response = Top100Response(updated_at=datetime.utcnow(), traders=traders)
        await redis.setex(cache_key, 3600, response.json())
        return response

    @staticmethod
    async def analyze_address(address: str, force_refresh: bool = False) -> AddressDetailResponse:
        addr = address.lower()
        redis = await get_redis_client()
        llm_cache_key = f"signalhub:llm:address:{addr}"

        # 1. 获取金表指标
        metrics_data = DetectiveRepository.get_address_gold_metrics(addr)

        if not metrics_data:
            probe = FallbackProbe()
            probe_result = await probe.probe_address(addr)
            return AddressDetailResponse(**probe_result)

        # 🌟 2. 彻底解耦：始终计算客观的 Trade 流水与 SFS 质量
        raw_trades = DetectiveRepository.get_recent_swaps(addr, limit=15)
        unique_tokens = list(set(t["token"] for t in raw_trades))
        sfs_dict = DetectiveRepository.get_tokens_sfs_metrics(unique_tokens)

        recent_trades = DetectiveCalculation.enrich_trades_with_sfs(raw_trades, sfs_dict)
        trade_quality = DetectiveCalculation.calc_trade_quality_stats(recent_trades)

        # 🌟 3. LLM 按需触发逻辑 (模式 A)
        llm_report = None

        if force_refresh:
            logger.info(f"Force refresh triggered for {addr}, generating new AI profile...")
            user_prompt = DetectiveCalculation.build_llm_payload(addr, metrics_data, recent_trades, trade_quality)
            try:
                llm_report = await request_llm_json(DetectiveCalculation.ADDRESS_SYSTEM_PROMPT, user_prompt)

                # 注入时间戳
                llm_report["generated_at"] = datetime.utcnow().isoformat()

                # 仅将报告存入 Redis，不再捆绑流水数据
                await redis.setex(llm_cache_key, 86400, json.dumps(llm_report))
            except Exception as e:
                logger.error(f"LLM fail for {addr}: {e}")
                llm_report = {"profile_report": "Analysis temporarily unavailable.", "style_label": "Unknown"}
        else:
            # 默认只读缓存，读不到就是 None，坚决不自动调用 LLM
            cached = await redis.get(llm_cache_key)
            if cached:
                llm_report = json.loads(cached)

        # 4. 组装返回数据 (保留了你上一轮最稳健的 .get() 赋值法)
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
            llm_analysis={
                "llm_analysis": llm_report, # 现在它可能是 None 了
                "recent_trades": recent_trades, # 但流水依然存在！
                "trade_quality": trade_quality
            },
            data_source="gold_precomputed",
            last_active=datetime.utcnow()
        )

    # ... (保留原有的 build_topology_graph 和 subscribe 方法)
    @staticmethod
    async def build_topology_graph(address: str) -> TopologyGraphResponse:
        nodes = [
            TopologyNode(id=address, label="Target Address", type="wallet"),
            TopologyNode(id="0x_moe_pool_mnt_usdt", label="MNT/USDT Pool", type="pool")
        ]
        edges = [
            TopologyEdge(source=address, target="0x_moe_pool_mnt_usdt", amount_usd=5000.0, token_symbol="MNT", timestamp=datetime.utcnow())
        ]
        return TopologyGraphResponse(nodes=nodes, edges=edges)

    @staticmethod
    async def subscribe(user_id: str, target_address: str):
        redis = await get_redis_client()
        try:
            await redis.sadd("subscribed_addresses", target_address)
            await redis.sadd(f"user_subs:{user_id}", target_address)
            logger.info(f"User {user_id} subscribed to {target_address}")
        except Exception as e:
            logger.error(f"Subscription failed: {e}")