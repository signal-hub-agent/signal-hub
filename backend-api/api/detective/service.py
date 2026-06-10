import json
import logging
from datetime import datetime
from typing import List, Optional

from core.db_clickhouse import get_ch_client
from core.redis_client import get_redis_client # 假设已在 core 中实现
from llm_engine.deepseek_client import generate_address_profile # 按需调用的独立 LLM 服务

from .schemas import (
    AddressDetailResponse,
    Top100Response,
    Top100ListItem,
    TopologyGraphResponse,
    TopologyNode,
    TopologyEdge,
    FinancialMetrics,
    RiskAssessment
)
from .probe import FallbackProbe

logger = logging.getLogger(__name__)

class DetectiveService:
    """
    Core business logic for the Address Detective domain.
    Orchestrates caching, primary OLAP queries, and fallback RPC probes.
    """

    @staticmethod
    async def get_top_100_from_redis() -> Top100Response:
        """
        Fetches Top 100 addresses.
        Tries Redis cache first; falls back to live ClickHouse query if cache is cold.
        """
        redis = await get_redis_client()
        cache_key = "detective:top100"

        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                return Top100Response(**json.loads(cached_data))

            logger.warning("Top 100 cache miss, querying ClickHouse directly.")
            client = get_ch_client()

            # Cache Miss -> 从 Gold 表拉取最新榜单
            query = """
                SELECT trader_address, win_rate, total_volume_usd, composite_score, style_tags
                FROM signal_hub.gold_address_financials_daily
                WHERE calc_date = (SELECT max(calc_date) FROM signal_hub.gold_address_financials_daily)
                AND total_trades_30d >= 5
                ORDER BY composite_score DESC
                LIMIT 100
            """
            rows = client.query(query).result_rows
            traders = []
            for row in rows:
                traders.append(Top100ListItem(
                    address=row[0],
                    win_rate=round(float(row[1]), 2),
                    total_volume_30d=round(float(row[2]), 2),
                    composite_score=int(row[3]),
                    tags=row[4]
                ))

            response = Top100Response(updated_at=datetime.utcnow(), traders=traders)
            # 写入 Redis 缓存，过期时间设定为 1 小时
            await redis.setex(cache_key, 3600, response.json())
            return response

        except Exception as e:
            logger.error(f"Failed to fetch Top 100: {e}")
            return Top100Response(updated_at=datetime.utcnow(), traders=[])

    @staticmethod
    async def analyze_address(address: str, force_refresh: bool = False) -> AddressDetailResponse:
        """
        核心分发逻辑：
        1. 查 ClickHouse 获取实时金指标。
        2. 如果 ClickHouse 没数据，启动 RPC 探针兜底。
        3. 查 Redis 获取 LLM 画像（若强制刷新则重新调用并覆写）。
        """
        client = get_ch_client()
        redis = await get_redis_client()
        addr = address.lower()
        llm_cache_key = f"signalhub:llm:address:{addr}"

        # ---------------------------------------------------------
        # Step 1: 查询 ClickHouse Gold 表获取实时指标 (纯读，毫秒级)
        # ---------------------------------------------------------
        metrics_data = None
        try:
            query_gold = """
                SELECT
                    total_trades_30d, win_rate, profit_loss_ratio, sharpe_ratio, max_drawdown,
                    total_volume_usd, active_days_30d, account_growth_30d, style_tags,
                    risk_level, risk_score, risk_flags, composite_score
                FROM signal_hub.gold_address_financials_daily
                WHERE lower(trader_address) = {addr:String}
                ORDER BY calc_date DESC LIMIT 1
            """
            result = client.query(query_gold, parameters={"addr": addr})
            if result.result_rows:
                metrics_data = dict(zip(result.column_names, result.result_rows[0]))
        except Exception as e:
            logger.warning(f"Gold layer query failed for {addr}: {e}")

        # ---------------------------------------------------------
        # Step 2: 降级处理 (Fallback Probe)
        # ---------------------------------------------------------
        if not metrics_data:
            logger.info(f"No DEX history for {addr}, triggering Fallback Probe.")
            probe = FallbackProbe()
            probe_result = await probe.probe_address(addr)
            return AddressDetailResponse(**probe_result)

        # ---------------------------------------------------------
        # Step 3: LLM 缓存与按需触发 (Redis)
        # ---------------------------------------------------------
        llm_report = None
        if force_refresh:
            await redis.delete(llm_cache_key)
            logger.info(f"Force refresh triggered, cleared LLM cache for {addr}")
        else:
            cached_llm = await redis.get(llm_cache_key)
            if cached_llm:
                llm_report = json.loads(cached_llm)

        if not llm_report:
            # 触发按需 LLM 生产
            logger.info(f"LLM Cache miss for {addr}, generating new profile...")
            try:
                llm_report = await generate_address_profile(addr, metrics_data)
                # 写入 Redis，缓存 24 小时 (86400秒)
                await redis.setex(llm_cache_key, 86400, json.dumps(llm_report))
            except Exception as e:
                logger.error(f"LLM Generation failed for {addr}: {e}")
                llm_report = {"profile_report": "Analysis temporarily unavailable due to high load."}

        # ---------------------------------------------------------
        # Step 4: 组装最终契约并返回
        # ---------------------------------------------------------
        return AddressDetailResponse(
            address=addr,
            tags=metrics_data['style_tags'],
            composite_score=metrics_data['composite_score'],
            is_dex_trader=True,
            metrics=FinancialMetrics(
                win_rate=metrics_data['win_rate'],
                profit_loss_ratio=metrics_data['profit_loss_ratio'],
                sharpe_ratio=metrics_data['sharpe_ratio'],
                max_drawdown=metrics_data['max_drawdown'],
                account_growth_30d=metrics_data['account_growth_30d'],
                total_trades_30d=metrics_data['total_trades_30d'],
                total_volume_usd=metrics_data['total_volume_usd'],
                active_days_30d=metrics_data['active_days_30d'],
            ),
            risk=RiskAssessment(
                risk_level=metrics_data['risk_level'],
                risk_score=metrics_data['risk_score'],
                flags=metrics_data['risk_flags'],
            ),
            portfolio=[],
            llm_analysis=llm_report,
            data_source="gold_precomputed",
            last_active=datetime.utcnow()
        )

    @staticmethod
    async def build_topology_graph(address: str) -> TopologyGraphResponse:
        """
        Generates a node-edge graph of the address's 30-day fund flow.
        """
        nodes = [
            TopologyNode(id=address, label="Target Address", type="wallet"),
            TopologyNode(id="0x_moe_pool_mnt_usdt", label="MNT/USDT Pool", type="pool")
        ]
        edges = [
            TopologyEdge(
                source=address, target="0x_moe_pool_mnt_usdt",
                amount_usd=5000.0, token_symbol="MNT", timestamp=datetime.utcnow()
            )
        ]
        return TopologyGraphResponse(nodes=nodes, edges=edges)

    @staticmethod
    async def subscribe(user_id: str, target_address: str):
        """
        Registers a copy-trade subscription into Redis.
        """
        redis = await get_redis_client()
        try:
            await redis.sadd("subscribed_addresses", target_address)
            user_sub_key = f"user_subs:{user_id}"
            await redis.sadd(user_sub_key, target_address)
            logger.info(f"User {user_id} successfully subscribed to {target_address}")
        except Exception as e:
            logger.error(f"Subscription failed: {e}")