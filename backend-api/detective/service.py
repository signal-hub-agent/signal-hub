import json
import logging
from datetime import datetime
from typing import List

from core.db_clickhouse import ch_manager
from core.redis_client import redis_manager
from detective.schemas import (
    AddressDetailResponse,
    Top100Response,
    Top100ListItem,
    TopologyGraphResponse,
    TopologyNode,
    TopologyEdge,
    FinancialMetrics,
    RiskAssessment
)
from detective.fallback_probe import fallback_probe
from detective.detective_service import analyze_address_metrics, get_top_traders

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
        redis = redis_manager.get_client()
        cache_key = "detective:top100"

        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                data_dict = json.loads(cached_data)
                return Top100Response(**data_dict)

            logger.warning("Top 100 cache miss. Querying ClickHouse directly.")

        except Exception as e:
            logger.warning("Redis unavailable, falling back to ClickHouse: %s", str(e))

        # Fallback: query gold_address_financials_daily directly
        traders = get_top_traders(limit=100)
        return Top100Response(
            updated_at=datetime.utcnow(),
            traders=[Top100ListItem(**t) for t in traders]
        )

    @staticmethod
    async def analyze_address(address: str) -> AddressDetailResponse:
        """
        Central analysis engine for a wallet address.
        Level 1: gold_address_financials_daily (pre-computed)
        Level 2: clean_swaps real-time calculation
        Level 3: Web3 RPC fallback (no DEX history)
        """
        try:
            result = analyze_address_metrics(address)

            return AddressDetailResponse(
                address=result["address"],
                tags=result["tags"],
                composite_score=result["composite_score"],
                is_dex_trader=result["is_dex_trader"],
                metrics=FinancialMetrics(**result["metrics"]) if result["metrics"] else None,
                portfolio=[],
                risk=RiskAssessment(**result["risk"]),
                last_active=datetime.utcnow()
            )

        except Exception as e:
            logger.error("Error analyzing address %s: %s", address, str(e))
            # Final safety net: RPC fallback
            return await fallback_probe.probe_address(address)

    @staticmethod
    async def build_topology_graph(address: str) -> TopologyGraphResponse:
        """
        Extracts a graph of fund flows for the given address over the last 7 days.
        TODO: Replace mock with real clean_swaps query.
        """
        nodes = [
            TopologyNode(id=address, label="Target Address", type="wallet"),
            TopologyNode(id="0x_moe_pool_mnt_usdt", label="MNT/USDT Pool", type="pool")
        ]

        edges = [
            TopologyEdge(
                source=address,
                target="0x_moe_pool_mnt_usdt",
                amount_usd=5000.0,
                token_symbol="MNT",
                timestamp=datetime.utcnow()
            )
        ]

        return TopologyGraphResponse(nodes=nodes, edges=edges)

    @staticmethod
    async def subscribe(user_id: str, target_address: str):
        """
        Registers a copy-trade subscription.
        Adds address to Redis watchlist so the data pipeline emits real-time signals.
        """
        redis = redis_manager.get_client()

        try:
            # Add to global watchlist for the interceptor
            await redis.sadd("subscribed_addresses", target_address)

            # Record per-user subscription
            user_sub_key = f"user_subs:{user_id}"
            await redis.sadd(user_sub_key, target_address)

            # TODO: Persist to PostgreSQL for durability

            logger.info("User %s successfully subscribed to %s", user_id, target_address)

        except Exception as e:
            logger.error("Subscription failed for user %s, address %s: %s", user_id, target_address, str(e))
            raise