import json
import logging
from datetime import datetime
from typing import List

from core.db_clickhouse import ch_manager
from core.redis_client import redis_manager
# from core.db_postgres import pg_manager  # Uncomment when implementing PG subscription persistence
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

logger = logging.getLogger(__name__)

class DetectiveService:
    """
    Core business logic for the Address Detective domain.
    Orchestrates caching, primary OLAP queries, and fallback RPC probes.
    """

    @staticmethod
    async def get_top_100_from_redis() -> Top100Response:
        """
        Fetches the pre-calculated Top 100 addresses directly from Redis cache.
        Returns empty list if cache is missing (e.g., background task hasn't run yet).
        """
        redis = redis_manager.get_client()
        cache_key = "detective:top100"

        try:
            cached_data = await redis.get(cache_key)
            if cached_data:
                # Assuming the background task stored a JSON string matching our schema
                data_dict = json.loads(cached_data)
                return Top100Response(**data_dict)

            logger.warning("Top 100 cache miss. Returning empty list.")
            return Top100Response(updated_at=datetime.utcnow(), traders=[])

        except Exception as e:
            logger.error("Failed to fetch Top 100 from Redis: %s", str(e))
            raise

    @staticmethod
    async def analyze_address(address: str) -> AddressDetailResponse:
        """
        The central analysis engine.
        Level 1: Query ClickHouse for DEX history.
        Level 2: Fallback to Web3 RPC if no history found.
        """
        client = ch_manager.get_client()

        # 1. Check if the address exists in our clean_swaps table (Level 1)
        # We query the last 30 days to see if they are an active DEX trader
        query_check = """
            SELECT count() 
            FROM signal_hub.clean_swaps 
            WHERE trader_address = %(address)s 
              AND block_timestamp >= now() - INTERVAL 30 DAY
        """

        try:
            result = client.execute(query_check, {'address': address})
            swap_count = result[0][0] if result else 0

            if swap_count == 0:
                # 🌟 Level 2: Fallback Probe Triggered!
                logger.info("No DEX history for %s. Triggering fallback probe.", address)
                return await fallback_probe.probe_address(address)

            # --- If we reach here, they are a DEX Trader (Level 1 Hit) ---
            logger.info("Found %d swaps for %s. Calculating rich metrics.", swap_count, address)

            # TODO: Execute complex ClickHouse queries here to calculate real PnL, Win Rate, etc.
            # For demonstration, we construct a mocked rich response.
            # In reality, you would map the ClickHouse `result` tuples to these fields.

            return AddressDetailResponse(
                address=address,
                tags=["Whale", "High-Frequency", "DEX Veteran"],
                composite_score=85,
                is_dex_trader=True, # Flag tells frontend to render the radar chart
                metrics=FinancialMetrics(
                    win_rate=68.5,
                    profit_loss_ratio=2.1,
                    sharpe_ratio=1.8,
                    max_drawdown=-12.4,
                    account_growth_30d=34.2
                ),
                portfolio=[], # Would be populated via another CH query or RPC
                risk=RiskAssessment(
                    risk_level="GREEN",
                    risk_score=9,
                    flags=["No MEV activity detected."]
                ),
                last_active=datetime.utcnow()
            )

        except Exception as e:
            logger.error("Error analyzing address %s: %s", address, str(e))
            # Even if DB fails, try the fallback probe to prevent hard errors
            return await fallback_probe.probe_address(address)

    @staticmethod
    async def build_topology_graph(address: str) -> TopologyGraphResponse:
        """
        Extracts a graph of fund flows for the given address over the last 7 days.
        """
        # TODO: Query ClickHouse for all swaps involving this address.
        # client = ch_manager.get_client()
        # query = "SELECT ... FROM clean_swaps WHERE trader_address = %(address)s"

        # Mocking the topology generation for architectural demonstration
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
        Registers a subscription.
        Critical Step: Adds the address to a Redis Set so the offline data pipeline
        (the interceptor) knows to emit events for this address.
        """
        redis = redis_manager.get_client()

        try:
            # 1. Add to the global "watchlist" set for the interceptor
            await redis.sadd("subscribed_addresses", target_address)

            # 2. Record the specific user's subscription relation
            user_sub_key = f"user_subs:{user_id}"
            await redis.sadd(user_sub_key, target_address)

            # TODO: Persist this relationship to PostgreSQL for durability

            logger.info("User %s successfully subscribed to %s", user_id, target_address)
        except Exception as e:
            logger.error("Subscription failed for user %s, address %s: %s", user_id, target_address, str(e))
            raise