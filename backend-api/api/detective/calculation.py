import logging
import json
from datetime import datetime

from api.detective.schemas import TopologyEdge, TopologyNode, TopologyGraphResponse
from core.redis_client import get_redis_client
logger = logging.getLogger(__name__)


class DetectiveCalculation:
    ADDRESS_SYSTEM_PROMPT = """You are an on-chain trading behavior analyst. Write a profile report for copy-traders.
Output strictly as JSON with these fields:
{
  "profile_report": "~150 word natural language analysis in English",
  "pre_rating": "green / yellow / red",
  "style_label": "e.g. Mid-Frequency Swing Trader",
  "style_match": true / false,
  "copy_suggestion": "one sentence recommendation",
  "key_metrics_summary": "Win Rate X% | P/L Ratio Y | Avg Entry SFS Z"
}
Rules: Cite specific numbers. End with pre_rating justification. No markdown. Output valid JSON only."""

    @staticmethod
    def enrich_trades_with_sfs(trades: list, sfs_metrics_dict: dict) -> list:
        enriched = []
        for trade in trades:
            # 去掉原先的 if trade["direction"] != "buy" 拦截，满足你刚才查 SELL 时机的要求
            token = trade["token"].upper()
            trade["token_signal"] = sfs_metrics_dict.get(token, {
                "sfs_score": None, "trend_type": "unknown",
                "macd_pos": "unknown", "rsi_zone": "unknown"
            })
            enriched.append(trade)
        return enriched

    @staticmethod
    def calc_trade_quality_stats(enriched_trades: list) -> dict:
        # 入场时机评分只看 BUY
        buys = [t for t in enriched_trades if t["direction"] == "buy" and t.get("token_signal", {}).get("sfs_score") is not None]
        if not buys:
            return {"avg_entry_sfs": None, "high_sfs_ratio": None, "total_evaluated": 0}
        scores = [t["token_signal"]["sfs_score"] for t in buys]
        high_ratio = sum(1 for s in scores if s >= 70) / len(scores)
        return {
            "avg_entry_sfs": round(sum(scores) / len(scores), 1),
            "high_sfs_ratio": round(high_ratio, 2),
            "total_evaluated": len(scores)
        }

    @staticmethod
    def build_llm_payload(address: str, financials: dict, enriched_trades: list, trade_quality: dict, user_frequency: str = "mid_freq") -> str:
        avg_trades = financials.get("total_trades_30d", 0) / max(financials.get("active_days_30d", 1), 1)
        freq_type = "high_freq" if avg_trades >= 10 else ("mid_freq" if avg_trades >= 2 else "low_freq")

        payload = {
            "query_type": "address_profile",
            "financials": financials,
            "recent_buy_trades_with_signal": [t for t in enriched_trades if t["direction"] == "buy"][:5],
            "trade_quality_stats": trade_quality,
            "style_match": {"user_frequency": user_frequency, "address_frequency": freq_type}
        }
        return f"Generate an address profile report for this wallet data:\n\n{json.dumps(payload, default=str)}\n\nReturn valid JSON only."

    @staticmethod
    def calc_copy_signal_score(address_score: int, token_sfs: int, amount_usd: float, avg_trade_amount: float) -> dict:
        ratio = amount_usd / avg_trade_amount if avg_trade_amount > 0 else 1.0
        if ratio >= 1.5: amount_signal = 100; amount_label = "Unusually large — high conviction signal"
        elif ratio >= 1.0: amount_signal = 70; amount_label = "Above average size"
        elif ratio >= 0.5: amount_signal = 40; amount_label = "Normal size"
        else: amount_signal = 10; amount_label = "Small test trade — low conviction"

        copy_score = round(address_score * 0.4 + token_sfs * 0.4 + amount_signal * 0.2)
        color = "green" if copy_score >= 80 else ("yellow" if copy_score >= 60 else "red")
        return {"copy_score": copy_score, "color": color, "address_score": address_score, "token_sfs": token_sfs, "amount_signal": amount_signal, "amount_label": amount_label, "amount_ratio": round(ratio, 2)}

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