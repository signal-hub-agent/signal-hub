import logging
from core.db_clickhouse import get_ch_client

logger = logging.getLogger(__name__)

class DashboardRepository:
    @staticmethod
    def get_top_10_tokens() -> list:
        client = get_ch_client()
        # 按照综合得分 (composite_score) 取 Top 10
        query = """
            SELECT 
                token_symbol, 
                argMax(volume_24h_usd, calc_time) as vol, 
                argMax(mev_toxicity_pct, calc_time) as mev,
                argMax(composite_score, calc_time) as score
            FROM signal_hub.gold_token_metrics_1h 
            GROUP BY token_symbol 
            ORDER BY score DESC LIMIT 10
        """
        try:
            rows = client.query(query).result_rows
            return [
                {
                    "symbol": r[0].upper(),
                    "volume_1h_usd": float(r[1] or 0),
                    "mev_toxicity_pct": float(r[2] or 0),
                    "ai_score": float(r[3] or 0)
                } for r in rows
            ]
        except Exception as e:
            logger.error(f"Failed to fetch top 10 tokens: {e}")
            return []

    @staticmethod
    def get_top_10_smart_money() -> list:
        client = get_ch_client()
        # 按照综合得分 (composite_score) 取 Top 10
        query = """
            SELECT 
                trader_address, win_rate, profit_loss_ratio, style_tags, composite_score
            FROM signal_hub.gold_address_financials_daily 
            WHERE calc_date = (SELECT max(calc_date) FROM signal_hub.gold_address_financials_daily)
            ORDER BY composite_score DESC LIMIT 10
        """
        try:
            rows = client.query(query).result_rows
            res = []
            for r in rows:
                tags = r[3] if isinstance(r[3], list) else []
                res.append({
                    "address": r[0],
                    "win_rate": float(r[1] or 0),
                    "pnl_ratio": float(r[2] or 0),
                    "tags": tags,
                    "score": int(r[4] or 0)
                })
            return res
        except Exception as e:
            logger.error(f"Failed to fetch top 10 smart money: {e}")
            return []