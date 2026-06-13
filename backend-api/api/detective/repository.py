import logging
from core.db_clickhouse import get_ch_client
from .schemas import Top100ListItem

logger = logging.getLogger(__name__)

class DetectiveRepository:
    @staticmethod
    def get_top_100_financials() -> list:
        client = get_ch_client()
        query = """
            SELECT trader_address, win_rate, total_volume_usd, composite_score, style_tags
            FROM signal_hub.gold_address_financials_daily
            WHERE calc_date = (SELECT max(calc_date) FROM signal_hub.gold_address_financials_daily)
            AND total_trades_30d >= 5
            ORDER BY composite_score DESC LIMIT 100
        """
        rows = client.query(query).result_rows
        return [
            Top100ListItem(
                address=row[0], win_rate=round(float(row[1]), 2),
                total_volume_30d=round(float(row[2]), 2),
                composite_score=int(row[3]), tags=row[4]
            ) for row in rows
        ]

    @staticmethod
    def get_address_gold_metrics(address: str) -> dict:
        client = get_ch_client()
        query = """
            SELECT total_trades_30d, win_rate, profit_loss_ratio, sharpe_ratio, max_drawdown,
                   total_volume_usd, active_days_30d, account_growth_30d, style_tags,
                   risk_level, risk_score, risk_flags, composite_score
            FROM signal_hub.gold_address_financials_daily
            WHERE lower(trader_address) = {addr:String}
            ORDER BY calc_date DESC LIMIT 1
        """
        result = client.query(query, parameters={"addr": address.lower()})
        return dict(zip(result.column_names, result.result_rows[0])) if result.result_rows else {}

    @staticmethod
    def get_recent_swaps(address: str, limit: int = 15) -> list:
        client = get_ch_client()
        query = """
            SELECT toDateTime(block_timestamp/1000) AS trade_time,
                   token_in_symbol, token_out_symbol, token_in_amount, token_out_amount, amount_usd, tx_hash
            FROM signal_hub.clean_swaps
            WHERE lower(trader_address) = {addr:String} AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
            ORDER BY block_timestamp DESC LIMIT {limit:UInt32}
        """
        try:
            rows = client.query(query, parameters={"addr": address.lower(), "limit": limit}).result_rows
            trades = []
            for row in rows:
                stables = {"USDT", "USDC", "DAI", "USDE", "MUSD"}
                direction = "buy" if row[1].upper() in stables else "sell"
                token = row[2] if direction == "buy" else row[1]
                trades.append({
                    "time": str(row[0]), "token": token, "direction": direction,
                    "amount_usd": round(float(row[5]), 2), "tx_hash": row[6]
                })
            return trades
        except Exception as e:
            logger.warning(f"Failed to fetch swaps for {address}: {e}")
            return []

    @staticmethod
    def get_tokens_sfs_metrics(token_symbols: list) -> dict:
        if not token_symbols: return {}
        client = get_ch_client()
        tokens_tuple = tuple(t.upper() for t in token_symbols)
        query = f"""
            SELECT token_symbol, tech_score, ma_trend_type, macd_position, rsi_zone 
            FROM signal_hub.gold_token_metrics_1h 
            WHERE upper(token_symbol) IN {tokens_tuple}
        """
        try:
            rows = client.query(query).result_rows
            return {
                row[0].upper(): {
                    "sfs_score": int(row[1]) if row[1] is not None else None,
                    "trend_type": row[2] or "unknown",
                    "macd_pos": row[3] or "unknown",
                    "rsi_zone": row[4] or "unknown"
                } for row in rows
            }
        except Exception:
            return {}