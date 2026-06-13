import logging
from core.db_clickhouse import get_ch_client

logger = logging.getLogger(__name__)

class SignalRepository:
    @staticmethod
    def get_tokens_from_gold() -> list:
        client = get_ch_client()
        try:
            rows = client.query("""
                SELECT DISTINCT token_symbol
                FROM signal_hub.gold_token_metrics_1h
                ORDER BY token_symbol LIMIT 50
            """).result_rows
            return [r[0] for r in rows if r[0]]
        except Exception as e:
            logger.debug(f"Failed to fetch tokens from gold layer: {e}")
            return []

    @staticmethod
    def get_tokens_from_dune() -> list:
        client = get_ch_client()
        try:
            rows = client.query("""
                SELECT upper(token_bought_symbol) AS token, COUNT() AS cnt
                FROM web3_data.dune_swaps
                GROUP BY token ORDER BY cnt DESC LIMIT 50
            """).result_rows
            return [r[0] for r in rows if r[0]]
        except Exception as e:
            logger.error(f"Failed to fetch tokens from dune: {e}")
            return []

    @staticmethod
    def get_token_gold_metrics(token_symbol: str) -> dict:
        client = get_ch_client()
        query = """
            SELECT * FROM signal_hub.gold_token_metrics_1h
            WHERE upper(token_symbol) = {token:String}
            ORDER BY calc_time DESC LIMIT 1
        """
        result = client.query(query, parameters={"token": token_symbol.upper()})
        if not result.result_rows:
            return {}
        return dict(zip(result.column_names, result.result_rows[0]))