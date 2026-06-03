import requests
import json
import logging
from datetime import datetime
from config.database import get_clickhouse_client

logger = logging.getLogger(__name__)

BYBIT_API_URL = "https://api.bybit.com/v5/market/kline"
SUPPORTED_TOKENS = {"MNT": "MNTUSDT", "ETH": "ETHUSDT", "BTC": "BTCUSDT"}
INTERVALS = {"1h": "60", "4h": "240", "1d": "D"}


def fetch_and_store_raw_klines(limit: int = 200):
    client = get_clickhouse_client()
    records = []
    fetch_ts = datetime.utcnow()

    for token, symbol in SUPPORTED_TOKENS.items():
        for interval_name, interval_code in INTERVALS.items():
            params = {
                "category": "linear",
                "symbol": symbol,
                "interval": interval_code,
                "limit": limit
            }
            try:
                response = requests.get(BYBIT_API_URL, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                    raw_json_str = json.dumps(data["result"]["list"])
                    records.append((token, interval_name, raw_json_str, fetch_ts))
                else:
                    logger.warning("Invalid API response for %s (%s)", token, interval_name)

            except requests.RequestException as e:
                logger.error("Failed to fetch data for %s (%s): %s", token, interval_name, str(e))

    if records:
        try:
            client.execute(
                "INSERT INTO raw_klines (token_symbol, interval, raw_json, fetch_timestamp) VALUES",
                records
            )
            logger.info("Successfully inserted %d raw kline records.", len(records))
        except Exception as e:
            logger.error("ClickHouse insertion failed: %s", str(e))


if __name__ == "__main__":
    fetch_and_store_raw_klines()