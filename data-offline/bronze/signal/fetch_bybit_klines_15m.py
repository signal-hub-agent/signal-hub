import os
import sys
import time
import requests
import json
import logging
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
# 位于 bronze/signal/ 下，退两层到 data-offline
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)

from config.database import get_clickhouse_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BYBIT_API_URL = "https://api.bybit.com/v5/market/kline"

# ==============================================================================
# Token Mapping: 链上 Symbol -> Bybit 交易对 Symbol
# 已自动过滤掉 Bybit 不存在的合成股票(wTSLAx等)和未上所的土狗币
# ==============================================================================
SUPPORTED_TOKENS = {
    # 核心大盘与基础币
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "MNT": "MNTUSDT",
    "SOL": "SOLUSDT",

    # Top 30 映射转换
    "WMNT": "MNTUSDT",
    "WETH": "ETHUSDT",
    "WBTC": "BTCUSDT",
    "FBTC": "BTCUSDT",      # 映射到基础资产
    "USDe": "USDEUSDT",
    "USDC": "USDCUSDT",

    # Mantle 生态热门币 / 已上所
    "mETH": "METHUSDT",
    "COOK": "COOKUSDT",
    "PUFF": "PUFFUSDT",
    "CATI": "CATIUSDT"
}

INTERVALS = {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}

def fetch_kline_from_bybit(symbol: str, interval: str, limit: int = 200, max_retries: int = 3) -> list:
    """
    Abstracted method to fetch Kline data from Bybit API with retry logic.
    """
    params = {
        "category": "spot",
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(BYBIT_API_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                return data["result"]["list"]
            else:
                logger.warning("Bybit API returned invalid data for %s (%s) on attempt %d", symbol, interval, attempt + 1)

        except requests.RequestException as e:
            logger.error("API Request failed for %s (%s) on attempt %d: %s", symbol, interval, attempt + 1, str(e))

        time.sleep(2)

    return None

def fetch_and_store_raw_klines(limit: int = 200):
    """
    Main job execution for fetching and storing raw klines to the Bronze layer.
    """
    client = get_clickhouse_client()
    records = []
    fetch_ts = datetime.utcnow()

    for db_symbol, api_symbol in SUPPORTED_TOKENS.items():
        for interval_name, interval_code in INTERVALS.items():

            kline_list = fetch_kline_from_bybit(api_symbol, interval_code, limit)

            if kline_list:
                raw_json_str = json.dumps(kline_list)
                # 存入 ClickHouse 的是链上的名字 (db_symbol)，这样才能和 clean_swaps 里的名字对齐
                records.append((db_symbol, interval_name, raw_json_str, fetch_ts))
            else:
                logger.error("Skipping insertion for %s (%s) due to consecutive API failures.", db_symbol, interval_name)

    if records:
        try:
            client.execute(
                "INSERT INTO signal_hub.raw_klines (token_symbol, interval, raw_json, fetch_timestamp) VALUES",
                records
            )
            logger.info("Successfully inserted %d raw kline records to Bronze layer.", len(records))
        except Exception as e:
            logger.error("ClickHouse insertion failed: %s", str(e))

if __name__ == "__main__":
    fetch_and_store_raw_klines()