"""
K线数据拉取 - Bybit API (离线数据工程工具)
免费接口，无需API Key
支持1h / 4h / 日线
"""
import requests
import pandas as pd
from typing import Literal

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"

INTERVAL_MAP = {
    "1h": "60",
    "4h": "240",
    "1d": "D",
}

SYMBOL_MAP = {
    "BTC":   "BTCUSDT",
    "ETH":   "ETHUSDT",
    "BNB":   "BNBUSDT",
    "SOL":   "SOLUSDT",
    "MATIC": "MATICUSDT",
    "AVAX":  "AVAXUSDT",
    "ARB":   "ARBUSDT",
    "OP":    "OPUSDT",
    "LINK":  "LINKUSDT",
    "UNI":   "UNIUSDT",
    "AAVE":  "AAVEUSDT",
    "MNT":   "MNTUSDT",
    "MOE":   "MOEUSDT",
}

def fetch_klines(
        token_symbol: str,
        interval: Literal["1h", "4h", "1d"] = "1h",
        limit: int = 200,
) -> pd.DataFrame:
    """
    从Bybit拉取K线数据
    返回 DataFrame，列：open_time, open, high, low, close, volume
    时间从旧到新排列
    """
    symbol = SYMBOL_MAP.get(token_symbol.upper())
    if not symbol:
        raise ValueError(f"Unsupported token: {token_symbol}. Supported: {list(SYMBOL_MAP.keys())}")

    params = {
        "category": "linear",
        "symbol":   symbol,
        "interval": INTERVAL_MAP[interval],
        "limit":    limit,
    }

    resp = requests.get(BYBIT_KLINE_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit API error: {data.get('retMsg')}")

    raw = data["result"]["list"]
    if not raw:
        raise RuntimeError(f"No kline data returned for {token_symbol}")

    df = pd.DataFrame(raw, columns=["open_time", "open", "high", "low", "close", "volume", "turnover"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    df["open_time"] = pd.to_numeric(df["open_time"])
    df = df.sort_values("open_time").reset_index(drop=True)
    return df