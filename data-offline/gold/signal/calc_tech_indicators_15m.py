"""
离线调度任务：代币技术面特征计算
"""
import os
import sys
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from calc_token_metrics_1h import calc_volume_trend, calc_mev_toxicity

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)

from config.database import get_clickhouse_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= 保持原状的内部结构 =================
@dataclass
class EMAResult:
    ema7: float; ema25: float; ema99: float
    alignment: str; trend_type: str
    ema99_support: bool; compression: bool

@dataclass
class MACDResult:
    dif: float; dea: float; histogram: float
    dif_dea_position: str; histogram_trend: str
    divergence: Optional[str]; comment: str

@dataclass
class RSIResult:
    value: float; zone: str; divergence: Optional[str]

@dataclass
class BollingerResult:
    upper: float; mid: float; lower: float; bandwidth: str
    mid_band_direction: str; price_position: str
    support: float; resistance: float; touch_edge: Optional[str]

@dataclass
class KDJResult:
    k: float; d: float; j: float; zone: str

@dataclass
class SFIResult:
    value: Optional[float]; comment: str

@dataclass
class TechnicalAnalysisResult:
    token_symbol: str; current_price: float
    raw_score: float; tech_score: int
    ma: EMAResult; macd: MACDResult; rsi: RSIResult
    bollinger: BollingerResult; kdj: KDJResult; sfi: SFIResult

def get_klines_from_db(client, token_symbol: str, interval: str = "1h", limit: int = 200) -> pd.DataFrame:
    query = f"""
        SELECT open_time, open, high, low, close, volume
        FROM signal_hub.clean_klines
        WHERE upper(token_symbol) = %(token)s AND `interval` = '{interval}'
        ORDER BY open_time ASC LIMIT {int(limit)}
    """
    rows = client.execute(query, {'token': token_symbol.upper()})
    if not rows: return pd.DataFrame()
    return pd.DataFrame(rows, columns=['open_time', 'open', 'high', 'low', 'close', 'volume'])

def calculate_ema(df: pd.DataFrame) -> EMAResult:
    df['ema7'] = df['close'].ewm(span=7, adjust=False).mean()
    df['ema25'] = df['close'].ewm(span=25, adjust=False).mean()
    df['ema99'] = df['close'].ewm(span=99, adjust=False).mean()
    last = df.iloc[-1]
    e7, e25, e99, price = last['ema7'], last['ema25'], last['ema99'], last['close']
    if e7 > e25 > e99: alignment = "EMA7 > EMA25 > EMA99"
    elif e99 > e25 > e7: alignment = "EMA99 > EMA25 > EMA7"
    else: alignment = "mixed"
    gap_percent = abs(e7 - e99) / e99 * 100
    compression = gap_percent < 0.5
    if alignment == "EMA7 > EMA25 > EMA99" and not compression: trend = "bullish_trending"
    elif alignment == "EMA99 > EMA25 > EMA7" and not compression: trend = "bearish_trending"
    elif compression: trend = "compressing"
    else: trend = "range_bound"
    return EMAResult(e7, e25, e99, alignment, trend, (price > e99 * 0.99 and price < e99 * 1.05), compression)

def calculate_macd(df: pd.DataFrame) -> MACDResult:
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_dif'] = ema12 - ema26
    df['macd_dea'] = df['macd_dif'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = (df['macd_dif'] - df['macd_dea']) * 2
    last, prev = df.iloc[-1], df.iloc[-2]
    dif, dea, hist = last['macd_dif'], last['macd_dea'], last['macd_hist']
    if dif > 0 and dea > 0: pos = "zero_above_golden_cross" if dif > dea else "zero_above_bearish"
    elif dif < 0 and dea < 0: pos = "zero_below_golden_cross" if dif > dea else "zero_below_bearish"
    else: pos = "zero_cross_boundary"
    if hist > 0 and hist > prev['macd_hist']: hist_trend = "green_expanding"
    elif hist > 0: hist_trend = "green_shrinking"
    elif hist < 0 and hist < prev['macd_hist']: hist_trend = "red_expanding"
    else: hist_trend = "red_shrinking"
    return MACDResult(dif, dea, hist, pos, hist_trend, None, f"DIF: {dif:.4f}")

def calculate_rsi(df: pd.DataFrame, period=14) -> RSIResult:
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    rsi_val = df['rsi'].iloc[-1]
    if rsi_val > 70: zone = "overbought"
    elif rsi_val < 30: zone = "oversold"
    elif rsi_val > 50: zone = "bullish"
    else: zone = "bearish"
    return RSIResult(rsi_val, zone, None)

def calculate_bollinger(df: pd.DataFrame, period=20) -> BollingerResult:
    df['mid'] = df['close'].rolling(window=period).mean()
    df['std'] = df['close'].rolling(window=period).std()
    df['upper'] = df['mid'] + 2 * df['std']
    df['lower'] = df['mid'] - 2 * df['std']
    last = df.iloc[-1]
    price = last['close']
    bw = (last['upper'] - last['lower']) / last['mid'] * 100
    bw_str = "expanding" if bw > 5 else "compressing"
    pos = "above_mid" if price > last['mid'] else "below_mid"
    touch = "touch_upper" if price >= last['upper'] * 0.99 else ("touch_lower" if price <= last['lower'] * 1.01 else None)
    return BollingerResult(last['upper'], last['mid'], last['lower'], bw_str, "flat", pos, last['lower'], last['upper'], touch)

def calculate_kdj(df: pd.DataFrame, n=9) -> KDJResult:
    low_min = df['low'].rolling(window=n).min()
    high_max = df['high'].rolling(window=n).max()
    rsv = (df['close'] - low_min) / (high_max - low_min) * 100
    df['k'] = rsv.ewm(com=2).mean()
    df['d'] = df['k'].ewm(com=2).mean()
    df['j'] = 3 * df['k'] - 2 * df['d']
    last = df.iloc[-1]
    if last['j'] > 80: zone = "overbought"
    elif last['j'] < 20: zone = "oversold"
    else: zone = "normal"
    return KDJResult(last['k'], last['d'], last['j'], zone)

def run_technical_analysis(client, token_symbol: str) -> TechnicalAnalysisResult:
    df = get_klines_from_db(client, token_symbol, interval="1h", limit=200)
    if df is None or df.empty:
        raise ValueError(f"No kline data found in clean_klines for {token_symbol}")

    ema = calculate_ema(df)
    macd = calculate_macd(df)
    rsi = calculate_rsi(df)
    boll = calculate_bollinger(df)
    kdj = calculate_kdj(df)

    score = 15.0
    if ema.trend_type == "bullish_trending": score += 5
    elif ema.trend_type == "bearish_trending": score -= 5
    if macd.dif_dea_position.startswith("zero_above"): score += 3
    if rsi.zone == "oversold": score += 2
    elif rsi.zone == "overbought": score -= 2
    if boll.price_position == "above_mid": score += 2
    if kdj.zone == "oversold": score += 2
    final_score = max(0, min(30, int(score)))

    return TechnicalAnalysisResult(
        token_symbol, df.iloc[-1]['close'], score, final_score,
        ema, macd, rsi, boll, kdj, SFIResult(None, "Insufficient data")
    )

# 🌟 将 address_map 作为参数传入
def compute_and_save_tech_metrics(client, token_symbol: str, address_map: dict):
    try:
        res_tech = run_technical_analysis(client, token_symbol)

        # 🌟 传入 force_recalc=True 强行查底层流水，拒绝吃旧缓存
        res_vol = calc_volume_trend(token_symbol, force_recalc=True)
        res_mev = calc_mev_toxicity(token_symbol, force_recalc=True)

        contract_score = 28
        composite_score = min(100, res_tech.tech_score + res_vol.score + res_mev.score + contract_score)

        if composite_score >= 70: signal_color = "green"
        elif composite_score >= 40: signal_color = "yellow"
        else: signal_color = "red"

        table_name = "signal_hub.gold_token_metrics_1h"
        macd_divergence = res_tech.macd.divergence if res_tech.macd.divergence else "None"

        # 🌟 自动匹配真实合约地址
        actual_address = address_map.get(res_tech.token_symbol.upper(), "")

        row = (
            str(actual_address),
            str(res_tech.token_symbol),
            datetime.utcnow().replace(microsecond=0),

            float(res_vol.volume_24h), float(res_vol.volume_7d_avg), float(res_vol.ratio),
            int(res_vol.data_points_24h), int(res_mev.total_txs), int(res_mev.suspicious_txs), float(res_mev.toxicity_pct),

            float(res_tech.current_price), str(res_tech.ma.trend_type), str(res_tech.ma.alignment),
            str(res_tech.macd.dif_dea_position), str(res_tech.macd.histogram_trend), str(macd_divergence),
            float(res_tech.rsi.value), str(res_tech.rsi.zone), str(res_tech.bollinger.bandwidth),
            float(res_tech.bollinger.support), float(res_tech.bollinger.resistance), str(res_tech.kdj.zone),

            int(res_tech.tech_score), int(res_vol.score), int(res_mev.score), int(contract_score),
            int(composite_score), str(signal_color),
            datetime.utcnow().replace(microsecond=0)
        )

        client.execute(f"INSERT INTO {table_name} VALUES", [row])
        logger.info(f"✅ Successfully fused and saved full metrics for {token_symbol} (Score: {composite_score}, Vol: ${res_vol.volume_24h:,.0f})")

    except Exception as e:
        logger.error(f"❌ Failed to compute full metrics for {token_symbol}: {e}")

if __name__ == "__main__":
    client = get_clickhouse_client()
    logger.info("Starting batch processing for technical indicators...")
    try:
        # 🌟 提前捞取地址字典，常驻内存供跑批使用
        logger.info("Fetching token address map from clean_swaps...")
        addr_query = """
            SELECT DISTINCT upper(token_in_symbol), token_in_address 
            FROM signal_hub.clean_swaps WHERE token_in_address != ''
        """
        address_map = {row[0]: row[1] for row in client.execute(addr_query)}

        rows = client.execute("SELECT DISTINCT token_symbol FROM signal_hub.clean_klines")
        tokens_to_process = [row[0] for row in rows]
        logger.info(f"Found {len(tokens_to_process)} tokens to process in clean_klines.")

        for token in tokens_to_process:
            compute_and_save_tech_metrics(client, token, address_map)

        logger.info("Batch processing completed.")
    except Exception as e:
        logger.error(f"Batch job failed: {e}")