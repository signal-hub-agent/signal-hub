"""
离线调度任务：代币技术面特征计算
包含：EMA、MACD、RSI、布林带、KDJ、SFI，并落盘至 ClickHouse
每 15 分钟运行一次。
"""
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

# 依赖离线的抓取工具和数据库连接
from utils.kline_fetcher import fetch_klines
import clickhouse_connect

logger = logging.getLogger(__name__)

# ================= 保持原状的内部结构 (供 Pandas 使用) =================
@dataclass
class EMAResult:
    ema7: float
    ema25: float
    ema99: float
    alignment: str
    trend_type: str
    ema99_support: bool
    compression: bool

@dataclass
class MACDResult:
    dif: float
    dea: float
    histogram: float
    dif_dea_position: str
    histogram_trend: str
    divergence: Optional[str]
    comment: str
    bars_since_cross: Optional[int] = None
    cross_type: Optional[str] = None

@dataclass
class RSIResult:
    value: float
    zone: str
    divergence: Optional[str]

@dataclass
class BollingerResult:
    upper: float
    mid: float
    lower: float
    bandwidth: str
    mid_band_direction: str
    price_position: str
    support: float
    resistance: float
    touch_edge: Optional[str]

@dataclass
class KDJResult:
    k: float
    d: float
    j: float
    zone: str
    cross_type: Optional[str]

@dataclass
class SFIResult:
    value: Optional[float]
    comment: str

@dataclass
class TechnicalAnalysisResult:
    token_symbol: str
    current_price: float
    raw_score: float
    tech_score: int
    ma: EMAResult
    macd: MACDResult
    rsi: RSIResult
    bollinger: BollingerResult
    kdj: KDJResult
    sfi: SFIResult

# ================= 保持原状的金融量化核心算法 =================
def calculate_ema(df: pd.DataFrame) -> EMAResult:
    df['ema7'] = df['close'].ewm(span=7, adjust=False).mean()
    df['ema25'] = df['close'].ewm(span=25, adjust=False).mean()
    df['ema99'] = df['close'].ewm(span=99, adjust=False).mean()
    last = df.iloc[-1]

    e7, e25, e99 = last['ema7'], last['ema25'], last['ema99']
    price = last['close']

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

    last = df.iloc[-1]
    prev = df.iloc[-2]
    dif, dea, hist = last['macd_dif'], last['macd_dea'], last['macd_hist']

    if dif > 0 and dea > 0: pos = "zero_above_golden_cross" if dif > dea else "zero_above_bearish"
    elif dif < 0 and dea < 0: pos = "zero_below_golden_cross" if dif > dea else "zero_below_bearish"
    else: pos = "zero_cross_boundary"

    if hist > 0 and hist > prev['macd_hist']: hist_trend = "green_expanding"
    elif hist > 0: hist_trend = "green_shrinking"
    elif hist < 0 and hist < prev['macd_hist']: hist_trend = "red_expanding"
    else: hist_trend = "red_shrinking"

    return MACDResult(dif, dea, hist, pos, hist_trend, None, f"DIF: {dif:.4f}, Hist: {hist:.4f}")

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
    return KDJResult(last['k'], last['d'], last['j'], zone, None)

def run_technical_analysis(token_symbol: str) -> TechnicalAnalysisResult:
    """原有逻辑不变，计算并返回结果对象"""
    df = fetch_klines(token_symbol, interval="1h", limit=200)
    ema = calculate_ema(df)
    macd = calculate_macd(df)
    rsi = calculate_rsi(df)
    boll = calculate_bollinger(df)
    kdj = calculate_kdj(df)

    # 原版打分逻辑
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

# ================= 新增：落盘至 ClickHouse =================
def compute_and_save_tech_metrics(token_symbol: str):
    """
    抓取、计算并将技术指标持久化到 ClickHouse
    """
    try:
        res = run_technical_analysis(token_symbol)

        # 为了兼容线上环境配置
        client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='123456')

        insert_query = """
            INSERT INTO signal_hub.gold_token_metrics_1h 
            (token_symbol, calc_time, current_price, tech_score, ma_trend_type, ma_alignment,
             macd_position, macd_histogram_trend, rsi_value, rsi_zone, bollinger_pattern, 
             bollinger_support, bollinger_resistance, kdj_zone)
            VALUES
        """
        row = (
            res.token_symbol, datetime.utcnow(), res.current_price, res.tech_score,
            res.ma.trend_type, res.ma.alignment, res.macd.dif_dea_position,
            res.macd.histogram_trend, res.rsi.value, res.rsi.zone, res.bollinger.pattern,
            res.bollinger.support, res.bollinger.resistance, res.kdj.zone
        )
        client.insert(insert_query, [row])
        logger.info(f"Successfully saved technical metrics for {token_symbol}")

    except Exception as e:
        logger.error(f"Failed to compute tech metrics for {token_symbol}: {e}")

if __name__ == "__main__":
    # 在 Airflow/Cron 中被调度执行时，遍历计算
    for test_token in ["MNT", "MOE"]:
        compute_and_save_tech_metrics(test_token)