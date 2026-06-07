"""
技术指标计算模块
包含：EMA、MACD、RSI、布林带、KDJ、SFI
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional
from kline_service import fetch_klines


# ─────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────

@dataclass
class EMAResult:
    ema7: float
    ema25: float
    ema99: float
    alignment: str
    trend_type: str   # bullish_trending / bearish_trending / range_bound / compressing
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
    cross_type: Optional[str] = None  # 'golden' / 'dead' / None


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
    touch_edge: Optional[str]
    pattern: str
    support: float
    resistance: float


@dataclass
class KDJResult:
    k: float
    d: float
    j: float
    zone: str
    cross: Optional[str]
    divergence: Optional[str]
    is_blunting: bool


@dataclass
class SFIResult:
    value: Optional[float]
    trend: Optional[str]
    signal: str


@dataclass
class TechnicalAnalysisResult:
    token: str
    current_price: float
    ma: EMAResult
    macd: MACDResult
    rsi: RSIResult
    bollinger: BollingerResult
    kdj: KDJResult
    sfi: SFIResult
    tech_score: int
    raw_score: float
    is_real_data: bool = True
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "token": self.token,
            "current_price": self.current_price,
            "tech_score": self.tech_score,
            "is_real_data": self.is_real_data,
            "ma": {
                "ema7": round(self.ma.ema7, 4),
                "ema25": round(self.ma.ema25, 4),
                "ema99": round(self.ma.ema99, 4),
                "alignment": self.ma.alignment,
                "trend_type": self.ma.trend_type,
                "ema99_support": self.ma.ema99_support,
                "compression": self.ma.compression,
            },
            "macd": {
                "dif": round(self.macd.dif, 4),
                "dea": round(self.macd.dea, 4),
                "histogram": round(self.macd.histogram, 4),
                "dif_dea_position": self.macd.dif_dea_position,
                "histogram_trend": self.macd.histogram_trend,
                "divergence": self.macd.divergence,
                "comment": self.macd.comment,
                "bars_since_cross": self.macd.bars_since_cross,
                "cross_type": self.macd.cross_type,
            },
            "rsi": {
                "value": round(self.rsi.value, 1),
                "zone": self.rsi.zone,
                "divergence": self.rsi.divergence,
            },
            "bollinger": {
                "upper": round(self.bollinger.upper, 4),
                "mid": round(self.bollinger.mid, 4),
                "lower": round(self.bollinger.lower, 4),
                "bandwidth": self.bollinger.bandwidth,
                "mid_band_direction": self.bollinger.mid_band_direction,
                "price_position": self.bollinger.price_position,
                "touch_edge": self.bollinger.touch_edge,
                "pattern": self.bollinger.pattern,
                "support": round(self.bollinger.support, 4),
                "resistance": round(self.bollinger.resistance, 4),
            },
            "kdj": {
                "k": round(self.kdj.k, 1),
                "d": round(self.kdj.d, 1),
                "j": round(self.kdj.j, 1),
                "zone": self.kdj.zone,
                "cross": self.kdj.cross,
                "divergence": self.kdj.divergence,
                "is_blunting": self.kdj.is_blunting,
            },
            "sfi": {
                "value": self.sfi.value,
                "trend": self.sfi.trend,
                "signal": self.sfi.signal,
            },
        }


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────

def calc_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def calc_slope_pct(series: pd.Series, n: int = 5) -> float:
    if len(series) < n:
        return 0.0
    start = series.iloc[-n]
    end = series.iloc[-1]
    if start == 0:
        return 0.0
    return (end - start) / start * 100


def find_peaks(series: pd.Series, min_gap: int = 5):
    peaks = []
    for i in range(1, len(series) - 1):
        if series.iloc[i] > series.iloc[i-1] and series.iloc[i] > series.iloc[i+1]:
            if not peaks or i - peaks[-1] >= min_gap:
                peaks.append(i)
    return peaks


def find_troughs(series: pd.Series, min_gap: int = 5):
    troughs = []
    for i in range(1, len(series) - 1):
        if series.iloc[i] < series.iloc[i-1] and series.iloc[i] < series.iloc[i+1]:
            if not troughs or i - troughs[-1] >= min_gap:
                troughs.append(i)
    return troughs


# ─────────────────────────────────────────────
# 1. EMA 均线 — 修复：空头排列不再误判为compressing
# ─────────────────────────────────────────────

def analyze_ema(df: pd.DataFrame) -> EMAResult:
    close = df["close"]
    ema7  = calc_ema(close, 7)
    ema25 = calc_ema(close, 25)
    ema99 = calc_ema(close, 99)

    e7   = float(ema7.iloc[-1])
    e25  = float(ema25.iloc[-1])
    e99  = float(ema99.iloc[-1])
    price = float(close.iloc[-1])

    slope7  = calc_slope_pct(ema7)
    slope25 = calc_slope_pct(ema25)
    slope99 = calc_slope_pct(ema99)

    # 排列字符串
    vals = sorted([("EMA7", e7), ("EMA25", e25), ("EMA99", e99)],
                  key=lambda x: x[1], reverse=True)
    alignment = " > ".join(v[0] for v in vals)

    # 三条均线的两两间距（占价格比例）
    gap_7_25  = abs(e7 - e25)  / price if price > 0 else 1
    gap_25_99 = abs(e25 - e99) / price if price > 0 else 1
    gap_7_99  = abs(e7 - e99)  / price if price > 0 else 1

    # 均线粘合：三条线全部高度接近，最大间距 < 价格0.3%
    # 注意：必须三条线都紧密缠绕才算粘合，间距超过0.3%的排列是真实趋势
    compression = gap_7_25 < 0.003 and gap_25_99 < 0.003

    # 近5根K线内EMA7/EMA25交叉次数
    cross_count = 0
    for i in range(-5, 0):
        if i - 1 >= -len(ema7):
            prev_diff = float(ema7.iloc[i-1]) - float(ema25.iloc[i-1])
            curr_diff = float(ema7.iloc[i]) - float(ema25.iloc[i])
            if prev_diff * curr_diff < 0:
                cross_count += 1

    # 趋势判定：
    # 关键修复：空头排列（EMA7 < EMA25 < EMA99）间距 > 0.3% → bearish_trending
    # 多头排列（EMA7 > EMA25 > EMA99）间距 > 0.3% → bullish_trending
    # 粘合优先级最低，只有三线都极度接近才判compressing
    if e7 > e25 > e99 and gap_7_99 > 0.003:
        # 多头排列，斜率不作硬性要求（排列本身已说明趋势）
        trend_type = "bullish_trending"
    elif e7 < e25 < e99 and gap_7_99 > 0.003:
        # 空头排列，间距超过0.3%即判空头，不要求斜率
        trend_type = "bearish_trending"
    elif compression:
        trend_type = "compressing"
    elif cross_count >= 2:
        trend_type = "range_bound"
    else:
        trend_type = "range_bound"

    # EMA99支撑检测
    ema99_support = False
    if price >= e99 and slope99 >= 0:
        recent_low = float(df["low"].iloc[-3:].min())
        if recent_low <= e99 * 1.01:
            ema99_support = True

    return EMAResult(
        ema7=e7, ema25=e25, ema99=e99,
        alignment=alignment,
        trend_type=trend_type,
        ema99_support=ema99_support,
        compression=compression,
    )


# ─────────────────────────────────────────────
# 2. MACD（4h K线）— 修复：position更精确，背离有完整comment
# ─────────────────────────────────────────────

def analyze_macd(df_4h: pd.DataFrame) -> MACDResult:
    close = df_4h["close"]
    ema12 = calc_ema(close, 12)
    ema26 = calc_ema(close, 26)
    dif   = ema12 - ema26
    dea   = calc_ema(dif, 9)
    hist  = 2 * (dif - dea)

    dif_val  = float(dif.iloc[-1])
    dea_val  = float(dea.iloc[-1])
    hist_val = float(hist.iloc[-1])

    # 检测最近发生金叉/死叉的位置（找最近一次）
    # bars_since_cross 表示距离上次穿越发生了多少根K线
    bars_since_golden = None
    bars_since_dead   = None
    for i in range(-1, -min(len(dif), 30), -1):
        if i - 1 < -len(dif):
            break
        pd_ = float(dif.iloc[i-1])
        cd_ = float(dif.iloc[i])
        pe_ = float(dea.iloc[i-1])
        ce_ = float(dea.iloc[i])
        if pd_ < pe_ and cd_ > ce_ and bars_since_golden is None:
            bars_since_golden = abs(i) - 1   # 距今多少根K线
        if pd_ > pe_ and cd_ < ce_ and bars_since_dead is None:
            bars_since_dead = abs(i) - 1
        if bars_since_golden is not None and bars_since_dead is not None:
            break

    # 判断当前状态——金叉死叉哪个更近就用哪个作为基准状态
    # 区分三档：刚发生(0-2根) / 早期持续(3-10根) / 持续状态(>10根)
    cross_recent = None  # 'golden' / 'dead' / None
    bars_since_cross = None
    if bars_since_golden is not None and bars_since_dead is not None:
        if bars_since_golden < bars_since_dead:
            cross_recent = "golden"
            bars_since_cross = bars_since_golden
        else:
            cross_recent = "dead"
            bars_since_cross = bars_since_dead
    elif bars_since_golden is not None:
        cross_recent = "golden"
        bars_since_cross = bars_since_golden
    elif bars_since_dead is not None:
        cross_recent = "dead"
        bars_since_cross = bars_since_dead

    # position：金叉/死叉发生在最近3根K线内 → 标记为"刚发生"
    just_crossed = bars_since_cross is not None and bars_since_cross <= 2

    if just_crossed and cross_recent == "golden":
        position = "zero_above_golden_cross" if dif_val > 0 else "zero_below_golden_cross"
    elif just_crossed and cross_recent == "dead":
        position = "zero_above_dead_cross" if dif_val > 0 else "zero_below_dead_cross"
    else:
        # 持续状态（最近3根内无交叉）
        if dif_val > dea_val and dif_val > 0:
            position = "zero_above_bullish"
        elif dif_val > dea_val and dif_val <= 0:
            position = "zero_below_bullish"
        elif dif_val < dea_val and dif_val > 0:
            position = "zero_above_bearish"
        else:
            position = "zero_below_bearish"

    # 柱能趋势（近3根）
    h3 = [float(hist.iloc[i]) for i in [-3, -2, -1]]
    if all(h > 0 for h in h3):
        if h3[2] > h3[1] > h3[0]:
            hist_trend = "red_expanding"
        elif h3[2] < h3[1] < h3[0]:
            hist_trend = "red_shrinking"
        else:
            hist_trend = "red_stable"
    elif all(h < 0 for h in h3):
        if abs(h3[2]) > abs(h3[1]) > abs(h3[0]):
            hist_trend = "green_expanding"
        elif abs(h3[2]) < abs(h3[1]) < abs(h3[0]):
            hist_trend = "green_shrinking"
        else:
            hist_trend = "green_stable"
    else:
        hist_trend = "neutral"

    # 背离检测
    divergence = None
    lookback = 20
    if len(close) >= lookback:
        price_new_high = float(close.iloc[-1]) >= float(close.iloc[-lookback:].max()) * 0.99
        dif_peaks = find_peaks(dif, min_gap=5)
        if price_new_high and len(dif_peaks) >= 2:
            if float(dif.iloc[dif_peaks[-1]]) < float(dif.iloc[dif_peaks[-2]]):
                divergence = "bearish_divergence"

        if divergence is None:
            price_new_low = float(close.iloc[-1]) <= float(close.iloc[-lookback:].min()) * 1.01
            dif_troughs = find_troughs(dif, min_gap=5)
            if price_new_low and len(dif_troughs) >= 2:
                if float(dif.iloc[dif_troughs[-1]]) > float(dif.iloc[dif_troughs[-2]]):
                    divergence = "bullish_divergence"

    # comment — 体现"刚金叉"和"已金叉一阵了"的区别
    comment_parts = []
    if just_crossed:
        if "golden_cross" in position:
            label = "Above-zero golden cross just formed" if "above" in position else "Below-zero golden cross just formed"
            comment_parts.append(label)
        elif "dead_cross" in position:
            label = "Above-zero dead cross just formed" if "above" in position else "Below-zero dead cross just formed"
            comment_parts.append(label)
    elif cross_recent == "golden" and bars_since_cross is not None and bars_since_cross <= 10:
        # 早期持续：金叉发生后3-10根K线内
        loc = "above-zero" if dif_val > 0 else "below-zero"
        comment_parts.append(f"Bullish stance held {bars_since_cross} bars after {loc} golden cross")
    elif cross_recent == "dead" and bars_since_cross is not None and bars_since_cross <= 10:
        loc = "above-zero" if dif_val > 0 else "below-zero"
        comment_parts.append(f"Bearish stance held {bars_since_cross} bars after {loc} dead cross")
    else:
        # 长期持续（>10根）或无近期交叉
        loc = "above" if dif_val > 0 else "below"
        if "bullish" in position:
            comment_parts.append(f"Sustained bullish posture {loc} zero line")
        elif "bearish" in position:
            comment_parts.append(f"Sustained bearish posture {loc} zero line")

    trend_desc = {
        "red_expanding":  "histogram expanding, bullish momentum strengthening",
        "red_shrinking":  "histogram shrinking, bullish momentum fading",
        "red_stable":     "histogram stable, mild bullish",
        "green_expanding":"histogram expanding bearish, selling pressure intensifying",
        "green_shrinking":"histogram shrinking bearish, selling pressure easing",
        "green_stable":   "histogram stable, mild bearish",
        "neutral":        "histogram mixed",
    }.get(hist_trend, "")
    if trend_desc:
        comment_parts.append(trend_desc)

    if divergence == "bearish_divergence":
        comment_parts.append(
            "⚠️ 4H bearish divergence confirmed: price made new high but DIF did not — reversal risk elevated"
        )
    elif divergence == "bullish_divergence":
        comment_parts.append(
            "✅ 4H bullish divergence confirmed: price made new low but DIF did not — potential reversal upward"
        )

    comment = "; ".join(comment_parts) if comment_parts else "No significant MACD signal"

    return MACDResult(
        dif=dif_val, dea=dea_val, histogram=hist_val,
        dif_dea_position=position,
        histogram_trend=hist_trend,
        divergence=divergence,
        comment=comment,
        bars_since_cross=bars_since_cross,
        cross_type=cross_recent,
    )


# ─────────────────────────────────────────────
# 3. RSI（1h K线）
# ─────────────────────────────────────────────

def analyze_rsi(df: pd.DataFrame) -> RSIResult:
    close = df["close"]
    period = 14
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period-1, adjust=False).mean()
    avg_loss = loss.ewm(com=period-1, adjust=False).mean()
    rs  = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    rsi_val = float(rsi.iloc[-1])

    if rsi_val >= 80:
        zone = "overbought_severe"
    elif rsi_val >= 70:
        zone = "overbought"
    elif rsi_val > 55:
        zone = "bullish"
    elif rsi_val >= 45:
        zone = "neutral"
    elif rsi_val >= 30:
        zone = "bearish"
    else:
        zone = "oversold"

    divergence = None
    lookback = 20
    if len(close) >= lookback:
        price_new_high = float(close.iloc[-1]) >= float(close.iloc[-lookback:].max()) * 0.99
        rsi_peaks = find_peaks(rsi, min_gap=5)
        if price_new_high and len(rsi_peaks) >= 2:
            if float(rsi.iloc[rsi_peaks[-1]]) < float(rsi.iloc[rsi_peaks[-2]]):
                divergence = "bearish"

        if divergence is None:
            price_new_low = float(close.iloc[-1]) <= float(close.iloc[-lookback:].min()) * 1.01
            rsi_troughs = find_troughs(rsi, min_gap=5)
            if price_new_low and len(rsi_troughs) >= 2:
                if float(rsi.iloc[rsi_troughs[-1]]) > float(rsi.iloc[rsi_troughs[-2]]):
                    divergence = "bullish"

    return RSIResult(value=rsi_val, zone=zone, divergence=divergence)


# ─────────────────────────────────────────────
# 4. 布林带（1h K线）— 补全所有字段
# ─────────────────────────────────────────────

def analyze_bollinger(df: pd.DataFrame, macd_result: MACDResult) -> BollingerResult:
    close = df["close"]
    period = 20
    price = float(close.iloc[-1])

    mid   = close.rolling(period).mean()
    std   = close.rolling(period).std()
    upper = mid + 2 * std
    lower = mid - 2 * std
    bw    = (upper - lower) / mid * 100

    mid_val   = float(mid.iloc[-1])
    upper_val = float(upper.iloc[-1])
    lower_val = float(lower.iloc[-1])
    bw_val    = float(bw.iloc[-1])
    bw_avg    = float(bw.iloc[-20:].mean())

    # 带宽状态
    if bw_val > bw_avg * 1.2:
        bandwidth = "expanding"
    elif bw_val < bw_avg * 0.8:
        bandwidth = "contracting"
    else:
        bandwidth = "stable"

    # 中轨方向（近5根K线斜率）
    slope = calc_slope_pct(mid, 5)
    if slope > 0.1:
        mid_direction = "rising"
    elif slope < -0.1:
        mid_direction = "falling"
    else:
        mid_direction = "flat"

    # 价格与中轨关系（近3根K线）
    recent_closes = df["close"].iloc[-3:].values
    recent_mids   = mid.iloc[-3:].values
    if all(c > m for c, m in zip(recent_closes, recent_mids)):
        price_position = "above_mid"
    elif all(c < m for c, m in zip(recent_closes, recent_mids)):
        price_position = "below_mid"
    else:
        price_position = "crossing_mid"

    # 边轨触碰
    touch_edge = None
    if price >= upper_val * 0.99:
        touch_edge = "upper"
    elif price <= lower_val * 1.01:
        touch_edge = "lower"

    # 形态判定 — 按优先级从强到弱判断
    bearish_div = macd_result.divergence == "bearish_divergence"
    bullish_div = macd_result.divergence == "bullish_divergence"

    # 强信号：触边+收缩+背离 → 可能见顶/底
    if touch_edge == "upper" and bandwidth == "contracting" and bearish_div:
        pattern = "potential_top"
    elif touch_edge == "lower" and bandwidth == "contracting" and bullish_div:
        pattern = "potential_bottom"
    # 通道形态：价格位置+中轨方向 一致
    elif price_position == "above_mid" and mid_direction == "rising":
        pattern = "bullish_channel"
    elif price_position == "below_mid" and mid_direction == "falling":
        pattern = "bearish_channel"
    # 边缘形态：触下轨+中轨下行（即使price_position是crossing_mid）→ 偏空通道
    # SOL这种情况会命中此处：mid下行+触下轨，说明仍处在下跌通道末端
    elif touch_edge == "lower" and mid_direction == "falling":
        pattern = "bearish_channel"
    elif touch_edge == "upper" and mid_direction == "rising":
        pattern = "bullish_channel"
    # 震荡：穿越中轨+中轨走平
    elif price_position == "crossing_mid" and mid_direction == "flat":
        pattern = "range_contraction"
    # 偏弱/偏强震荡（中轨走平但价格偏向一边）
    elif price_position == "below_mid" and mid_direction == "flat":
        pattern = "bearish_channel"
    elif price_position == "above_mid" and mid_direction == "flat":
        pattern = "bullish_channel"
    else:
        pattern = "neutral"

    # 支撑阻力
    if pattern == "bullish_channel":
        support, resistance = mid_val, upper_val
    elif pattern == "bearish_channel":
        support, resistance = lower_val, mid_val
    else:
        support, resistance = lower_val, upper_val

    return BollingerResult(
        upper=upper_val, mid=mid_val, lower=lower_val,
        bandwidth=bandwidth,
        mid_band_direction=mid_direction,
        price_position=price_position,
        touch_edge=touch_edge,
        pattern=pattern,
        support=support,
        resistance=resistance,
    )


# ─────────────────────────────────────────────
# 5. KDJ（1h K线）
# ─────────────────────────────────────────────

def analyze_kdj(df: pd.DataFrame, trend_type: str) -> KDJResult:
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    period = 9

    low_min  = low.rolling(period).min()
    high_max = high.rolling(period).max()
    denom    = (high_max - low_min).replace(0, np.nan)
    rsv = (close - low_min) / denom * 100

    K = rsv.ewm(com=2, adjust=False).mean()
    D = K.ewm(com=2, adjust=False).mean()
    J = 3 * K - 2 * D

    k_val = float(K.iloc[-1])
    d_val = float(D.iloc[-1])
    j_val = float(J.iloc[-1])

    if k_val > 80 and d_val > 80:
        zone = "overbought"
    elif k_val < 20 and d_val < 20:
        zone = "oversold"
    else:
        zone = "normal"

    prev_k = float(K.iloc[-2])
    prev_d = float(D.iloc[-2])
    cross = None
    if prev_k < prev_d and k_val > d_val and zone == "oversold":
        cross = "low_golden_cross"
    elif prev_k > prev_d and k_val < d_val and zone == "overbought":
        cross = "high_dead_cross"

    is_blunting = (
        (zone == "overbought" and trend_type == "bullish_trending") or
        (zone == "oversold"   and trend_type == "bearish_trending")
    )

    divergence = None
    lookback = 20
    if len(close) >= lookback:
        price_new_low = float(close.iloc[-1]) <= float(close.iloc[-lookback:].min()) * 1.01
        k_troughs = find_troughs(K, min_gap=5)
        if price_new_low and len(k_troughs) >= 2:
            if float(K.iloc[k_troughs[-1]]) > float(K.iloc[k_troughs[-2]]):
                divergence = "bullish_divergence"

        if divergence is None:
            price_new_high = float(close.iloc[-1]) >= float(close.iloc[-lookback:].max()) * 0.99
            k_peaks = find_peaks(K, min_gap=5)
            if price_new_high and len(k_peaks) >= 2:
                if float(K.iloc[k_peaks[-1]]) < float(K.iloc[k_peaks[-2]]):
                    divergence = "bearish_divergence"

    return KDJResult(
        k=k_val, d=d_val, j=j_val,
        zone=zone, cross=cross,
        divergence=divergence,
        is_blunting=is_blunting,
    )


# ─────────────────────────────────────────────
# 6. SFI — 预留接口
# ─────────────────────────────────────────────

def analyze_sfi(token_symbol: str, smart_addresses: list = None) -> SFIResult:
    return SFIResult(value=None, trend=None, signal="Insufficient data")


# ─────────────────────────────────────────────
# 7. 综合加权评分 — 修复底背离加分
# ─────────────────────────────────────────────

def calc_tech_score(
    ma: EMAResult,
    macd: MACDResult,
    rsi: RSIResult,
    bollinger: BollingerResult,
    kdj: KDJResult,
    sfi: SFIResult,
) -> tuple:
    raw = 0.0

    # MA（-2 到 +2）
    ma_score = {
        "bullish_trending": 2,
        "bearish_trending": -2,
        "range_bound": 0,
        "compressing": 0,
    }.get(ma.trend_type, 0)
    raw += ma_score

    # MACD 位置+柱能（-2 到 +2）
    bullish_positions = ("zero_above_golden_cross", "zero_below_golden_cross",
                         "zero_above_bullish", "zero_below_bullish")
    bearish_positions = ("zero_above_dead_cross", "zero_below_dead_cross",
                         "zero_above_bearish", "zero_below_bearish")

    if macd.dif_dea_position in bullish_positions:
        if macd.histogram_trend == "red_expanding":
            raw += 2
        else:
            raw += 1
    elif macd.dif_dea_position in bearish_positions:
        if macd.histogram_trend == "green_expanding":
            raw -= 2
        else:
            raw -= 1

    # MACD 背离（顶背离-2，底背离+2.5）
    if macd.divergence == "bullish_divergence":
        raw += 2.5
    elif macd.divergence == "bearish_divergence":
        raw -= 2

    # RSI（-1 到 +1，背离额外±1.5）
    rsi_score = {
        "bullish": 1, "oversold": 1,
        "overbought": -1, "overbought_severe": -1, "bearish": -1,
        "neutral": 0,
    }.get(rsi.zone, 0)
    raw += rsi_score
    if rsi.divergence == "bullish":
        raw += 1.5
    elif rsi.divergence == "bearish":
        raw -= 1.5

    # 布林带（-1.5 到 +1.5）
    bb_score = {
        "bullish_channel":    1.5,
        "potential_bottom":   1.0,
        "neutral":            0,
        "range_contraction":  0,
        "potential_top":     -1.0,
        "bearish_channel":   -1.5,
    }.get(bollinger.pattern, 0)
    raw += bb_score

    # KDJ（钝化时权重为0）
    if not kdj.is_blunting:
        if kdj.cross == "low_golden_cross":
            raw += 1
        elif kdj.cross == "high_dead_cross":
            raw -= 1

    # SFI（null时跳过）
    if sfi.value is not None:
        if sfi.value >= 30 and sfi.trend == "rising":
            raw += 1.5
        elif sfi.value <= -30 and sfi.trend == "falling":
            raw -= 1.5

    # 映射到 0-30：(raw + 10) / 20 × 30
    normalized = (raw + 10) / 20
    normalized = max(0.0, min(1.0, normalized))
    score = round(normalized * 30)

    return raw, score


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def run_technical_analysis(token_symbol: str) -> TechnicalAnalysisResult:
    try:
        df_1h = fetch_klines(token_symbol, interval="1h", limit=200)
        df_4h = fetch_klines(token_symbol, interval="4h", limit=100)
    except Exception as e:
        return TechnicalAnalysisResult(
            token=token_symbol, current_price=0,
            ma=EMAResult(0,0,0,"N/A","range_bound",False,False),
            macd=MACDResult(0,0,0,"neutral","neutral",None,"Data unavailable"),
            rsi=RSIResult(50,"neutral",None),
            bollinger=BollingerResult(0,0,0,"stable","flat","crossing_mid",None,"neutral",0,0),
            kdj=KDJResult(50,50,50,"normal",None,None,False),
            sfi=SFIResult(None,None,"Insufficient data"),
            tech_score=15, raw_score=0,
            is_real_data=False, error=str(e),
        )

    current_price = float(df_1h["close"].iloc[-1])
    ma        = analyze_ema(df_1h)
    macd      = analyze_macd(df_4h)
    rsi       = analyze_rsi(df_1h)
    bollinger = analyze_bollinger(df_1h, macd)
    kdj       = analyze_kdj(df_1h, ma.trend_type)
    sfi       = analyze_sfi(token_symbol)

    raw_score, tech_score = calc_tech_score(ma, macd, rsi, bollinger, kdj, sfi)

    return TechnicalAnalysisResult(
        token=token_symbol, current_price=current_price,
        ma=ma, macd=macd, rsi=rsi,
        bollinger=bollinger, kdj=kdj, sfi=sfi,
        tech_score=tech_score, raw_score=raw_score,
        is_real_data=True,
    )


# ─────────────────────────────────────────────
# 测试入口 — 输出完整JSON + 终端摘要
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import json

    for token in ["ETH", "BTC", "SOL"]:
        print(f"\n{'='*55}")
        print(f"  {token} Technical Analysis")
        print(f"{'='*55}")

        result = run_technical_analysis(token)

        # 完整JSON（传给LLM用）
        full_json = result.to_dict()
        print("\n[Full JSON for LLM]")
        print(json.dumps(full_json, indent=2))

        # 终端摘要
        print(f"\n[Summary]")
        print(f"Price:        ${result.current_price:,.4f}")
        print(f"Tech Score:   {result.tech_score}/30  (raw={result.raw_score:.2f})")
        print(f"MA Trend:     {result.ma.trend_type}  ({result.ma.alignment})")
        print(f"MACD:         {result.macd.dif_dea_position} | {result.macd.histogram_trend}")
        print(f"              {result.macd.comment}")
        print(f"RSI:          {result.rsi.value:.1f} → {result.rsi.zone}"
              + (f" | divergence: {result.rsi.divergence}" if result.rsi.divergence else ""))
        print(f"Bollinger:    pattern={result.bollinger.pattern}")
        print(f"              mid_direction={result.bollinger.mid_band_direction}"
              f" | price_position={result.bollinger.price_position}"
              f" | bandwidth={result.bollinger.bandwidth}"
              + (f" | touch={result.bollinger.touch_edge}" if result.bollinger.touch_edge else ""))
        print(f"              Support={result.bollinger.support:.4f}"
              f"  Resistance={result.bollinger.resistance:.4f}")
        print(f"KDJ:          K={result.kdj.k:.1f} D={result.kdj.d:.1f} J={result.kdj.j:.1f}"
              f" | {result.kdj.zone}"
              + (f" | {result.kdj.cross}" if result.kdj.cross else "")
              + (" | blunting" if result.kdj.is_blunting else ""))
        print(f"SFI:          {result.sfi.signal}")