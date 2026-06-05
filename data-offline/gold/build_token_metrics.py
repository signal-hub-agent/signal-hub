import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config.database import get_clickhouse_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GoldTokenMetricsBuilder')

SUPPORTED_TOKENS = ["MNT", "ETH", "BTC", "SOL"]

def calc_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def calc_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if not rsi.empty else 50.0

def calc_macd(series: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_hist = (dif - dea) * 2
    return dif, dea, macd_hist

def calc_bollinger_bands(series: pd.Series, window=20, num_std=2):
    rolling_mean = series.rolling(window=window).mean()
    rolling_std = series.rolling(window=window).std()
    upper_band = rolling_mean + (rolling_std * num_std)
    lower_band = rolling_mean - (rolling_std * num_std)
    return rolling_mean, upper_band, lower_band

def build_hourly_token_metrics():
    """
    Aggregates Silver layer data into Gold layer multi-dimensional token metrics.
    Calculates Volume trends, MEV Toxicity, and Technical Indicators (MA, MACD, RSI, BOLL).
    """
    client = get_clickhouse_client()
    calc_time = datetime.utcnow()
    records_to_insert = []

    for token in SUPPORTED_TOKENS:
        logger.info("Calculating gold metrics for %s", token)

        try:
            # 1. Volume Trend & MEV Toxicity
            query_swaps = """
                WITH
                    recent_24h AS (
                        SELECT sum(amount_usd) AS vol_24h, count() AS tx_count_24h
                        FROM signal_hub.clean_swaps
                        WHERE block_timestamp >= now() - INTERVAL 24 HOUR
                          AND (token_in_symbol = %(token)s OR token_out_symbol = %(token)s)
                    ),
                    recent_7d AS (
                        SELECT sum(amount_usd) / 7 AS vol_7d_avg
                        FROM signal_hub.clean_swaps
                        WHERE block_timestamp >= now() - INTERVAL 8 DAY 
                          AND block_timestamp < now() - INTERVAL 1 DAY
                          AND (token_in_symbol = %(token)s OR token_out_symbol = %(token)s)
                    ),
                    mev_stats AS (
                        SELECT count(DISTINCT tx_hash) AS total_txs, countIf(buy_count > 0 AND sell_count > 0) AS suspicious_txs
                        FROM (
                            SELECT tx_hash, trader_address,
                                countIf(token_out_symbol = %(token)s) AS buy_count,
                                countIf(token_in_symbol = %(token)s) AS sell_count
                            FROM signal_hub.clean_swaps
                            WHERE block_timestamp >= now() - INTERVAL 24 HOUR
                              AND (token_in_symbol = %(token)s OR token_out_symbol = %(token)s)
                            GROUP BY tx_hash, trader_address
                        )
                    )
                SELECT vol_24h, tx_count_24h, vol_7d_avg, total_txs, suspicious_txs
                FROM recent_24h, recent_7d, mev_stats
            """

            swap_result = client.execute(query_swaps, {'token': token})
            if not swap_result:
                logger.warning("No swap data found for %s, skipping metrics calculation.", token)
                continue

            v24h, tx24h, v7d, mev_tot, mev_sus = swap_result[0]
            v24h = float(v24h or 0)
            v7d = float(v7d or 0)
            tx24h = int(tx24h or 0)
            mev_tot = int(mev_tot or 0)
            mev_sus = int(mev_sus or 0)

            vol_ratio = (v24h / v7d) if v7d > 0 else 0.0
            mev_pct = (mev_sus / mev_tot * 100) if mev_tot > 0 else 0.0

            # 2. Technical Indicators
            query_klines = """
                SELECT close 
                FROM signal_hub.clean_klines 
                WHERE token_symbol = %(token)s AND interval = '1h'
                ORDER BY open_time ASC
                LIMIT 200
            """
            kline_result = client.execute(query_klines, {'token': token})

            if len(kline_result) < 100:
                logger.warning("Insufficient kline data for %s to calculate TA properly.", token)
                current_price, e7, e25, e99, rsi14 = 0.0, 0.0, 0.0, 0.0, 50.0
                current_dif, current_dea, current_macd_hist = 0.0, 0.0, 0.0
                current_boll_mid, current_boll_up, current_boll_low = 0.0, 0.0, 0.0
            else:
                df = pd.DataFrame(kline_result, columns=['close'])
                close_series = df['close']

                current_price = float(close_series.iloc[-1])
                e7 = float(calc_ema(close_series, 7).iloc[-1])
                e25 = float(calc_ema(close_series, 25).iloc[-1])
                e99 = float(calc_ema(close_series, 99).iloc[-1])
                rsi14 = calc_rsi(close_series, 14)

                dif, dea, macd_hist = calc_macd(close_series)
                boll_mid, boll_up, boll_low = calc_bollinger_bands(close_series)

                current_dif = float(dif.iloc[-1])
                current_dea = float(dea.iloc[-1])
                current_macd_hist = float(macd_hist.iloc[-1])
                current_boll_mid = float(boll_mid.iloc[-1])
                current_boll_up = float(boll_up.iloc[-1])
                current_boll_low = float(boll_low.iloc[-1])

            # 3. Assemble Record aligned with the new DDL
            record = (
                token, calc_time,
                v24h, v7d, vol_ratio, tx24h,
                mev_tot, mev_sus, mev_pct,
                current_price, e7, e25, e99, rsi14,
                current_dif, current_dea, current_macd_hist,
                current_boll_mid, current_boll_up, current_boll_low,
                calc_time
            )
            records_to_insert.append(record)

        except Exception as e:
            logger.error("Failed to build metrics for %s: %s", token, str(e), exc_info=True)

    if records_to_insert:
        try:
            client.execute("""
                INSERT INTO signal_hub.gold_token_metrics_1h (
                    token_symbol, calc_time, 
                    volume_24h_usd, volume_7d_avg_usd, volume_ratio, tx_count_24h,
                    mev_total_txs, mev_suspicious_txs, mev_toxicity_pct,
                    current_price, ema7, ema25, ema99, rsi_14,
                    macd_dif, macd_dea, macd_hist,
                    boll_mid, boll_up, boll_low,
                    updated_at
                ) VALUES
            """, records_to_insert)
            logger.info("Successfully inserted gold token metrics for %d tokens.", len(records_to_insert))
        except Exception as e:
            logger.error("Failed to insert gold token metrics: %s", str(e))

if __name__ == "__main__":
    build_hourly_token_metrics()