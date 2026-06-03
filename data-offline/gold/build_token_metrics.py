import logging
import pandas as pd
import numpy as np
from datetime import datetime
from config.database import get_clickhouse_client

logger = logging.getLogger('GoldTokenMetricsBuilder')

SUPPORTED_TOKENS = ["MNT", "ETH", "BTC", "SOL"]


def calc_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()


def calc_rsi(series: pd.Series, period: int = 14) -> float:
    """Calculate Relative Strength Index."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if not rsi.empty else 50.0


def build_hourly_token_metrics():
    """
    Aggregates Silver layer data (clean_swaps, clean_klines) into Gold layer token metrics.
    Replaces the dynamic queries previously executed in FastAPI data_layer.py.
    """
    client = get_clickhouse_client()
    calc_time = datetime.utcnow()
    records_to_insert = []

    for token in SUPPORTED_TOKENS:
        logger.info("Calculating gold metrics for %s", token)

        try:
            # 1. Volume Trend & MEV Toxicity (Aggregated directly via ClickHouse)
            # This SQL perfectly replicates the logic from the teammate's data_layer.py
            query_swaps = """
                WITH
                    -- 24h Volume and Tx Count
                    recent_24h AS (
                        SELECT 
                            sum(amount_usd) AS vol_24h,
                            count() AS tx_count_24h
                        FROM signal_hub.clean_swaps
                        WHERE block_timestamp >= now() - INTERVAL 24 HOUR
                          AND (token_in_symbol = %(token)s OR token_out_symbol = %(token)s)
                    ),
                    -- 7d Average Volume (excluding last 24h)
                    recent_7d AS (
                        SELECT 
                            sum(amount_usd) / 7 AS vol_7d_avg
                        FROM signal_hub.clean_swaps
                        WHERE block_timestamp >= now() - INTERVAL 8 DAY 
                          AND block_timestamp < now() - INTERVAL 1 DAY
                          AND (token_in_symbol = %(token)s OR token_out_symbol = %(token)s)
                    ),
                    -- MEV Suspicious Transactions (Sandwich attack pattern)
                    mev_stats AS (
                        SELECT 
                            count(DISTINCT tx_hash) AS total_txs,
                            countIf(buy_count > 0 AND sell_count > 0) AS suspicious_txs
                        FROM (
                            SELECT 
                                tx_hash,
                                trader_address,
                                countIf(token_out_symbol = %(token)s) AS buy_count,
                                countIf(token_in_symbol = %(token)s) AS sell_count
                            FROM signal_hub.clean_swaps
                            WHERE block_timestamp >= now() - INTERVAL 24 HOUR
                              AND (token_in_symbol = %(token)s OR token_out_symbol = %(token)s)
                            GROUP BY tx_hash, trader_address
                        )
                    )
                SELECT 
                    vol_24h, tx_count_24h, vol_7d_avg,
                    total_txs, suspicious_txs
                FROM recent_24h, recent_7d, mev_stats
            """

            swap_result = client.execute(query_swaps, {'token': token})
            if not swap_result:
                continue

            v24h, tx24h, v7d, mev_tot, mev_sus = swap_result[0]
            v24h = float(v24h or 0)
            v7d = float(v7d or 0)
            tx24h = int(tx24h or 0)
            mev_tot = int(mev_tot or 0)
            mev_sus = int(mev_sus or 0)

            vol_ratio = (v24h / v7d) if v7d > 0 else 0.0
            mev_pct = (mev_sus / mev_tot * 100) if mev_tot > 0 else 0.0

            # 2. Technical Indicators (Fetched from clean_klines, calculated via Pandas)
            query_klines = """
                SELECT close 
                FROM signal_hub.clean_klines 
                WHERE token_symbol = %(token)s AND interval = '1h'
                ORDER BY open_time ASC
                LIMIT 200
            """
            kline_result = client.execute(query_klines, {'token': token})

            if len(kline_result) < 100:
                logger.warning("Insufficient kline data for %s to calculate TA.", token)
                current_price, e7, e25, e99, rsi14 = 0.0, 0.0, 0.0, 0.0, 50.0
            else:
                df = pd.DataFrame(kline_result, columns=['close'])
                close_series = df['close']

                current_price = float(close_series.iloc[-1])
                e7 = float(calc_ema(close_series, 7).iloc[-1])
                e25 = float(calc_ema(close_series, 25).iloc[-1])
                e99 = float(calc_ema(close_series, 99).iloc[-1])
                rsi14 = calc_rsi(close_series, 14)

            # 3. Assemble Record
            record = (
                token, calc_time,
                v24h, v7d, vol_ratio, tx24h,
                mev_tot, mev_sus, mev_pct,
                current_price, e7, e25, e99, rsi14,
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
                    current_price, ema7, ema25, ema99, rsi_14, updated_at
                ) VALUES
            """, records_to_insert)
            logger.info("Successfully inserted gold token metrics for %d tokens.", len(records_to_insert))
        except Exception as e:
            logger.error("Failed to insert gold token metrics: %s", str(e))


if __name__ == "__main__":
    build_hourly_token_metrics()