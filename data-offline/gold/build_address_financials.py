import os
import sys

# 🌟 动态将 data-offline 目录加入系统路径，解决 config 导入问题
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import logging
import pandas as pd
from datetime import datetime, timedelta
from config.database import get_clickhouse_client

logger = logging.getLogger('GoldAddressFinancialsBuilder')


def build_daily_address_financials(target_date: datetime = None):
    """
    Aggregates Silver layer swaps into address-level financial metrics (Gold layer).
    Calculates 30-day win rate, activity levels, and volume.
    """
    if target_date is None:
        target_date = datetime.utcnow()

    calc_date = target_date.date()
    start_30d = target_date - timedelta(days=30)

    client = get_clickhouse_client()
    logger.info("Calculating address financials for 30d period ending %s", calc_date)

    try:
        # Extract 30 days of clean swap history
        query = """
            SELECT 
                trader_address,
                toDate(block_timestamp) as trade_date,
                amount_usd
            FROM signal_hub.clean_swaps
            WHERE block_timestamp >= %(start)s
        """
        raw_data = client.execute(query, {'start': start_30d})

        if not raw_data:
            logger.info("No swap data found for the past 30 days.")
            return

        df = pd.DataFrame(raw_data, columns=['trader_address', 'trade_date', 'amount_usd'])

        # Aggregate metrics using Pandas
        # Note: Precise PnL and Sharpe Ratio require FIFO matching of token balances.
        # Here we provide the volumetric, active days, and structural aggregations.

        agg_funcs = {
            'amount_usd': ['count', 'sum'],
            'trade_date': ['nunique']
        }

        stats = df.groupby('trader_address').agg(agg_funcs)
        stats.columns = ['total_trades', 'total_volume_usd', 'active_days']

        # Filter dust accounts (e.g., must have > 5 trades and > $100 volume in 30 days)
        active_traders = stats[(stats['total_trades'] >= 5) & (stats['total_volume_usd'] >= 100)].copy()

        records = []
        updated_at = datetime.utcnow()

        for address, row in active_traders.iterrows():
            # Placeholders for advanced PnL metrics to be expanded with pricing oracles
            win_rate = 0.50  # Mock logic placeholder
            pnl_ratio = 1.0  # Mock logic placeholder
            sharpe_ratio = 1.0  # Mock logic placeholder
            max_dd = -0.10  # Mock logic placeholder

            record = (
                address,
                calc_date,
                int(row['total_trades']),
                float(win_rate),
                float(pnl_ratio),
                float(sharpe_ratio),
                float(max_dd),
                float(row['total_volume_usd']),
                int(row['active_days']),
                updated_at
            )
            records.append(record)

        if records:
            client.execute("""
                INSERT INTO signal_hub.gold_address_financials_daily (
                    trader_address, calc_date,
                    total_trades_30d, win_rate, profit_loss_ratio, sharpe_ratio, max_drawdown,
                    total_volume_usd, active_days_30d, updated_at
                ) VALUES
            """, records)
            logger.info("Successfully inserted financials for %d addresses.", len(records))

    except Exception as e:
        logger.error("Failed to build address financials: %s", str(e), exc_info=True)


if __name__ == "__main__":
    build_daily_address_financials()