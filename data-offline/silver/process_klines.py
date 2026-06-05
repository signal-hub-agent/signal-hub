import os
import sys

# 🌟 动态将 data-offline 目录加入系统路径，解决 config 导入问题
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import json
import logging
from datetime import datetime
import pandas as pd
from config.database import get_clickhouse_client

logger = logging.getLogger(__name__)


def process_raw_klines_to_silver():
    client = get_clickhouse_client()

    try:
        raw_data = client.execute("""
            SELECT token_symbol, interval, raw_json 
            FROM raw_klines 
            WHERE fetch_timestamp >= now() - INTERVAL 1 HOUR
        """)

        if not raw_data:
            logger.info("No new raw kline data to process.")
            return

        clean_records = []
        updated_at = datetime.utcnow()

        for row in raw_data:
            token, interval, raw_json_str = row
            try:
                kline_list = json.loads(raw_json_str)
                for item in kline_list:
                    open_time = datetime.utcfromtimestamp(int(item[0]) / 1000)
                    clean_records.append((
                        token,
                        interval,
                        open_time,
                        float(item[1]),  # open
                        float(item[2]),  # high
                        float(item[3]),  # low
                        float(item[4]),  # close
                        float(item[5]),  # volume
                        float(item[6]),  # turnover
                        'bybit',
                        updated_at
                    ))
            except (json.JSONDecodeError, ValueError, IndexError) as e:
                logger.error("Data parsing error for %s (%s): %s", token, interval, str(e))

        if clean_records:
            client.execute("""
                INSERT INTO clean_klines (
                    token_symbol, interval, open_time, open, high, low, close, volume, turnover, source, updated_at
                ) VALUES
            """, clean_records)
            logger.info("Successfully processed and inserted %d clean kline rows.", len(clean_records))

    except Exception as e:
        logger.error("Silver layer kline processing failed: %s", str(e), exc_info=True)


if __name__ == "__main__":
    process_raw_klines_to_silver()