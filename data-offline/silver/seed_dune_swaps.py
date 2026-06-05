import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import logging
from pathlib import Path
import pandas as pd
import numpy as np
from config.database import get_clickhouse_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DuneSilverSeeder')

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "dune_30d_swaps.csv"  # Ensure this matches your downloaded file name


def seed_dune_to_silver():
    """
    Reads raw CSV data from the Bronze layer, maps Dune-specific columns to the
    Silver layer schema, converts timestamps to Int64 milliseconds, and inserts
    the standardized records into the clean_swaps table.
    """
    if not CSV_PATH.exists():
        logger.error("Source data file not found at %s. Run the Bronze fetcher first.", CSV_PATH)
        return

    logger.info("Loading raw data from %s", CSV_PATH)
    try:
        df = pd.read_csv(CSV_PATH)

        # 1. Comprehensive Column Renaming mapping Dune semantics to our Silver schema
        rename_map = {
            'block_time': 'block_timestamp',
            'token_bought_symbol': 'token_out_symbol',
            'token_sold_symbol': 'token_in_symbol',
            'token_bought_amount': 'token_out_amount',
            'token_sold_amount': 'token_in_amount',
            'token_bought_address': 'token_out_address',
            'token_sold_address': 'token_in_address',
        }

        existing_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=existing_rename_map)

        # 2. Schema Padding for missing strict requirements
        if 'log_index' not in df.columns:
            df['log_index'] = 0

        if 'tx_hash' not in df.columns:
            df['tx_hash'] = '0x_missing_hash'

        df['source'] = 'dune_seed'

        # Safely handle missing numerical columns
        if 'block_number' not in df.columns:
            df['block_number'] = 0
        if 'amount_usd' not in df.columns:
            df['amount_usd'] = 0.0
        if 'tx_fee_mnt' not in df.columns:
            df['tx_fee_mnt'] = 0.0

        # 3. Type Casting & ClickHouse Compatibility (CRITICAL FIX)
        # Convert string timestamp to Pandas datetime (UTC), then safely to Epoch Milliseconds (Int64)
        df['block_timestamp'] = pd.to_datetime(df['block_timestamp'], utc=True)
        # Using astype(np.int64) avoids deprecation warnings in newer pandas versions
        df['block_timestamp'] = df['block_timestamp'].astype(np.int64) // 10**6

        # Ensure correct numerical types and fill NaNs
        df['amount_usd'] = df['amount_usd'].fillna(0.0).astype(float)
        df['tx_fee_mnt'] = df['tx_fee_mnt'].fillna(0.0).astype(float)
        df['block_number'] = df['block_number'].fillna(0).astype(int)
        df['log_index'] = df['log_index'].fillna(0).astype(int)

        if 'token_in_amount' in df.columns:
            df['token_in_amount'] = df['token_in_amount'].fillna(0.0).astype(float)
        else:
            df['token_in_amount'] = 0.0

        if 'token_out_amount' in df.columns:
            df['token_out_amount'] = df['token_out_amount'].fillna(0.0).astype(float)
        else:
            df['token_out_amount'] = 0.0

        # Ensure strings are strings and fill NaNs to prevent ClickHouse insertion errors
        str_columns = [
            'tx_hash', 'trader_address', 'pool_address', 'router_address',
            'dex_name', 'token_in_address', 'token_in_symbol',
            'token_out_address', 'token_out_symbol', 'source'
        ]
        for col in str_columns:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
            else:
                df[col] = '' # Fallback for completely missing string columns

        # 4. Column Projection (Strict Ordering for ClickHouse Tuple Insert)
        # Updated to include pool_address, router_address, and tx_fee_mnt
        target_columns = [
            'block_timestamp', 'block_number', 'tx_hash', 'log_index',
            'trader_address', 'pool_address', 'router_address', 'dex_name',
            'token_in_address', 'token_in_symbol', 'token_in_amount',
            'token_out_address', 'token_out_symbol', 'token_out_amount',
            'amount_usd', 'tx_fee_mnt', 'source'
        ]

        missing_cols = [col for col in target_columns if col not in df.columns]
        if missing_cols:
            logger.error("Schema mismatch. Missing required columns after mapping: %s", missing_cols)
            return

        df_final = df[target_columns]
        records = df_final.to_records(index=False)
        data_to_insert = list(records)

        # 5. Database Insertion
        client = get_clickhouse_client()
        insert_query = f"INSERT INTO signal_hub.clean_swaps ({', '.join(target_columns)}) VALUES"

        logger.info("Executing batch insert of %d normalized records to clean_swaps.", len(data_to_insert))
        client.execute(insert_query, data_to_insert)

        logger.info("Silver layer seeding completed successfully. SignalHub is ready for analysis.")

    except pd.errors.EmptyDataError:
        logger.warning("The source CSV file is empty.")
    except Exception as e:
        logger.error("Failed to seed Dune data to Silver layer: %s", str(e), exc_info=True)


if __name__ == "__main__":
    seed_dune_to_silver()