import logging
from pathlib import Path
import pandas as pd
from config.database import get_clickhouse_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DuneSilverSeeder')

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "dune_30d_swaps.csv"


def seed_dune_to_silver():
    """
    Reads raw CSV data from the Bronze layer, applies Silver layer schema transformations,
    and inserts the standardized records into the DWD clean_swaps table.
    """
    if not CSV_PATH.exists():
        logger.error("Source data file not found at %s. Run the Bronze fetcher first.", CSV_PATH)
        return

    logger.info("Loading raw data from %s", CSV_PATH)
    try:
        df = pd.read_csv(CSV_PATH)

        # 1. Column Renaming to match Silver layer schema
        df = df.rename(columns={
            'block_time': 'block_timestamp',
            'token_bought_symbol': 'token_out_symbol',
            'token_sold_symbol': 'token_in_symbol'
        })

        # 2. Schema Padding & Type Casting
        # Dune trades table does not expose log_index or precise token addresses
        df['log_index'] = 0
        df['token_in_address'] = '0x0000000000000000000000000000000000000000'
        df['token_in_amount'] = 0.0
        df['token_out_address'] = '0x0000000000000000000000000000000000000000'
        df['token_out_amount'] = 0.0
        df['source'] = 'dune_seed'

        # Safely handle potential missing columns from the Dune result
        if 'block_number' not in df.columns:
            df['block_number'] = 0

        # Type conversion ensuring ClickHouse compatibility
        df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])
        df['amount_usd'] = df['amount_usd'].fillna(0.0).astype(float)
        df['block_number'] = df['block_number'].fillna(0).astype(int)

        # 3. Column Projection (Strict Ordering)
        target_columns = [
            'block_timestamp', 'block_number', 'tx_hash', 'log_index',
            'trader_address', 'dex_name',
            'token_in_address', 'token_in_symbol', 'token_in_amount',
            'token_out_address', 'token_out_symbol', 'token_out_amount',
            'amount_usd', 'source'
        ]

        missing_cols = [col for col in target_columns if col not in df.columns]
        if missing_cols:
            logger.error("Schema mismatch. Missing required columns: %s", missing_cols)
            return

        df_final = df[target_columns]
        records = df_final.to_records(index=False)
        data_to_insert = list(records)

        # 4. Database Insertion
        client = get_clickhouse_client()
        insert_query = f"INSERT INTO signal_hub.clean_swaps ({', '.join(target_columns)}) VALUES"

        logger.info("Executing batch insert of %d normalized records to clean_swaps.", len(data_to_insert))
        client.execute(insert_query, data_to_insert)

        logger.info("Silver layer seeding completed successfully.")

    except pd.errors.EmptyDataError:
        logger.warning("The source CSV file is empty.")
    except Exception as e:
        logger.error("Failed to seed Dune data to Silver layer: %s", str(e), exc_info=True)


if __name__ == "__main__":
    seed_dune_to_silver()