import pandas as pd
from clickhouse_driver import Client
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ClickHouse connection configuration
CH_HOST = 'localhost'
CH_PORT = 9000
CH_DB = 'signal_forge'


def import_dune_data(csv_path: str):
    logger.info(f"Starting Dune cold data import from {csv_path}")

    try:
        # Load CSV using pandas
        df = pd.read_csv(csv_path)

        # Map Dune columns to clean_swaps schema
        # Dune SQL provided: block_time, block_number, tx_hash, trader_address, dex_name, token_bought_symbol, token_sold_symbol, amount_usd
        df = df.rename(columns={
            'block_time': 'block_timestamp',
            'token_bought_symbol': 'token_out_symbol',
            'token_sold_symbol': 'token_in_symbol'
        })

        # Fill missing fields required by SignalForge schema
        df['log_index'] = 0  # Dune trades table does not expose log_index
        df['token_in_address'] = '0x0000000000000000000000000000000000000000'
        df['token_in_amount'] = 0.0
        df['token_out_address'] = '0x0000000000000000000000000000000000000000'
        df['token_out_amount'] = 0.0
        df['source'] = 'dune'

        # Ensure correct data types
        df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])
        df['amount_usd'] = df['amount_usd'].fillna(0.0).astype(float)

        # Reorder columns to match ClickHouse table
        columns = [
            'block_timestamp', 'block_number', 'tx_hash', 'log_index',
            'trader_address', 'dex_name',
            'token_in_address', 'token_in_symbol', 'token_in_amount',
            'token_out_address', 'token_out_symbol', 'token_out_amount',
            'amount_usd', 'source'
        ]
        df = df[columns]

        # Convert to list of tuples for ClickHouse driver
        records = df.to_records(index=False)
        data_to_insert = list(records)

        # Execute batch insert
        client = Client(host=CH_HOST, port=CH_PORT, database=CH_DB)
        insert_query = f"INSERT INTO clean_swaps ({', '.join(columns)}) VALUES"

        client.execute(insert_query, data_to_insert)
        logger.info(f"Successfully imported {len(data_to_insert)} records into clean_swaps")

    except Exception as e:
        logger.error(f"Failed to import Dune data: {e}", exc_info=True)


if __name__ == "__main__":
    # Replace with your actual CSV file path
    import_dune_data("../data/dune_30d_swaps.csv")