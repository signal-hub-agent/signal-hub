from datetime import datetime, timedelta
from clickhouse_driver import Client
from eth_utils import decode_hex
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CH_HOST = 'localhost'
CH_PORT = 9000
CH_DB = 'signal_forge'

# ERC20 Transfer / Uniswap V2 Swap Topics for reference
SWAP_TOPIC_V2 = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'


def extract_and_clean_yesterday_data(target_date: datetime):
    start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1)

    logger.info(f"Starting ETL pipeline for data between {start_of_day} and {end_of_day}")

    client = Client(host=CH_HOST, port=CH_PORT, database=CH_DB)

    # 1. Extract raw logs using FINAL to ensure we get the latest state from ReplacingMergeTree
    extract_query = """
        SELECT 
            block_timestamp, block_number, tx_hash, log_index, 
            contract_address, topic1, topic2, data
        FROM raw_logs FINAL
        WHERE block_timestamp >= %(start)s AND block_timestamp < %(end)s
          AND topic0 = %(topic0)s
    """

    try:
        raw_rows = client.execute(extract_query, {
            'start': start_of_day,
            'end': end_of_day,
            'topic0': SWAP_TOPIC_V2
        })
        logger.info(f"Extracted {len(raw_rows)} raw swap logs for processing.")

        if not raw_rows:
            return

        clean_records = []

        # 2. Transform and Decode
        for row in raw_rows:
            block_ts, block_num, tx_hash, log_idx, pool_address, t1, t2, data = row

            # NOTE: Decoding logic strictly depends on the DEX ABI (V2 vs V3).
            # This is a structural template. You will need to parse amount0In, amount1In etc.
            # from the 'data' hex string based on the specific pool pair configuration.

            # Placeholder for data parsing logic
            trader_address = f"0x{t2[-40:]}" if t2 else "0x0000000000000000000000000000000000000000"

            # Placeholder for USD valuation (requires external price feed or routing logic)
            amount_usd = 0.0

            record = (
                block_ts,
                block_num,
                tx_hash,
                log_idx,
                trader_address,
                'merchant_moe',  # Derive from factory/pool mapping
                '0x_token_in',  # Derive from pool metadata
                'TKN_IN',
                0.0,  # Decode from data
                '0x_token_out',  # Derive from pool metadata
                'TKN_OUT',
                0.0,  # Decode from data
                amount_usd,
                'rpc'
            )
            clean_records.append(record)

        # 3. Load into DWD layer
        insert_query = """
            INSERT INTO clean_swaps (
                block_timestamp, block_number, tx_hash, log_index, 
                trader_address, dex_name, 
                token_in_address, token_in_symbol, token_in_amount, 
                token_out_address, token_out_symbol, token_out_amount, 
                amount_usd, source
            ) VALUES
        """
        client.execute(insert_query, clean_records)
        logger.info(f"Successfully loaded {len(clean_records)} records into clean_swaps.")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)


if __name__ == "__main__":
    # By default, run for yesterday
    yesterday = datetime.utcnow() - timedelta(days=1)
    extract_and_clean_yesterday_data(yesterday)