import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config.database import get_clickhouse_client
from utils.job_monitor import update_job_start, update_job_end, record_system_alert
from utils.rpc_manager import batch_fetch_pools

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

V2_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
REGISTRY_PATH = Path(__file__).resolve().parent / "pool_registry_generated.json"

JOB_NAME = "process_swaps_silver"
CRON_EXPR = "0 2 * * *"  # Example: runs at 2 AM daily

try:
    with open(REGISTRY_PATH, "r") as f:
        POOL_REGISTRY = json.load(f)
    logger.info("Loaded %d verified pools into memory registry.", len(POOL_REGISTRY))
except Exception as e:
    logger.warning("Failed to load pool registry from %s. Error: %s", REGISTRY_PATH, str(e))
    POOL_REGISTRY = {}

def parse_v2_swap_data(data_hex: str):
    clean_hex = data_hex.replace('0x', '')
    if len(clean_hex) < 256:
        return 0, 0, 0, 0
    return int(clean_hex[0:64], 16), int(clean_hex[64:128], 16), int(clean_hex[128:192], 16), int(clean_hex[192:256], 16)

def process_raw_logs_to_silver(target_date: datetime):
    start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=1)

    # Calculate expected next execution time (simplified for daily jobs)
    next_exec_time = datetime.utcnow() + timedelta(days=1)

    update_job_start(JOB_NAME, CRON_EXPR, start_time, next_exec_time)

    try:
        client = get_clickhouse_client()
        extract_query = """
            SELECT block_timestamp, block_number, tx_hash, log_index, contract_address, topic1, topic2, data
            FROM signal_hub.raw_logs FINAL
            WHERE block_timestamp >= %(start)s AND block_timestamp < %(end)s AND topic0 = %(topic0)s
        """
        raw_rows = client.execute(extract_query, {
            'start': int(start_time.timestamp() * 1000),
            'end': int(end_time.timestamp() * 1000),
            'topic0': V2_SWAP_TOPIC
        })

        if not raw_rows:
            logger.info("No swap logs found for date %s", start_time.strftime('%Y-%m-%d'))
            update_job_end(JOB_NAME, "SUCCESS")
            return

        # PASS 1: Accumulate unknown pools
        unknown_pools = set()
        for row in raw_rows:
            pool_address = row[4].lower()
            if pool_address not in POOL_REGISTRY:
                unknown_pools.add(pool_address)

        # INTERMEDIATE: Batch resolve via RPC Pool
        if unknown_pools:
            new_pools = batch_fetch_pools(unknown_pools)
            if new_pools:
                POOL_REGISTRY.update(new_pools)
                try:
                    with open(REGISTRY_PATH, "w") as f:
                        json.dump(POOL_REGISTRY, f, indent=4)
                    logger.info("Registry updated and persisted with %d new pools.", len(new_pools))
                except Exception as file_error:
                    logger.error("Failed to persist registry JSON: %s", str(file_error))

        # PASS 2: Build clean records
        clean_records = []
        for row in raw_rows:
            block_ts, block_num, tx_hash, log_idx, pool_address, topic1, topic2, data = row
            pool_address = pool_address.lower()

            pool_meta = POOL_REGISTRY.get(pool_address)
            if not pool_meta:
                continue

            trader_address = f"0x{topic2[-40:]}" if topic2 and len(topic2) >= 40 else "0x0000000000000000000000000000000000000000"
            a0_in, a1_in, a0_out, a1_out = parse_v2_swap_data(data)

            if a0_in > 0 and a1_out > 0:
                token_in_address, token_in_sym, t_in_amount_raw = pool_meta["t0_address"], pool_meta["t0_sym"], a0_in
                token_out_address, token_out_sym, t_out_amount_raw = pool_meta["t1_address"], pool_meta["t1_sym"], a1_out
                token_in_amount = t_in_amount_raw / (10 ** pool_meta["t0_dec"])
                token_out_amount = t_out_amount_raw / (10 ** pool_meta["t1_dec"])
            elif a1_in > 0 and a0_out > 0:
                token_in_address, token_in_sym, t_in_amount_raw = pool_meta["t1_address"], pool_meta["t1_sym"], a1_in
                token_out_address, token_out_sym, t_out_amount_raw = pool_meta["t0_address"], pool_meta["t0_sym"], a0_out
                token_in_amount = t_in_amount_raw / (10 ** pool_meta["t1_dec"])
                token_out_amount = t_out_amount_raw / (10 ** pool_meta["t0_dec"])
            else:
                continue

            if token_in_amount > 1e15 or token_out_amount > 1e15:
                continue

            record = (
                block_ts, block_num, tx_hash, log_idx,
                trader_address, pool_address, '', 'merchant_moe',
                token_in_address, token_in_sym, float(token_in_amount),
                token_out_address, token_out_sym, float(token_out_amount),
                0.0, 0.0, 'rpc'
            )
            clean_records.append(record)

        if clean_records:
            partition_id = start_time.strftime('%Y%m%d')
            try:
                client.execute(f"ALTER TABLE signal_hub.clean_swaps DROP PARTITION '{partition_id}'")
                logger.info("Dropped existing partition %s for idempotency.", partition_id)
            except Exception as e:
                logger.debug("Partition %s drop ignored. Details: %s", partition_id, str(e))

            client.execute("""
                INSERT INTO signal_hub.clean_swaps (
                    block_timestamp, block_number, tx_hash, log_index, 
                    trader_address, pool_address, router_address, dex_name, 
                    token_in_address, token_in_symbol, token_in_amount, 
                    token_out_address, token_out_symbol, token_out_amount, 
                    amount_usd, tx_fee_mnt, source
                ) VALUES
            """, clean_records)
            logger.info("Successfully inserted %d records to ClickHouse.", len(clean_records))

        update_job_end(JOB_NAME, "SUCCESS")

    except Exception as e:
        logger.error("Job Failed: %s", str(e), exc_info=True)
        update_job_end(JOB_NAME, "FAILED", str(e))
        record_system_alert("JOB_FAILED", f"Job {JOB_NAME} failed for target date {target_date.date()}. Error: {str(e)}")

if __name__ == "__main__":
    yesterday = datetime.utcnow() - timedelta(days=1)
    process_raw_logs_to_silver(yesterday)