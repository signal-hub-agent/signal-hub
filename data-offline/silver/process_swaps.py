import logging
from datetime import datetime, timedelta
from config.database import get_clickhouse_client

logger = logging.getLogger(__name__)

# Uniswap V2 Swap Event Signature
V2_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"


def parse_v2_swap_data(data_hex: str):
    """
    Decodes the unindexed data from a V2 Swap event.
    Format: amount0In, amount1In, amount0Out, amount1Out (uint256 each, 32 bytes)
    """
    clean_hex = data_hex.replace('0x', '')
    if len(clean_hex) < 256:
        return 0, 0, 0, 0

    amount0_in = int(clean_hex[0:64], 16)
    amount1_in = int(clean_hex[64:128], 16)
    amount0_out = int(clean_hex[128:192], 16)
    amount1_out = int(clean_hex[192:256], 16)

    return amount0_in, amount1_in, amount0_out, amount1_out


def process_raw_logs_to_silver(target_date: datetime):
    start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=1)

    client = get_clickhouse_client()

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
            'start': start_time,
            'end': end_time,
            'topic0': V2_SWAP_TOPIC
        })

        if not raw_rows:
            logger.info("No swap logs found for date %s", start_time.strftime('%Y-%m-%d'))
            return

        clean_records = []

        for row in raw_rows:
            block_ts, block_num, tx_hash, log_idx, pool_address, topic1, topic2, data = row

            # The 'to' address is typically indexed in topic2
            trader_address = f"0x{topic2[-40:]}" if topic2 and len(
                topic2) >= 40 else "0x0000000000000000000000000000000000000000"

            a0_in, a1_in, a0_out, a1_out = parse_v2_swap_data(data)

            # TODO: Integrate contract resolution to identify Token0 and Token1 symbols based on pool_address
            # For pipeline resilience, setting placeholders to avoid failure; external dictionary mapping required here.
            token_in_sym = "UNKNOWN"
            token_out_sym = "UNKNOWN"
            amount_usd = 0.0  # External pricing oracle calculation required

            # Filter out obvious dust transactions (Mock logic)
            if a0_in == 0 and a1_in == 0:
                continue

            record = (
                block_ts, block_num, tx_hash, log_idx,
                trader_address,
                'merchant_moe',
                '0x_token0_address_placeholder', token_in_sym, float(max(a0_in, a1_in)),
                '0x_token1_address_placeholder', token_out_sym, float(max(a0_out, a1_out)),
                amount_usd,
                'rpc'
            )
            clean_records.append(record)

        if clean_records:
            client.execute("""
                INSERT INTO clean_swaps (
                    block_timestamp, block_number, tx_hash, log_index, 
                    trader_address, dex_name, 
                    token_in_address, token_in_symbol, token_in_amount, 
                    token_out_address, token_out_symbol, token_out_amount, 
                    amount_usd, source
                ) VALUES
            """, clean_records)
            logger.info("Processed and loaded %d clean swap records.", len(clean_records))

    except Exception as e:
        logger.error("Swap log processing failed: %s", str(e), exc_info=True)


if __name__ == "__main__":
    yesterday = datetime.utcnow() - timedelta(days=1)
    process_raw_logs_to_silver(yesterday)