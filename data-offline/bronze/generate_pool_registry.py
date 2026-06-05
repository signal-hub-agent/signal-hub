import pandas as pd
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data" / "dune_30d_swaps.csv"
OUTPUT_PATH = BASE_DIR / "pool_registry_generated.json"

def generate_registry_from_dune():
    if not CSV_PATH.exists():
        logger.error(f"CSV file not found at {CSV_PATH}")
        return

    logger.info(f"Loading data from {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)

    registry = {}

    # 按照池子地址分组，提取所有出现过的流动性池
    grouped = df.groupby('project_contract_address')

    for pool_addr, group in grouped:
        if pd.isna(pool_addr):
            continue

        sample = group.iloc[0]

        t0_sym = str(sample['token_bought_symbol']).upper()
        t1_sym = str(sample['token_sold_symbol']).upper()

        # 启发式精度推导 (USDT/USDC 为 6，其余默认为 18)
        t0_dec = 6 if t0_sym in ['USDT', 'USDC'] else 18
        t1_dec = 6 if t1_sym in ['USDT', 'USDC'] else 18

        registry[str(pool_addr)] = {
            "t0_address": str(sample['token_bought_address']),
            "t0_sym": t0_sym,
            "t0_dec": t0_dec,
            "t1_address": str(sample['token_sold_address']),
            "t1_sym": t1_sym,
            "t1_dec": t1_dec
        }

    logger.info(f"Successfully extracted {len(registry)} unique pools from Dune data!")

    with open(OUTPUT_PATH, "w") as f:
        json.dump(registry, f, indent=4)

    logger.info(f"Registry saved to {OUTPUT_PATH}. Your pipeline is ready for cold start.")

if __name__ == "__main__":
    generate_registry_from_dune()