import time
import logging
from web3 import Web3
from utils.job_monitor import record_system_alert

logger = logging.getLogger(__name__)

# High Availability RPC Pool Configuration
RPC_ENDPOINTS = [
    "https://rpc.mantle.xyz",
    "https://mantle-rpc.publicnode.com",
    "https://mantle.public-rpc.com",
    "https://rpc.ankr.com/mantle"
]

PAIR_ABI = [
    {"constant": True, "inputs": [], "name": "token0", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "payable": False, "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "token1", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "payable": False, "stateMutability": "view", "type": "function"}
]
ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"internalType": "string", "name": "", "type": "string"}], "payable": False, "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}], "payable": False, "stateMutability": "view", "type": "function"}
]

def get_healthy_w3() -> Web3:
    """
    Sequential heartbeat check. Returns the first healthy Web3 instance.
    """
    for endpoint in RPC_ENDPOINTS:
        try:
            w3 = Web3(Web3.HTTPProvider(endpoint, request_kwargs={'timeout': 5}))
            if w3.is_connected():
                logger.debug("RPC Heartbeat OK: %s", endpoint)
                return w3
        except Exception:
            continue
    return None

def get_w3_with_retry(sleep_minutes: int = 5) -> Web3:
    """
    Attempts to get a healthy RPC connection with a 3x3 retry logic.
    """
    for sleep_retry in range(3):
        for immediate_retry in range(3):
            w3 = get_healthy_w3()
            if w3:
                return w3
            logger.warning("All RPC nodes unresponsive. Immediate retry %d/3...", immediate_retry + 1)
            time.sleep(3)

        logger.error("RPC Pool failed immediate retries. Sleeping for %d minutes. Sleep retry %d/3...", sleep_minutes, sleep_retry + 1)
        time.sleep(sleep_minutes * 60)

    record_system_alert("RPC_POOL_DEAD", "All Mantle RPC nodes are unreachable after max retries.")
    return None

def batch_fetch_pools(pool_addresses: set) -> dict:
    """
    Accumulates a batch of unknown pool addresses and queries them using a single
    healthy Web3 connection session. Includes rate-limiting to prevent RPC bans.
    """
    if not pool_addresses:
        return {}

    w3 = get_w3_with_retry()
    if not w3:
        logger.error("Cannot perform batch fetch. No healthy RPC available.")
        return {}

    total_pools = len(pool_addresses)
    logger.info("Starting batch fetch for %d unknown pools with rate limiting...", total_pools)
    results = {}
    processed_count = 0

    for pool_address in pool_addresses:
        try:
            pool_contract = w3.eth.contract(address=w3.to_checksum_address(pool_address), abi=PAIR_ABI)
            token0_addr = pool_contract.functions.token0().call()
            token1_addr = pool_contract.functions.token1().call()

            t0_contract = w3.eth.contract(address=token0_addr, abi=ERC20_ABI)
            t1_contract = w3.eth.contract(address=token1_addr, abi=ERC20_ABI)

            try: t0_sym = t0_contract.functions.symbol().call()
            except Exception as inner_e:
                logger.debug("Failed to get token0 symbol for pool %s: %s", pool_address, inner_e)
                t0_sym = "UNKNOWN"

            try: t0_dec = t0_contract.functions.decimals().call()
            except Exception as inner_e:
                logger.debug("Failed to get token0 decimals for pool %s: %s", pool_address, inner_e)
                t0_dec = 18

            try: t1_sym = t1_contract.functions.symbol().call()
            except Exception as inner_e:
                logger.debug("Failed to get token1 symbol for pool %s: %s", pool_address, inner_e)
                t1_sym = "UNKNOWN"

            try: t1_dec = t1_contract.functions.decimals().call()
            except Exception as inner_e:
                logger.debug("Failed to get token1 decimals for pool %s: %s", pool_address, inner_e)
                t1_dec = 18

            results[str(pool_address).lower()] = {
                "t0_address": token0_addr.lower(),
                "t0_sym": t0_sym,
                "t0_dec": t0_dec,
                "t1_address": token1_addr.lower(),
                "t1_sym": t1_sym,
                "t1_dec": t1_dec
            }
            processed_count += 1

            # Progress logging every 20 pools
            if processed_count % 20 == 0:
                logger.info("Progress: Fetched %d/%d pools...", processed_count, total_pools)

        except Exception as e:
            logger.warning("Failed to fetch metadata for pool %s: %s", pool_address, str(e))
            continue

        # RATE LIMITING: Pause for 200ms between each pool to avoid triggering RPC DDoS protection
        time.sleep(0.2)

    logger.info("Batch fetch completed. Successfully resolved %d/%d pools.", len(results), total_pools)
    return results