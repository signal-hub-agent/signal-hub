"""
实时 RPC 降级探针
当本地 ClickHouse 完全没数据时，走这里查链上余额进行兜底
"""
import logging
from typing import Optional
from datetime import datetime
from web3 import AsyncWeb3, AsyncHTTPProvider

# TODO: 根据新目录结构调整 config 和 Schema 路径
from core.config import settings
from .schemas import AddressDetailResponse, RiskAssessment, TokenBalance

logger = logging.getLogger(__name__)

class FallbackProbe:
    def __init__(self):
        self.rpc_urls = settings.MANTLE_RPC_URLS
        self.w3 = None

    async def _get_async_w3(self) -> Optional[AsyncWeb3]:
        if self.w3 and await self.w3.is_connected():
            return self.w3
        for url in self.rpc_urls:
            try:
                w3 = AsyncWeb3(AsyncHTTPProvider(url))
                if await w3.is_connected():
                    logger.info("AsyncWeb3 connected to fallback RPC: %s", url)
                    self.w3 = w3
                    return self.w3
            except Exception as e:
                logger.debug("Async RPC endpoint %s failed: %s", url, str(e))
                continue
        logger.error("All fallback RPC endpoints failed.")
        return None

    async def probe_address(self, address: str) -> dict: # Note: Changed return type to dict for API compat
        w3 = await self._get_async_w3()
        mnt_balance_ether = 0.0

        if w3 and w3.is_address(address):
            try:
                checksum_addr = w3.to_checksum_address(address)
                mnt_balance_wei = await w3.eth.get_balance(checksum_addr)
                mnt_balance_ether = float(w3.from_wei(mnt_balance_wei, 'ether'))
            except Exception as e:
                logger.warning("Failed to fetch balance for %s via fallback probe: %s", address, str(e))

        mock_mnt_price = 0.85
        usd_value = mnt_balance_ether * mock_mnt_price

        portfolio = []
        if mnt_balance_ether > 0:
            portfolio.append(
                TokenBalance(symbol="MNT", amount=mnt_balance_ether, usd_value=usd_value, percentage=100.0)
            )

        # 这里的构建逻辑和原版 _build_fallback_response 相同
        return {
            "address": address,
            "tags": ["Non-DEX User", "On-Chain Hodler"],
            "composite_score": 0,
            "is_dex_trader": False,
            "metrics": None,
            "portfolio": portfolio,
            "risk": {
                "risk_level": "YELLOW",
                "risk_score": 5,
                "flags": ["No DEX trading history found. Assessing base balance only."]
            },
            "data_source": "rpc_probe",
            "last_active": datetime.utcnow().isoformat()
        }