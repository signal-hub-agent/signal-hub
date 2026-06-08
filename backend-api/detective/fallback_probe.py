import logging
from typing import Optional
from datetime import datetime
from web3 import AsyncWeb3, AsyncHTTPProvider
from core.config import settings
from detective.schemas import AddressDetailResponse, RiskAssessment, TokenBalance

logger = logging.getLogger(__name__)

class FallbackProbe:
    """
    The Fallback Probe acts as the secondary data source when ClickHouse yields no swap history.
    It utilizes AsyncWeb3 to fetch real-time on-chain balances without blocking the event loop.
    """
    def __init__(self):
        # We initialize AsyncWeb3 with the first healthy endpoint from our High Availability pool
        self.rpc_urls = settings.MANTLE_RPC_URLS
        self.w3 = None

    async def _get_async_w3(self) -> Optional[AsyncWeb3]:
        """
        Retrieves a connected AsyncWeb3 instance using the HA endpoint pool.
        """
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

        logger.error("All fallback RPC endpoints are unreachable.")
        return None

    async def probe_address(self, address: str) -> AddressDetailResponse:
        """
        Probes the blockchain for basic address information.
        Returns a gracefully degraded AddressDetailResponse.
        """
        w3 = await self._get_async_w3()

        # Default empty portfolio structure
        mnt_balance_ether = 0.0

        if w3:
            try:
                # Fetch Native MNT Balance
                checksum_addr = w3.to_checksum_address(address)
                wei_balance = await w3.eth.get_balance(checksum_addr)
                mnt_balance_ether = float(w3.from_wei(wei_balance, 'ether'))
            except Exception as e:
                logger.error("Failed to fetch balance for %s via fallback probe: %s", address, str(e))

        # TODO: In a production scenario, you could also fetch the price of MNT via a price oracle
        # or Bybit API to calculate the usd_value. We default to 0 for this skeleton.
        mock_mnt_price = 0.85 # Assume $0.85 per MNT for demonstration
        usd_value = mnt_balance_ether * mock_mnt_price

        portfolio = []
        if mnt_balance_ether > 0:
            portfolio.append(
                TokenBalance(
                    symbol="MNT",
                    amount=mnt_balance_ether,
                    usd_value=usd_value,
                    percentage=100.0 # Only tracking native token in fallback
                )
            )

        # Construct the degraded response matching our strict schema
        return AddressDetailResponse(
            address=address,
            tags=["Non-DEX User", "On-Chain Hodler"],
            composite_score=0, # No score for non-DEX users
            is_dex_trader=False, # 🌟 The crucial flag that tells the frontend this is fallback data
            metrics=None, # No trading metrics available
            portfolio=portfolio,
            risk=RiskAssessment(
                risk_level="YELLOW",
                risk_score=5,
                flags=["No DEX trading history found. Assessing base balance only."]
            ),
            last_active=datetime.utcnow()
        )

# Export a singleton instance
fallback_probe = FallbackProbe()