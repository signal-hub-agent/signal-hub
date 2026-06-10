import logging
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from typing import List

from .service import DetectiveService
from .schemas import (
    AddressDetailResponse,
    Top100Response,
    TopologyGraphResponse,
    SubscribeRequest
)

# Placeholder for dependencies
# from core.dependencies.auth import get_current_user_id

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/detective",
    tags=["Address Detective"]
)

@router.get("/top100", response_model=Top100Response)
async def get_top_100_traders():
    """
    Fetches the Top 100 most valuable addresses to copy-trade.
    This endpoint reads directly from the ultra-fast Redis cache.
    """
    return await DetectiveService.get_top_100_from_redis()

@router.get("/search/{address}", response_model=AddressDetailResponse)
async def search_address(
        address: str = Path(..., description="EVM Address to analyze"),
        force_refresh: bool = Query(False, description="Bypass cache and regenerate LLM report")
):
    """
    Analyzes any given address.
    Implements dynamic fallback: Redis (LLM) -> ClickHouse (Indicators) -> Web3 RPC (Fallback).
    """
    return await DetectiveService.analyze_address(address, force_refresh)

@router.get("/{address}/topology", response_model=TopologyGraphResponse)
async def get_address_topology(
        address: str = Path(..., description="EVM Address for graph topology")
):
    """
    Generates a node-edge graph of the address's 30-day fund flow.
    """
    return await DetectiveService.build_topology_graph(address.lower())

@router.post("/subscribe", status_code=201)
async def subscribe_to_address(
        request: SubscribeRequest,
        # user_id: str = Depends(get_current_user_id) # Require authentication
):
    """
    Subscribes the current user to real-time alerts for the target address.
    """
    user_id = "mock_user_123" # 临时 Mock
    target = request.target_address.lower()
    await DetectiveService.subscribe(user_id, target)
    return {"status": "success", "message": f"Subscribed to {target}"}