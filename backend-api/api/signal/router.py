import logging
from fastapi import APIRouter, Path, Query
from .service import SignalService
from .schemas import TokenSignalResponse
from typing import List

logger = logging.getLogger(__name__)

router = APIRouter()
# 🌟 新增的接口：获取所有可用代币的列表 (必须放在 /{token} 之前)
@router.get("/available/tokens", response_model=List[str])
async def get_available_token_list():
    """
    Fetches a list of all available token symbols from the database.
    """
    return SignalService.get_available_tokens()

@router.get("/{token}", response_model=TokenSignalResponse)
async def get_token_signal(
        token: str = Path(..., description="Token symbol (e.g., MNT, MOE)"),
        force_refresh: bool = Query(False, description="Bypass cache to regenerate LLM report")
):
    """
    Fetches the comprehensive signal score and LLM report for a specific token.
    """
    return await SignalService.analyze_token(token, force_refresh)