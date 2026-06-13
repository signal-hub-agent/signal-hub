import logging
from fastapi import APIRouter, Path, Query
from .service import SignalService
from .schemas import TokenSignalResponse

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/{token}", response_model=TokenSignalResponse)
async def get_token_signal(
        token: str = Path(..., description="Token symbol (e.g., MNT, MOE)"),
        force_refresh: bool = Query(False, description="Bypass cache to regenerate LLM report")
):
    """
    Fetches the comprehensive signal score and LLM report for a specific token.
    """
    return await SignalService.analyze_token(token, force_refresh)