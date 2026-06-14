from pydantic import BaseModel, Field
from typing import List

# ==========================================
# Dashboard KPI 数据结构
# ==========================================
class KpiResponse(BaseModel):
    smart_swaps_count: int
    smart_swaps_sum: float
    zero_day_count: int
    whale_moves_count: int
    whale_moves_sum: float
    liquidity_count: int
    liquidity_sum: float
    bridges_count: int
    bridges_sum: float

# ==========================================
# 左翼：Token 雷达数据结构
# ==========================================
class TokenRadarItem(BaseModel):
    symbol: str
    name: str = ""
    volume_1h_usd: float
    volume_change_pct: float = 0.0
    mev_toxicity_pct: float
    ai_score: float
    ai_insight: str

# ==========================================
# 右翼：聪明钱侦探数据结构
# ==========================================
class SmartMoneyItem(BaseModel):
    address: str
    win_rate: float
    pnl_ratio: float
    tags: List[str]
    ai_profiling: str
    score: int