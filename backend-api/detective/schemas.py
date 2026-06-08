from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

# ==========================================
# Common & Sub-components
# ==========================================
class TokenBalance(BaseModel):
    symbol: str = Field(..., description="Token symbol, e.g., MNT")
    amount: float = Field(..., description="Amount of token held")
    usd_value: float = Field(..., description="Estimated USD value")
    percentage: float = Field(..., description="Percentage of total portfolio")

class FinancialMetrics(BaseModel):
    win_rate: float = Field(..., description="Win rate percentage (0-100)")
    profit_loss_ratio: float = Field(..., description="Pnl Ratio, e.g., 2.5")
    sharpe_ratio: float = Field(..., description="Sharpe ratio, >1.5 is excellent")
    max_drawdown: float = Field(..., description="Max drawdown percentage")
    account_growth_30d: float = Field(..., description="30-day account growth percentage")

class RiskAssessment(BaseModel):
    risk_level: str = Field(..., description="GREEN, YELLOW, or RED")
    risk_score: int = Field(..., description="Risk score 0-10")
    flags: List[str] = Field(default_factory=list, description="List of risk warnings, e.g., 'MEV Suspected'")

# ==========================================
# Responses (Output to Frontend)
# ==========================================
class AddressDetailResponse(BaseModel):
    address: str
    tags: List[str] = Field(description="Neutral tags like 'Whale', 'High-Frequency'")
    composite_score: int = Field(description="Total copy-trade value score 0-100")
    is_dex_trader: bool = Field(description="True if CH has swap history, False if fallback RPC was used")
    metrics: Optional[FinancialMetrics] = None
    portfolio: List[TokenBalance] = Field(default_factory=list)
    risk: RiskAssessment
    last_active: datetime

class Top100ListItem(BaseModel):
    address: str
    composite_score: int
    tags: List[str]
    win_rate: float
    total_volume_30d: float

class Top100Response(BaseModel):
    updated_at: datetime
    traders: List[Top100ListItem]

class TopologyNode(BaseModel):
    id: str
    label: str
    type: str = Field(description="e.g., 'wallet', 'pool', 'cex'")

class TopologyEdge(BaseModel):
    source: str
    target: str
    amount_usd: float
    token_symbol: str
    timestamp: datetime

class TopologyGraphResponse(BaseModel):
    nodes: List[TopologyNode]
    edges: List[TopologyEdge]

# ==========================================
# Requests (Input from Frontend)
# ==========================================
class SubscribeRequest(BaseModel):
    target_address: str = Field(..., min_length=42, max_length=42, description="The EVM address to subscribe to")