"""
信号参谋 - API 领域契约 (Schemas)
纯粹供前端调用的输出模型结构
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
from enum import Enum
from datetime import datetime

# ==========================================
# Enums
# ==========================================
class SignalColor(str, Enum):
    GREEN = "green"   # >= 80分，可以考虑跟单
    YELLOW = "yellow" # 60-79分，谨慎观望
    RED = "red"       # < 60分，风险较高

# ==========================================
# Sub-components Details
# ==========================================
class MetricDetailBase(BaseModel):
    label: str
    score: int
    max_score: int
    is_real_data: bool = True
    status: Optional[str] = None

class VolumeTrendDetail(MetricDetailBase):
    details: Dict[str, Any] = Field(description="e.g., {'volume_24h': 10000, 'ratio': 1.5, 'trend_label': 'Volume surge'}")

class MevToxicityDetail(MetricDetailBase):
    details: Dict[str, Any] = Field(description="e.g., {'toxicity_pct': 12.5, 'toxicity_label': 'Moderate MEV'}")

class ContractSafetyDetail(MetricDetailBase):
    details: Dict[str, Any] = Field(description="e.g., {'risk_items': ['Honeypot risk']}")

class TechnicalBiasDetail(MetricDetailBase):
    details: Dict[str, Any] = Field(description="e.g., {'ma_trend': 'bullish', 'rsi_zone': 'oversold'}")

class ComponentScores(BaseModel):
    volume_trend: VolumeTrendDetail
    mev_toxicity: MevToxicityDetail
    contract_safety: ContractSafetyDetail
    technical_bias: TechnicalBiasDetail

# ==========================================
# Responses (Output to Frontend)
# ==========================================
class TokenSignalResponse(BaseModel):
    token_symbol: str
    token_address: Optional[str] = None
    current_price: float = Field(default=0.0)

    total_score: int = Field(..., description="0-100 total composite score")
    signal_color: SignalColor = Field(..., description="green/yellow/red decision indicator")

    # 承载从 Redis 缓存中取出的 LLM 报告
    llm_report: Optional[str] = Field(default=None, description="AI narrative analysis")

    components: ComponentScores
    generated_at: datetime
    data_source: str = Field(default="gold_precomputed")