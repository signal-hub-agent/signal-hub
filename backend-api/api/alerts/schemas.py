# backend-api/api/alerts/schemas.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime

class TargetType(str, Enum):
    TOKEN = "TOKEN"
    ADDRESS = "ADDRESS"
    SYSTEM = "SYSTEM"

# === 核心：五大告警类别的自定义参数模型 ===

class WhaleMovementConfig(BaseModel):
    enabled: bool = False
    threshold_usd: float = Field(50000.0, description="大额转账阈值(USD)")

class SmartMoneySwapConfig(BaseModel):
    enabled: bool = False
    # 未来可扩展: min_volume_usd: float = 0.0

class ZeroDayInteractionConfig(BaseModel):
    enabled: bool = False
    max_contract_age_hours: int = Field(24, description="合约部署时间限制(小时)")

class LiquidityProvisioningConfig(BaseModel):
    enabled: bool = False

class BridgeTransferConfig(BaseModel):
    enabled: bool = False
    threshold_usd: float = Field(10000.0, description="跨链资金阈值(USD)")

class AlertConfig(BaseModel):
    """汇总的告警配置项"""
    whale_movement: WhaleMovementConfig = Field(default_factory=WhaleMovementConfig)
    smart_swap: SmartMoneySwapConfig = Field(default_factory=SmartMoneySwapConfig)
    zero_day: ZeroDayInteractionConfig = Field(default_factory=ZeroDayInteractionConfig)
    liquidity: LiquidityProvisioningConfig = Field(default_factory=LiquidityProvisioningConfig)
    bridge: BridgeTransferConfig = Field(default_factory=BridgeTransferConfig)

# === API 交互模型 ===

class SubscribeRequest(BaseModel):
    target_id: str
    target_type: TargetType
    user_email: str
    name: str = Field(..., description="用户自定义的告警名称")
    config: AlertConfig = Field(..., description="告警触发规则集合")

class SubscriptionItem(BaseModel):
    target_id: str
    target_type: TargetType
    name: str
    config: AlertConfig
    created_at: datetime

class AlertMessage(BaseModel):
    alert_id: str
    target_id: str
    target_type: TargetType
    severity: str
    message: str
    timestamp: datetime