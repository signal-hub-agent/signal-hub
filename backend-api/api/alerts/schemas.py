# backend-api/api/alerts/schemas.py
from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum
from datetime import datetime

class TargetType(str, Enum):
    TOKEN = "TOKEN"
    ADDRESS = "ADDRESS"
    SYSTEM = "SYSTEM"  # 用于零日信号等全局配置

class SubscribeRequest(BaseModel):
    target_id: str = Field(..., description="代币Symbol、钱包地址或系统事件名")
    target_type: TargetType
    user_email: str = Field(..., description="用户的唯一标识(Google邮箱)")

class SubscriptionItem(BaseModel):
    target_id: str
    target_type: TargetType
    created_at: datetime

class AlertMessage(BaseModel):
    alert_id: str
    target_id: str
    target_type: TargetType
    severity: str = Field(..., description="HIGH, MEDIUM, LOW")
    message: str
    is_read: bool = False
    timestamp: datetime