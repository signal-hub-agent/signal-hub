# backend-api/api/alerts/router.py
from fastapi import APIRouter, Depends
from typing import List
from core.db_postgres import pg_manager # 导入你的 pg_manager
from .schemas import SubscribeRequest, SubscriptionItem, AlertMessage
from .service import AlertsService

router = APIRouter()

# 获取连接池依赖
async def get_db_pool():
    return pg_manager.get_pool()

# 初始化 Service 的工厂函数
def get_alerts_service(pool = Depends(get_db_pool)):
    return AlertsService(pool)

@router.post("/subscribe", summary="添加新订阅")
async def subscribe_target(req: SubscribeRequest, service: AlertsService = Depends(get_alerts_service)):
    await service.add_subscription(req)
    return {"status": "success", "message": f"Successfully subscribed to {req.target_id}"}

@router.get("/subscriptions", response_model=List[SubscriptionItem], summary="获取用户订阅列表")
async def list_subscriptions(user_email: str, service: AlertsService = Depends(get_alerts_service)):
    return await service.get_user_subscriptions(user_email)

@router.get("/history", response_model=List[AlertMessage], summary="获取用户的告警时间轴")
async def list_alerts(user_email: str, service: AlertsService = Depends(get_alerts_service)):
    return await service.get_user_alerts(user_email)