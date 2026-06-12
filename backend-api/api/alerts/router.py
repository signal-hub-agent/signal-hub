# backend-api/api/alerts/router.py
from fastapi import APIRouter, Depends, Query
from typing import List
from core.db_postgres import pg_manager
from .schemas import SubscribeRequest, SubscriptionItem, AlertMessage
from .service import AlertsService

router = APIRouter()

async def get_db_pool():
    return pg_manager.get_pool()

def get_alerts_service(pool = Depends(get_db_pool)):
    return AlertsService(pool)

@router.post("/subscribe", summary="新增或编辑地址订阅(全量覆盖配置)")
async def subscribe_target(req: SubscribeRequest, service: AlertsService = Depends(get_alerts_service)):
    await service.add_or_update_subscription(req)
    return {"status": "success", "message": f"Successfully updated subscription for {req.target_id}"}

@router.delete("/unsubscribe", summary="删除地址订阅")
async def unsubscribe_target(
        user_email: str = Query(..., description="用户邮箱"),
        target_id: str = Query(..., description="钱包地址或Token"),
        target_type: str = Query("ADDRESS", description="订阅类型"),
        service: AlertsService = Depends(get_alerts_service)
):
    await service.delete_subscription(user_email, target_id, target_type)
    return {"status": "success", "message": "Subscription removed."}

@router.get("/subscriptions", response_model=List[SubscriptionItem], summary="获取用户订阅列表及详细配置")
async def list_subscriptions(user_email: str, service: AlertsService = Depends(get_alerts_service)):
    return await service.get_user_subscriptions(user_email)

@router.get("/history", response_model=List[AlertMessage], summary="获取用户的告警时间轴")
async def list_alerts(user_email: str, service: AlertsService = Depends(get_alerts_service)):
    return await service.get_user_alerts(user_email)