# backend-api/api/alerts/service.py
from typing import List
import json
from .schemas import SubscribeRequest, SubscriptionItem, AlertMessage, TargetType

class AlertsService:
    def __init__(self, pool):
        self.pool = pool

    async def add_subscription(self, req: SubscribeRequest) -> bool:
        # 直接写入新增的 email 字段
        query = """
            INSERT INTO signal_hub.user_subscriptions (email, target_id, target_type)
            VALUES ($1, $2, $3)
            ON CONFLICT (email, target_id, target_type) DO NOTHING;
        """
        await self.pool.execute(query, req.user_email, req.target_id, req.target_type.value)
        return True

    async def get_user_subscriptions(self, user_email: str) -> List[SubscriptionItem]:
        # 变动：WHERE 条件直接匹配 email 字段
        query = """
            SELECT target_id, target_type, subscribed_at 
            FROM signal_hub.user_subscriptions 
            WHERE email = $1
        """
        rows = await self.pool.fetch(query, user_email)

        items = []
        for r in rows:
            try:
                t_type = TargetType(r['target_type'])
            except ValueError:
                t_type = TargetType.ADDRESS

            items.append(SubscriptionItem(
                target_id=r['target_id'],
                target_type=t_type,
                created_at=r['subscribed_at']
            ))
        return items

    async def get_user_alerts(self, user_email: str) -> List[AlertMessage]:
        # 核心变动：JOIN 联查时直接利用 s.email = $1 进行过滤，不再经过 wallet_address
        query = """
            SELECT z.id, z.target_address, z.severity, z.alert_type, z.detected_at, z.payload
            FROM signal_hub.zero_day_alerts z
            JOIN signal_hub.user_subscriptions s ON z.target_address = s.target_id
            WHERE s.email = $1
            ORDER BY z.detected_at DESC
            LIMIT 50
        """
        rows = await self.pool.fetch(query, user_email)

        alerts = []
        for r in rows:
            payload_data = r['payload']
            if isinstance(payload_data, str):
                try:
                    payload_data = json.loads(payload_data)
                except json.JSONDecodeError:
                    payload_data = {}

            detail = payload_data.get("detail", "") if isinstance(payload_data, dict) else ""
            msg = f"Alert: {r['alert_type']} detected. {detail}".strip()

            alerts.append(AlertMessage(
                alert_id=str(r['id']),
                target_id=str(r['target_address']),
                target_type=TargetType.ADDRESS,
                severity=str(r['severity']),
                message=msg,
                timestamp=r['detected_at']
            ))
        return alerts