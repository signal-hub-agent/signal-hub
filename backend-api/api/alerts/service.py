# backend-api/api/alerts/service.py
from typing import List
import json
from .schemas import SubscribeRequest, SubscriptionItem, AlertMessage, TargetType, AlertConfig

class AlertsService:
    def __init__(self, pool):
        self.pool = pool

    async def add_or_update_subscription(self, req: SubscribeRequest) -> bool:
        """
        利用 PostgreSQL 的 UPSERT 特性：
        如果用户之前没订阅过，就创建；如果订阅过，就更新它的名称和 config 规则。
        """
        query = """
            INSERT INTO signal_hub.user_subscriptions (email, target_id, target_type, name, config)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            ON CONFLICT (email, target_id, target_type) 
            DO UPDATE SET 
                name = EXCLUDED.name,
                config = EXCLUDED.config,
                subscribed_at = CURRENT_TIMESTAMP;
        """
        # 将 Pydantic 模型转为 JSON 字符串存入 JSONB 字段
        config_json = req.config.model_dump_json()
        await self.pool.execute(query, req.user_email, req.target_id, req.target_type.value, req.name, config_json)
        return True

    async def delete_subscription(self, user_email: str, target_id: str, target_type: str) -> bool:
        """删除订阅"""
        query = "DELETE FROM signal_hub.user_subscriptions WHERE email = $1 AND target_id = $2 AND target_type = $3"
        await self.pool.execute(query, user_email, target_id, target_type)
        return True

    async def get_user_subscriptions(self, user_email: str) -> List[SubscriptionItem]:
        query = """
            SELECT target_id, target_type, name, config, subscribed_at 
            FROM signal_hub.user_subscriptions 
            WHERE email = $1
        """
        rows = await self.pool.fetch(query, user_email)

        items = []
        for r in rows:
            # 安全解析 JSONB 配置
            config_data = r['config'] if isinstance(r['config'], str) else json.dumps(r['config'])
            try:
                parsed_config = AlertConfig.model_validate_json(config_data)
            except Exception as e:
                print(f"Config parse error for {r['target_id']}: {e}")
                parsed_config = AlertConfig() # fallback

            try:
                t_type = TargetType(r['target_type'])
            except ValueError:
                t_type = TargetType.ADDRESS

            items.append(SubscriptionItem(
                target_id=r['target_id'],
                target_type=t_type,
                name=r.get('name') or "Unnamed Alert",
                config=parsed_config,
                created_at=r['subscribed_at']
            ))
        return items

    async def get_user_alerts(self, user_email: str) -> List[AlertMessage]:
        # 告警查询逻辑保持不变，但你的流处理后端写入 zero_day_alerts 时，
        # 需要读取新的 config 规则来决定是否触发该用户的告警。
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