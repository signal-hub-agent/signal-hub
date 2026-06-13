import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from core.db_postgres import pg_manager
from core.telegram import send_tg_message
from core.config import settings

logger = logging.getLogger(__name__)

class AlertRouterWorker:
    def __init__(self):
        self.is_running = False
        self.consumer = None
        # Flink 写入的新 Topic 名称
        self.topic_name = "signal-hub-alerts"

        self.bootstrap_servers = getattr(settings, "KAFKA_BROKERS", "localhost:9092")

    async def start(self):
        """启动后台 Kafka 消费路由进程"""
        self.is_running = True
        logger.info(f"🚀 Alert Router Worker started. Connecting to Kafka {self.bootstrap_servers} topic: {self.topic_name}")

        pool = pg_manager.get_pool()

        # 初始化 AIOKafkaConsumer
        self.consumer = AIOKafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_servers,
            group_id="python-alert-router-group",
            auto_offset_reset="latest", # 告警系统通常只关心最新数据，避免重启时收到大量积压历史告警
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        try:
            await self.consumer.start()
        except Exception as e:
            logger.error(f"Failed to start Kafka Consumer: {e}")
            self.is_running = False
            return

        # 持续监听消息
        try:
            while self.is_running:
                # 阻塞等待消息
                async for msg in self.consumer:
                    if not self.is_running:
                        break
                    signal_data = msg.value
                    # 将单条信号分发给处理函数
                    await self.process_signal(signal_data, pool)
        except Exception as e:
            logger.error(f"Error in Alert Router consume loop: {e}")
        finally:
            await self.stop()

    async def stop(self):
        """优雅关闭消费者"""
        self.is_running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("⏹️ Alert Router Kafka Consumer stopped.")

    async def process_signal(self, signal: dict, pool):
        """
        核心规则引擎：根据 Flink 传来的原生信号，匹配用户的 JSONB 订阅配置
        """
        target_address = signal.get("target_address")
        event_type = signal.get("event_type")
        event_data = signal.get("data", {})
        tx_hash = signal.get("tx_hash", "Unknown")
        chain_name = signal.get("chain_name", "mantle")

        if not target_address or not event_type:
            logger.warning(f"Invalid signal format received: {signal}")
            return

        # 1. 联合查询：只查订阅了该地址，并且已经绑定了 Telegram 的用户
        query = """
            SELECT s.email, s.config, u.telegram_chat_id 
            FROM signal_hub.user_subscriptions s
            JOIN signal_hub.users u ON s.email = u.email
            WHERE s.target_id = $1 AND u.telegram_chat_id IS NOT NULL
        """
        try:
            subscribers = await pool.fetch(query, target_address)
        except Exception as e:
            logger.error(f"Database query failed in process_signal: {e}")
            return

        # 如果没有人订阅该地址（或者订阅者没绑 TG），直接丢弃
        if not subscribers:
            return

        # 2. 遍历所有订阅者，进行规则引擎匹配
        for sub in subscribers:
            config_str = sub['config']
            # asyncpg 返回的 JSONB 可能是字符串，也可能是 dict，做一层安全解析
            try:
                config = json.loads(config_str) if isinstance(config_str, str) else config_str
            except Exception:
                config = {}

            chat_id = sub['telegram_chat_id']
            is_triggered = False
            alert_message = ""

            # =========== 规则引擎逻辑 ===========

            if event_type == "WHALE_MOVEMENT":
                rule = config.get("whale_movement", {})
                if rule.get("enabled", False):
                    usd_value = float(event_data.get("usd_value", 0))
                    threshold = float(rule.get("threshold_usd", 50000))
                    flow_dir = event_data.get("flow_direction", "MOVED")
                    token_sym = event_data.get("token_symbol", "Tokens")

                    if usd_value >= threshold:
                        is_triggered = True
                        alert_message = (
                            f"🐋 **Whale Movement Alert**\n\n"
                            f"**Address:** `{target_address}`\n"
                            f"**Action:** {flow_dir} **${usd_value:,.2f}** worth of {token_sym}\n"
                            f"**Chain:** {chain_name.capitalize()}\n"
                            f"**TxHash:** `{tx_hash}`\n"
                            f"*(Threshold setting: ${threshold:,.2f})*"
                        )

            elif event_type == "SMART_SWAP":
                rule = config.get("smart_swap", {})
                if rule.get("enabled", False):
                    token_sym = event_data.get("token_symbol", "Unknown Token")
                    dex_name = event_data.get("dex_name", "DEX")
                    is_triggered = True
                    alert_message = (
                        f"⚡ **Smart Money Swap**\n\n"
                        f"**Address:** `{target_address}`\n"
                        f"**Action:** Bought new/low-cap token **{token_sym}** on {dex_name}\n"
                        f"**Chain:** {chain_name.capitalize()}\n"
                        f"**TxHash:** `{tx_hash}`"
                    )

            elif event_type == "ZERO_DAY":
                rule = config.get("zero_day", {})
                if rule.get("enabled", False):
                    contract_age = float(event_data.get("contract_age_hours", 0))
                    max_age = float(rule.get("max_contract_age_hours", 24))

                    if contract_age <= max_age:
                        is_triggered = True
                        alert_message = (
                            f"🛡️ **Zero-Day Interaction Warning**\n\n"
                            f"**Address:** `{target_address}`\n"
                            f"**Action:** Interacted with a new contract (Age: {contract_age:.1f} hours)\n"
                            f"**Chain:** {chain_name.capitalize()}\n"
                            f"**TxHash:** `{tx_hash}`\n"
                            f"*(Threshold setting: < {max_age} hours)*"
                        )

            elif event_type == "LIQUIDITY":
                rule = config.get("liquidity", {})
                if rule.get("enabled", False):
                    pool_name = event_data.get("pool_name", "Unknown Pool")
                    action = event_data.get("action", "REMOVED")
                    is_triggered = True
                    alert_message = (
                        f"💧 **Liquidity Provisioning Alert**\n\n"
                        f"**Address:** `{target_address}`\n"
                        f"**Action:** {action} massive liquidity from **{pool_name}**\n"
                        f"**Chain:** {chain_name.capitalize()}\n"
                        f"**TxHash:** `{tx_hash}`"
                    )

            elif event_type == "BRIDGE":
                rule = config.get("bridge", {})
                if rule.get("enabled", False):
                    usd_value = float(event_data.get("usd_value", 0))
                    threshold = float(rule.get("threshold_usd", 10000))
                    bridge_dir = event_data.get("bridge_direction", "Mantle -> L1")

                    if usd_value >= threshold:
                        is_triggered = True
                        alert_message = (
                            f"🌉 **Bridge Transfer Alert**\n\n"
                            f"**Address:** `{target_address}`\n"
                            f"**Action:** Bridged **${usd_value:,.2f}** ({bridge_dir})\n"
                            f"**TxHash:** `{tx_hash}`\n"
                            f"*(Threshold setting: ${threshold:,.2f})*"
                        )

            # =========== 触发发送与埋点 ===========
            if is_triggered and alert_message:
                logger.info(f"Triggering TG alert to {chat_id} for event {event_type}")
                # 1. 异步发送 TG 消息
                asyncio.create_task(send_tg_message(chat_id, alert_message))

                # 2. 🌟 新增：大屏今日告警数统计 (Redis 埋点)
                try:
                    from datetime import datetime
                    from core.redis_client import get_redis_client
                    redis_client = await get_redis_client()
                    today_str = datetime.utcnow().strftime('%Y%m%d')
                    kpi_key = f"dashboard:alerts_today:{today_str}"
                    # 计数器 +1，并设置 48 小时自动过期（防止内存泄漏）
                    await redis_client.incr(kpi_key)
                    await redis_client.expire(kpi_key, 172800)
                except Exception as e:
                    logger.error(f"Failed to increment dashboard alert KPI: {e}")