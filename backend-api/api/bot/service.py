import random
import string
import logging
from core.redis_client import get_redis_client
from core.telegram import send_tg_message
from .schemas import BindCodeResponse

logger = logging.getLogger(__name__)

class BotService:
    def __init__(self, pool):
        self.pool = pool

    async def generate_and_store_bind_code(self, user_email: str) -> BindCodeResponse:
        redis = await get_redis_client()
        code = ''.join(random.choices(string.digits, k=6))

        await redis.setex(f"tg_bind:{code}", 600, user_email)

        return BindCodeResponse(
            status="success",
            bind_code=code,
            expires_in_secs=600,
            bot_username="SignalHubAlert_bot"
        )

    async def process_telegram_webhook(self, chat_id: str, text: str) -> bool:
        if text.startswith("/bind"):
            parts = text.split()
            if len(parts) == 2:
                code = parts[1]
                redis = await get_redis_client()
                email = await redis.get(f"tg_bind:{code}")

                if email:
                    existing_user = await self.pool.fetchrow(
                        "SELECT wallet_address FROM signal_hub.users WHERE email = $1",
                        email
                    )

                    if existing_user:
                        await self.pool.execute(
                            "UPDATE signal_hub.users SET telegram_chat_id = $1 WHERE email = $2",
                            chat_id, email
                        )
                    else:
                        dummy_wallet = f"mock_{hash(email)}"
                        await self.pool.execute(
                            "INSERT INTO signal_hub.users (wallet_address, email, telegram_chat_id) VALUES ($1, $2, $3)",
                            dummy_wallet, email, chat_id
                        )

                    await redis.delete(f"tg_bind:{code}")

                    welcome_text = (
                        f"✅ **绑定成功！**\n\n"
                        f"您的邮箱 `{email}` 已成功关联此 Telegram 账号。\n"
                        f"Signal Hub 实时流处理引擎已就绪，您将在这里第一时间收到链上高危事件与智能追踪告警。"
                    )
                    await send_tg_message(chat_id, welcome_text)
                    return True
                else:
                    await send_tg_message(chat_id, "❌ **绑定失败**：验证码无效或已过期(10分钟)，请在网页端重新生成。")
                    return False
            else:
                await send_tg_message(chat_id, "ℹ️ 格式错误。请发送完整的绑定命令，例如：`/bind 123456`")
                return False

        elif text == "/start":
            start_text = (
                "👋 **欢迎使用 Signal Hub 流处理告警终端！**\n\n"
                "要开始接收告警，请在平台网页端获取 6 位绑定码，并在此回复：\n"
                "`/bind 您的验证码`"
            )
            await send_tg_message(chat_id, start_text)
            return True

        return False