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
                        f"✅ **Binding successful!**\n\n"
                        f"Your email address `{email}` has been successfully linked to this Telegram account.\n"
                        f"The Signal Hub real-time stream processing engine is ready, and you will receive alerts for high-risk on-chain events and intelligent tracking here immediately."
                    )
                    await send_tg_message(chat_id, welcome_text)
                    return True
                else:
                    await send_tg_message(chat_id, "❌ **Binding failed**: The verification code is invalid or has expired (10 minutes). Please regenerate it on the web page.")
                    return False
            else:
                await send_tg_message(chat_id, "ℹ️ Incorrect format. Please send the complete binding command, for example: `/bind 123456`")
                return False

        elif text == "/start":
            start_text = (
                "👋 **Welcome to the Signal Hub Streaming Alarm Terminal!**\n\n"
                "To start receiving alarms, please obtain a 6-digit binding code from the platform's web interface and reply here:\n"
                "`/bind [Your verification code]`"
            )
            await send_tg_message(chat_id, start_text)
            return True

        return False