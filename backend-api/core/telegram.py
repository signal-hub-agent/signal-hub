import httpx
import logging
from core.config import settings

logger = logging.getLogger(__name__)

TG_API_URL = f"https://api.telegram.org/bot{settings.TG_BOT_TOKEN}"

async def send_tg_message(chat_id: str, text: str) -> bool:
    """向指定的 Telegram chat_id 发送 Markdown 格式的消息"""
    if not settings.TG_BOT_TOKEN:
        logger.error("TG_BOT_TOKEN is not configured in settings!")
        return False

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{TG_API_URL}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "Markdown"
                },
                timeout=10.0
            )
            response.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            logger.error(f"Telegram API HTTP error: {e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"Failed to send TG message to {chat_id}: {str(e)}")
            return False