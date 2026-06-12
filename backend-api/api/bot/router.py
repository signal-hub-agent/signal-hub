from fastapi import APIRouter, Request, Depends
import logging
from core.db_postgres import pg_manager
from .schemas import BindCodeResponse
from .service import BotService

logger = logging.getLogger(__name__)
router = APIRouter()

async def get_db_pool():
    return pg_manager.get_pool()

def get_bot_service(pool = Depends(get_db_pool)):
    return BotService(pool)

@router.get("/bind-code", response_model=BindCodeResponse, summary="生成 Telegram 绑定验证码")
async def generate_bind_code(user_email: str, service: BotService = Depends(get_bot_service)):
    return await service.generate_and_store_bind_code(user_email)

@router.post("/webhook", summary="接收 Telegram 消息回调")
async def telegram_webhook(request: Request, service: BotService = Depends(get_bot_service)):
    try:
        data = await request.json()
    except Exception as e:
        logger.warning(f"Invalid JSON received in webhook: {e}")
        return {"ok": True}

    if "message" in data and "text" in data["message"]:
        chat_id = str(data["message"]["chat"]["id"])
        text = data["message"]["text"].strip()
        await service.process_telegram_webhook(chat_id, text)

    return {"ok": True}