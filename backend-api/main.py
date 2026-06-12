"""
SignalHub API - 唯一核心入口
不再包含任何业务逻辑，仅负责应用初始化与路由挂载。
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from core.config import settings
import logging
from core.redis_client import init_redis, close_redis
from core.db_postgres import pg_manager
logger = logging.getLogger(__name__)
from api.alerts import router as alerts_router
# 导入领域驱动下的各模块路由
from api.detective.router import router as detective_router
from api.signal.router import router as signal_router
# from api.dashboard.router import router as dashboard_router # 如果首页聚合也抽成了独立模块

from api.bot.router import router as bot_router
import asyncio
from workers.alert_router import AlertRouterWorker

logger = logging.getLogger(__name__)
alert_worker = AlertRouterWorker()
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ================= 启动阶段 (Startup) =================
    logger.info("Starting up FastAPI application...")
    await init_redis()
    await pg_manager.connect()

    router_task = asyncio.create_task(alert_worker.start())
    yield

    # ================= 关闭阶段 (Shutdown) =================
    logger.info("Shutting down FastAPI application...")
    await alert_worker.stop()
    router_task.cancel()
    await close_redis()
    await pg_manager.disconnect()

app = FastAPI(lifespan=lifespan)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# 挂载路由 (Routers)
app.include_router(detective_router)
app.include_router(signal_router)
app.include_router(alerts_router.router, prefix="/api/v1/alerts", tags=["Alerts"])
app.include_router(bot_router, prefix="/api/v1/bot", tags=["Telegram Bot"])
# app.include_router(dashboard_router)

@app.get("/health", tags=["System"])
async def health_check():
    """System health check endpoint."""
    return {"status": "healthy", "version": settings.VERSION}

if __name__ == "__main__":
    import uvicorn
    # 本地开发启动命令：python main.py
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)