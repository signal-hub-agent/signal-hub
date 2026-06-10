"""
SignalHub API - 唯一核心入口
不再包含任何业务逻辑，仅负责应用初始化与路由挂载。
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from core.config import settings

# 导入领域驱动下的各模块路由
from api.detective.router import router as detective_router
from api.signal.router import router as signal_router
# from api.dashboard.router import router as dashboard_router # 如果首页聚合也抽成了独立模块

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="AI-driven Web3 Copy-trade Signal Platform for the Mantle Ecosystem.",
)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载路由 (Routers)
app.include_router(detective_router)
app.include_router(signal_router)
# app.include_router(dashboard_router)

@app.get("/health", tags=["System"])
async def health_check():
    """System health check endpoint."""
    return {"status": "healthy", "version": settings.VERSION}

if __name__ == "__main__":
    import uvicorn
    # 本地开发启动命令：python main.py
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)