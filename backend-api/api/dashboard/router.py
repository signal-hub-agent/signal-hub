import uuid
import json
import asyncio
import logging
from datetime import datetime

import httpx
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from typing import List
from aiokafka import AIOKafkaConsumer

from core.redis_client import get_redis_client
from core.config import settings
from .service import DashboardService
from .schemas import KpiResponse, TokenRadarItem, SmartMoneyItem

logger = logging.getLogger(__name__)
router = APIRouter()

# ==========================================
# 接口 1：顶部 KPI 看板
# ==========================================
@router.get("/kpis", response_model=KpiResponse)
async def get_dashboard_kpis():
    redis_client = await get_redis_client()
    today_str = datetime.utcnow().strftime('%Y%m%d')

    keys = [
        f"kpi:smart_swaps:count:{today_str}", f"kpi:smart_swaps:sum:{today_str}",
        f"kpi:zero_day:count:{today_str}",
        f"kpi:whale_moves:count:{today_str}", f"kpi:whale_moves:sum:{today_str}",
        f"kpi:liquidity:count:{today_str}", f"kpi:liquidity:sum:{today_str}",
        f"kpi:bridges:count:{today_str}", f"kpi:bridges:sum:{today_str}"
    ]
    vals = await redis_client.mget(keys)

    return KpiResponse(
        smart_swaps_count=int(vals[0] or 0), smart_swaps_sum=float(vals[1] or 0.0),
        zero_day_count=int(vals[2] or 0),
        whale_moves_count=int(vals[3] or 0), whale_moves_sum=float(vals[4] or 0.0),
        liquidity_count=int(vals[5] or 0), liquidity_sum=float(vals[6] or 0.0),
        bridges_count=int(vals[7] or 0), bridges_sum=float(vals[8] or 0.0)
    )

# ==========================================
# 接口 2：左翼 - AI 代币异动雷达 (Top 10)
# ==========================================
@router.get("/tokens/top", response_model=List[TokenRadarItem])
async def get_top_tokens_radar():
    return await DashboardService.get_top_tokens_with_ai()

# ==========================================
# 接口 3：右翼 - AI 聪明钱侦探 (Top 10)
# ==========================================
@router.get("/smart-money/top", response_model=List[SmartMoneyItem])
async def get_top_smart_money():
    return await DashboardService.get_top_smart_money_with_ai()

# ==========================================
# 接口 4：中枢核心 - 全局实时异动 WebSocket
# ==========================================
# ... 保持顶部 import 不变 (你的代码里已经有 import httpx 和 asyncio 了) ...

@router.websocket("/stream")
async def dashboard_live_stream(websocket: WebSocket):
    await websocket.accept()
    logger.info("New dashboard WebSocket connection accepted.")

    kafka_broker = getattr(settings, "KAFKA_BROKERS", "localhost:9092")
    unique_group_id = f"dashboard-ws-{uuid.uuid4().hex}"
    consumer = AIOKafkaConsumer(
        "signal-hub-alerts",
        bootstrap_servers=kafka_broker,
        group_id=unique_group_id,
        auto_offset_reset="latest"
    )

    try:
        await consumer.start()
    except Exception as e:
        logger.error(f"WebSocket Kafka Consumer failed to start: {e}")
        await websocket.close(code=1011, reason="Backend Kafka connection failed")
        return

    # 🌟 新增一个内部函数：快速核实池子价值
    async def check_pool_liquidity(pool_address: str) -> float:
        try:
            # 调用 DexScreener 免费 API 查验该池子的实时数据
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"https://api.dexscreener.com/latest/dex/pairs/mantle/{pool_address}", timeout=2.0)
                if resp.status_code == 200:
                    data = resp.json()
                    pairs = data.get("pairs", [])
                    if pairs:
                        # 提取池子的流动性 USD 价值
                        return float(pairs[0].get("liquidity", {}).get("usd", 0))
        except Exception as e:
            logger.debug(f"Failed to verify pool liquidity for {pool_address}: {e}")
        return 0.0  # 查不到或接口超时，默认视为毫无价值的垃圾池

    try:
        async for msg in consumer:
            alert_data = json.loads(msg.value.decode('utf-8'))

            # ==========================================
            # 🌟 核心拦截器：清洗 ZERO_DAY 垃圾土狗池
            # ==========================================
            if alert_data.get("event_type") == "ZERO_DAY":
                pool_addr = alert_data.get("data", {}).get("pool_address")
                if pool_addr:
                    # 动态查验真实美元价值
                    pool_usd_value = await check_pool_liquidity(pool_addr)

                    if pool_usd_value < 100.0:
                        # 价值低于 $100，确认为无价值土狗池，直接拦截不推送！
                        logger.info(f"Filtered out junk ZERO_DAY pool: {pool_addr} (Liquidity: ${pool_usd_value})")
                        continue

                        # 如果是有价值的池子，顺便把真实的 USD 价值塞进去，让前端展示出来
                    alert_data["data"]["usd_value"] = pool_usd_value

            # 通过拦截器的数据，推送给前端
            await websocket.send_json(alert_data)
            await asyncio.sleep(0.1)

    except WebSocketDisconnect:
        logger.info("Dashboard WebSocket client disconnected.")
    except Exception as e:
        logger.error(f"Error in dashboard WebSocket loop: {e}")
    finally:
        await consumer.stop()