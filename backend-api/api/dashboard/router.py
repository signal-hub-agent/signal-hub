# backend-api/api/dashboard/router.py
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import List, Optional
import httpx
import json
import asyncio
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer

from core.redis_client import get_redis_client
from core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()

# ClickHouse HTTP 接口地址 (依据你的 docker-compose 配置)
CH_HTTP_URL = "http://localhost:8123/"
CH_USER = "default"
CH_PASSWORD = ""

# ==========================================
# 内部工具：异步查询 ClickHouse (防阻塞)
# ==========================================
async def _async_ch_query(query: str) -> dict:
    # 强制让 ClickHouse 返回容易解析的 JSON 格式
    formatted_query = f"{query} FORMAT JSON"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                CH_HTTP_URL,
                data=formatted_query.encode('utf-8'),
                auth=(CH_USER, CH_PASSWORD) if CH_PASSWORD else None,
                timeout=5.0
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"ClickHouse async query failed: {e}")
            return {"data": []}

# ==========================================
# 数据契约 (Schemas)
# ==========================================
class KpiResponse(BaseModel):
    volume_24h_usd: float
    active_smart_money_count: int
    alerts_today: int

class TokenRadarItem(BaseModel):
    symbol: str
    volume_1h_usd: float
    mev_toxicity_pct: float
    ai_insight: str

class SmartMoneyItem(BaseModel):
    address: str
    win_rate: float
    pnl_ratio: float
    tags: List[str]
    ai_profiling: str

# ==========================================
# 接口 1：顶部 KPI 看板
# ==========================================
@router.get("/kpis", response_model=KpiResponse, summary="获取大屏顶部全局 KPI")
async def get_dashboard_kpis():
    # 1. 查 24H 监控资金总量
    vol_query = """
        SELECT sum(amount_usd) as total_vol 
        FROM signal_hub.clean_swaps 
        WHERE toDateTime(block_timestamp/1000) >= now() - INTERVAL 1 DAY
    """
    vol_data = await _async_ch_query(vol_query)
    total_vol = vol_data["data"][0]["total_vol"] if vol_data["data"] else 0.0

    # 2. 查今日活跃的高分聪明钱数量
    sm_query = """
        SELECT count(DISTINCT trader_address) as sm_count 
        FROM signal_hub.gold_address_financials_daily 
        WHERE calc_date = today() AND composite_score >= 80
    """
    sm_data = await _async_ch_query(sm_query)
    sm_count = sm_data["data"][0]["sm_count"] if sm_data["data"] else 0

    # 3. 查今日告警数 (从 Redis)
    redis_client = await get_redis_client()
    today_str = datetime.utcnow().strftime('%Y%m%d')
    alerts_count = await redis_client.get(f"dashboard:alerts_today:{today_str}")
    alerts_count = int(alerts_count) if alerts_count else 0

    return KpiResponse(
        volume_24h_usd=float(total_vol or 0),
        active_smart_money_count=int(sm_count or 0),
        alerts_today=alerts_count
    )

# ==========================================
# 接口 2：左翼 - AI 代币异动雷达
# ==========================================
@router.get("/tokens/top", response_model=List[TokenRadarItem], summary="获取 Top 5 异动代币及 AI 简评")
async def get_top_tokens_radar():
    # 使用 argMax 获取每个代币最新一小时的数据，并按交易量排序取前 5
    query = """
        SELECT 
            token_symbol, 
            argMax(volume_24h_usd, calc_time) as vol, 
            argMax(mev_toxicity_pct, calc_time) as mev 
        FROM signal_hub.gold_token_metrics_1h 
        GROUP BY token_symbol 
        ORDER BY vol DESC LIMIT 5
    """
    ch_result = await _async_ch_query(query)
    rows = ch_result.get("data", [])

    redis_client = await get_redis_client()
    result_list = []

    for row in rows:
        symbol = row["token_symbol"]

        # 🌟 从 Redis 提取大模型针对该代币生成的最新一句话简评
        ai_insight_raw = await redis_client.get(f"llm:token:{symbol}")
        ai_insight = ai_insight_raw.decode('utf-8') if ai_insight_raw else "AI 检测中：近期交易活跃，建议持续关注其资金流向。"

        result_list.append(TokenRadarItem(
            symbol=symbol,
            volume_1h_usd=float(row["vol"]),
            mev_toxicity_pct=float(row["mev"]),
            ai_insight=ai_insight
        ))

    return result_list

# ==========================================
# 接口 3：右翼 - AI 聪明钱侦探
# ==========================================
@router.get("/smart-money/top", response_model=List[SmartMoneyItem], summary="获取 Top 5 聪明钱及 AI 画像")
async def get_top_smart_money():
    query = """
        SELECT 
            trader_address, win_rate, profit_loss_ratio, style_tags 
        FROM signal_hub.gold_address_financials_daily 
        WHERE calc_date = today() 
        ORDER BY composite_score DESC LIMIT 5
    """
    ch_result = await _async_ch_query(query)
    rows = ch_result.get("data", [])

    redis_client = await get_redis_client()
    result_list = []

    for row in rows:
        address = row["trader_address"]
        # ClickHouse 的 Array(String) 转 JSON 可能会带单引号等，简单清洗
        tags_raw = row["style_tags"]
        tags = tags_raw if isinstance(tags_raw, list) else []

        # 🌟 从 Redis 提取大模型针对该地址生成的最新画像总结
        ai_profiling_raw = await redis_client.get(f"llm:address:{address}")
        ai_profiling = ai_profiling_raw.decode('utf-8') if ai_profiling_raw else "AI 画像提取中：近期胜率稳定，建议观察其建仓周期。"

        result_list.append(SmartMoneyItem(
            address=address,
            win_rate=float(row["win_rate"]),
            pnl_ratio=float(row["profit_loss_ratio"]),
            tags=tags,
            ai_profiling=ai_profiling
        ))

    return result_list

# ==========================================
# 接口 4：中枢核心 - 全局实时异动 WebSocket
# ==========================================
@router.websocket("/stream")
async def dashboard_live_stream(websocket: WebSocket):
    await websocket.accept()
    logger.info("New dashboard WebSocket connection accepted.")

    # 假设 Kafka 运行在本地。如果用 Docker 互联，可能需要改成 settings.KAFKA_BROKERS (如 kafka:29092)
    kafka_broker = getattr(settings, "KAFKA_BROKERS", "localhost:9092")

    consumer = AIOKafkaConsumer(
        "signal-hub-alerts", # 直接订阅 Flink 吐出来的告警 Topic
        bootstrap_servers=kafka_broker,
        group_id="frontend-dashboard-ws-group",
        auto_offset_reset="latest" # 大屏必须实时，不能把历史数据一股脑推给前端导致卡死
    )

    try:
        await consumer.start()
    except Exception as e:
        logger.error(f"WebSocket Kafka Consumer failed to start: {e}")
        await websocket.close(code=1011, reason="Backend Kafka connection failed")
        return

    try:
        async for msg in consumer:
            # 获取 Flink 解析好的 JSON
            alert_data = json.loads(msg.value.decode('utf-8'))

            # 直接推送给前端
            await websocket.send_json(alert_data)

            # 添加 100ms 延迟，给前端 Vue/React 渲染留出呼吸空间，防止高并发下浏览器卡顿
            await asyncio.sleep(0.1)

    except WebSocketDisconnect:
        logger.info("Dashboard WebSocket client disconnected.")
    except Exception as e:
        logger.error(f"Error in dashboard WebSocket loop: {e}")
    finally:
        await consumer.stop()