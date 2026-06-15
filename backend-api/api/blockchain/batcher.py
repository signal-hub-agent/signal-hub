import json
import time
import asyncio
import logging
import httpx
from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware
from aiokafka import AIOKafkaConsumer

from core.redis_client import get_redis_client
from core.config import settings

logger = logging.getLogger(__name__)

# ==========================================
# 启动入口 (车钥匙)
# ==========================================
async def main():
    batcher = BlockchainBatcher()

    # 使用 asyncio.gather 并发运行两个无限循环的守护进程
    logger.info("Starting Blockchain Batcher services...")
    await asyncio.gather(
        batcher.consume_kafka_to_queue(), # 负责搬运实时数据到 Redis
        batcher.run_daemon()              # 负责定时将 Redis 数据打包上链
    )

if __name__ == "__main__":
    # 配置基础的日志输出
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 启动异步事件循环
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Batcher gracefully shut down.")

class BlockchainBatcher:
    def __init__(self):
        self.QUEUE_KEY = "signalhub:chain_queue"
        self.LAST_PUBLISH_KEY = "signalhub:last_publish_time"
        self.INSIGHT_QUEUE_KEY = "signalhub:insight_queue"
        self.MAX_BATCH_SIZE = 50       # 满 50 条立刻上链
        self.MAX_DELAY_SECONDS = 3600  # 或者兜底 1 小时上链一次

        # Web3 初始化
        self.w3 = AsyncWeb3(AsyncHTTPProvider("https://rpc.sepolia.mantle.xyz"))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        # 🌟 从 env 获取私钥和合约地址
        self.private_key = settings.PUBLISHER_PRIVATE_KEY
        if not self.private_key:
            logger.warning("PUBLISHER_PRIVATE_KEY is not set in .env!")

        self.account = self.w3.eth.account.from_key(self.private_key)
        self.contract_address = settings.CONTRACT_ADDRESS

        # 智能合约的 ABI
        self.abi = [
            {"inputs":[{"components":[{"internalType":"string","name":"txHash","type":"string"},{"internalType":"address","name":"target","type":"address"},{"internalType":"string","name":"eventType","type":"string"}],"internalType":"struct SignalRegistry.AlertItem[]","name":"_alerts","type":"tuple[]"}],"name":"publishAlertBatch","outputs":[],"stateMutability":"nonpayable","type":"function"},
            {"inputs":[{"components":[{"internalType":"address","name":"targetAddress","type":"address"},{"internalType":"uint8","name":"aiScore","type":"uint8"},{"internalType":"string","name":"reason","type":"string"}],"internalType":"struct SignalRegistry.TopAddressItem[]","name":"_topAddresses","type":"tuple[]"}],"name":"publishTopAddresses","outputs":[],"stateMutability":"nonpayable","type":"function"}
        ]
        self.contract = self.w3.eth.contract(address=self.contract_address, abi=self.abi)

    async def _check_pool_liquidity(self, pool_address: str) -> float:
        """过滤土狗池，保证上链的数据都是有价值的"""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"https://api.dexscreener.com/latest/dex/pairs/mantle/{pool_address}", timeout=2.0)
                if resp.status_code == 200:
                    pairs = resp.json().get("pairs", [])
                    if pairs:
                        return float(pairs[0].get("liquidity", {}).get("usd", 0))
        except Exception:
            pass
        return 0.0

    async def consume_kafka_to_queue(self):
        """独立的数据搬运工：监听 Kafka 并推入 Redis 缓冲池"""
        kafka_broker = getattr(settings, "KAFKA_BROKERS", "localhost:9092")
        consumer = AIOKafkaConsumer(
            "signal-hub-alerts",
            bootstrap_servers=kafka_broker,
            group_id="blockchain-batcher-group", # 🌟 固定 Group ID，确保全局只有一份数据入队
            auto_offset_reset="latest"
        )

        await consumer.start()
        logger.info("🎧 Blockchain Batcher Kafka Consumer started.")

        try:
            async for msg in consumer:
                alert_data = json.loads(msg.value.decode('utf-8'))

                # 过滤无价值的 ZERO_DAY 土狗池，不让垃圾数据浪费 Gas
                if alert_data.get("event_type") == "ZERO_DAY":
                    pool_addr = alert_data.get("data", {}).get("pool_address")
                    if pool_addr and await self._check_pool_liquidity(pool_addr) < 100.0:
                        continue

                # 提取核心字段压缩体积，存入 Redis
                item = {
                    "txHash": alert_data.get("tx_hash", ""),
                    "target": alert_data.get("target_address", "0x0000000000000000000000000000000000000000"),
                    "eventType": alert_data.get("event_type", "UNKNOWN")
                }
                redis = await get_redis_client()
                await redis.lpush(self.QUEUE_KEY, json.dumps(item))

        except Exception as e:
            logger.error(f"Batcher Kafka error: {e}")
        finally:
            await consumer.stop()

    async def process_batch(self):
        """执行上链的核心逻辑"""
        redis = await get_redis_client()
        queue_len = await redis.llen(self.QUEUE_KEY)
        if queue_len == 0: return

        batch_size = min(queue_len, self.MAX_BATCH_SIZE)
        raw_items = await redis.rpop(self.QUEUE_KEY, count=batch_size)
        if not raw_items: return

        alerts_to_publish = [json.loads(item) for item in raw_items]
        logger.info(f"🚀 Publishing batch of {len(alerts_to_publish)} alerts to Mantle Testnet...")

        try:
            nonce = await self.w3.eth.get_transaction_count(self.account.address)
            tx = await self.contract.functions.publishAlertBatch(alerts_to_publish).build_transaction({
                'from': self.account.address,
                'nonce': nonce,
                'gas': 3000000,
                'gasPrice': await self.w3.eth.gas_price
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)

            await redis.set(self.LAST_PUBLISH_KEY, int(time.time()))
            logger.info(f"✅ Batch successfully submitted! Tx Hash: {tx_hash.hex()}")

        except Exception as e:
            logger.error(f"❌ Failed to publish batch: {e}")
            for item in raw_items:
                await redis.lpush(self.QUEUE_KEY, item) # 失败则退回队列

    async def process_insight_batch(self):
        """执行 AI Top 地址评分上链的核心逻辑"""
        redis = await get_redis_client()
        queue_len = await redis.llen(self.INSIGHT_QUEUE_KEY)
        if queue_len == 0: return

        # AI 数据通常是一次性计算出的 Top 10 或 Top 20，直接全部拉取
        raw_items = await redis.rpop(self.INSIGHT_QUEUE_KEY, count=queue_len)
        if not raw_items: return

        insights_to_publish = [json.loads(item) for item in raw_items]
        logger.info(f"🧠 Publishing AI Insight batch of {len(insights_to_publish)} addresses...")

        try:
            nonce = await self.w3.eth.get_transaction_count(self.account.address)
            tx = await self.contract.functions.publishTopAddresses(insights_to_publish).build_transaction({
                'from': self.account.address,
                'nonce': nonce,
                'gas': 2000000, # AI 数据结构稍大，预留足够的 Gas
                'gasPrice': await self.w3.eth.gas_price
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            logger.info(f"✅ AI Insights successfully submitted! Tx Hash: {tx_hash.hex()}")

        except Exception as e:
            logger.error(f"❌ Failed to publish AI insights: {e}")
            # 失败退回队列
            for item in raw_items:
                await redis.lpush(self.INSIGHT_QUEUE_KEY, item)

    async def run_daemon(self):
        """后台定时巡检任务 (双轨制)"""
        logger.info("🛡️ Blockchain Batcher Daemon started.")
        redis = await get_redis_client()

        while True:
            try:
                # 1. 检查实时告警队列
                queue_len = await redis.llen(self.QUEUE_KEY)
                last_publish = await redis.get(self.LAST_PUBLISH_KEY)
                last_publish = int(last_publish) if last_publish else 0
                time_since_last = time.time() - last_publish

                if queue_len >= self.MAX_BATCH_SIZE or (time_since_last >= self.MAX_DELAY_SECONDS and queue_len > 0):
                    await self.process_batch()

                # 2. 🌟 检查 AI 洞察队列
                # AI 分析属于定时任务产生的批数据，只要队列里有数据，就立刻上链
                insight_len = await redis.llen(self.INSIGHT_QUEUE_KEY)
                if insight_len > 0:
                    await self.process_insight_batch()

            except Exception as e:
                logger.error(f"Daemon error: {e}")

            await asyncio.sleep(60)

    # 2. 新增上链 AI 洞察的方法
    async def process_ai_insight_batch(self, ai_insights: list):
        """专门用于上链 AI 评分数据的函数"""
        try:
            nonce = await self.w3.eth.get_transaction_count(self.account.address)
            # 调用合约中的 publishTopAddresses 方法
            tx = await self.contract.functions.publishTopAddresses(ai_insights).build_transaction({
                'from': self.account.address,
                'nonce': nonce,
                'gas': 2000000,
                'gasPrice': await self.w3.eth.gas_price
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            logger.info(f"🧠 AI Insight batch submitted! Tx: {tx_hash.hex()}")
        except Exception as e:
            logger.error(f"❌ Failed to publish AI insights: {e}")