import asyncio
import json
import random
import time
import uuid
from aiokafka import AIOKafkaProducer
from core.config import settings

async def mock_kafka_stream():
    # 获取 Kafka 地址
    bootstrap_servers = getattr(settings, "KAFKA_BROKERS", "localhost:9092")
    topic_name = "signal-hub-alerts"

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    await producer.start()
    print(f"🚀 开启 Kafka 实时流模拟器，目标 Topic: {topic_name}")
    print("按 Ctrl+C 停止发送...\n")

    event_types = ["SMART_SWAP", "WHALE_MOVEMENT", "ZERO_DAY", "LIQUIDITY", "BRIDGE"]
    tokens = ["MNT", "USDC", "WMNT", "USDT", "PENDLE", "LEND"]
    actions = ["Bought", "Sold", "Added", "Removed", "Bridged"]

    try:
        while True:
            # 随机生成一条告警
            event_type = random.choice(event_types)
            tx_hash = f"0x{uuid.uuid4().hex}"
            target_address = f"0x{uuid.uuid4().hex[:40]}"

            # 组装符合 Flink 输出规范的 JSON
            mock_alert = {
                "event_id": f"{tx_hash}-{event_type}",
                "target_address": target_address,
                "event_type": event_type,
                "chain_name": "mantle",
                "tx_hash": tx_hash,
                "timestamp": int(time.time() * 1000),
                "data": {
                    "usd_value": round(random.uniform(1000.0, 500000.0), 2),
                    "token_symbol": random.choice(tokens),
                    "flow_direction": random.choice(actions) if event_type != "ZERO_DAY" else "Interact"
                }
            }

            # 发送到 Kafka
            await producer.send_and_wait(topic_name, mock_alert)
            print(f"📡 已发送模拟信号: {event_type} - {mock_alert['data']['usd_value']} USD")

            # 随机等待 1 到 4 秒，模拟真实不均匀的链上数据流
            await asyncio.sleep(random.uniform(1.0, 4.0))

    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"❌ 发生错误: {e}")
    finally:
        await producer.stop()
        print("🛑 模拟器已停止。")

if __name__ == "__main__":
    asyncio.run(mock_kafka_stream())