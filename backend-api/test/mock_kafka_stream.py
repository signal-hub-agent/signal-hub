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

    # 🌟 你订阅的测试地址
    subscribed_addresses = [
        "0x17fbd10c4023f6df588bef7f96c200e3a423115c",
        "0xd36213af34089b66c0d62166661f9f0337181efb"
    ]

    try:
        while True:
            # 随机生成一条告警
            event_type = random.choice(event_types)
            tx_hash = f"0x{uuid.uuid4().hex}"

            # 🌟 50% 的概率使用订阅的地址，50% 的概率生成随机地址
            if random.random() < 0.5:
                target_address = random.choice(subscribed_addresses)
                is_sub = "⭐[订阅地址]"
            else:
                target_address = f"0x{uuid.uuid4().hex[:40]}"
                is_sub = "  [随机地址]"

            # 组装符合 Flink 输出规范的 JSON
            mock_alert = {
                "event_id": f"{tx_hash}-{event_type}",
                "target_address": target_address,
                "event_type": event_type,
                "chain_name": "mantle",
                "tx_hash": tx_hash,
                "timestamp": int(time.time() * 1000),
                "data": {
                    # 为了确保能突破你的 Telegram 预警阈值，我把这里的最低金额稍微调高了一点
                    "usd_value": round(random.uniform(5000.0, 500000.0), 2),
                    "token_symbol": random.choice(tokens),
                    "flow_direction": random.choice(actions) if event_type != "ZERO_DAY" else "Interact"
                }
            }

            # 发送到 Kafka
            await producer.send_and_wait(topic_name, mock_alert)
            print(f"📡 已发送 {is_sub}: {event_type} - {mock_alert['data']['usd_value']} USD (地址: {target_address[:10]}...)")

            # 随机等待 1 到 4 秒（原先这里是 10 到 30 秒，为了方便你快速测试 TG，我改成了 2 到 5 秒）
            await asyncio.sleep(random.uniform(10.0, 20.0))

    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"❌ 发生错误: {e}")
    finally:
        await producer.stop()
        print("🛑 模拟器已停止。")

if __name__ == "__main__":
    asyncio.run(mock_kafka_stream())