import asyncio
import json
from aiokafka import AIOKafkaProducer

async def send_mock_alert():
    # 连接到本地 Kafka
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()

    try:
        # 伪造一条满足你订阅规则的信号
        mock_alert = {
            "event_id": "0xmock_hash_123456789",
            "target_address": "0x17fbd10c4023f6df588bef7f96c200e3a423115c",
            "event_type": "WHALE_MOVEMENT",
            "chain_name": "mantle",
            "tx_hash": "0xmock_hash_123456789",
            "timestamp": 1718210000,
            "data": {
                "usd_value": 85000.50, # 确保大于你设置的阈值
                "token_symbol": "MNT",
                "dex_name": "merchant_moe",
                "flow_direction": "SWAP"
            }
        }

        print(f"Sending mock alert for {mock_alert['target_address']}...")
        await producer.send_and_wait(
            "signal-hub-alerts",
            json.dumps(mock_alert).encode('utf-8')
        )
        print("✅ Mock alert sent successfully!")

    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(send_mock_alert())