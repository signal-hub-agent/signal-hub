from clickhouse_driver import Client
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_clickhouse_client() -> Client:
    # In a production environment, load these from environment variables
    return Client(
        host='localhost',
        port=9000,
        database='signal_hub',
        user='default',
        password=''
    )