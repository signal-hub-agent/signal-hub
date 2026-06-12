import os
from typing import List
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "SignalForge API"
    VERSION: str = "1.0.0"

    # ClickHouse Configuration
    CH_HOST: str = os.getenv("CH_HOST", "localhost")
    CH_PORT: int = int(os.getenv("CH_PORT", 8123))
    CH_USER: str = os.getenv("CH_USER", "default")
    CH_PASSWORD: str = os.getenv("CH_PASSWORD", "")
    CH_DATABASE: str = os.getenv("CH_DATABASE", "signal_hub")

    # PostgreSQL Configuration
    PG_HOST: str = os.getenv("PG_HOST", "localhost")
    PG_PORT: int = int(os.getenv("PG_PORT", 5433))
    PG_USER: str = os.getenv("PG_USER", "postgres")
    PG_PASSWORD: str = os.getenv("PG_PASSWORD", "postgres")
    PG_DATABASE: str = os.getenv("PG_DATABASE", "signal_hub")

    # Redis Configuration
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # LLM Engine Configuration
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "your_llm_api_key_here")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o")

    # 🌟 Web3 / RPC High Availability Pool
    # Pydantic will parse a JSON-like string or a valid list directly
    MANTLE_RPC_URLS: List[str] = [
        "https://rpc.mantle.xyz",
        "https://mantle-rpc.publicnode.com",
        "https://mantle.public-rpc.com",
        "https://rpc.ankr.com/mantle"
    ]

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()