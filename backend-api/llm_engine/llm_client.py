"""
通用大模型客户端 (LLM Client)
职责：纯粹的 HTTP 请求封装、重试、错误兜底与 JSON 解析。无任何业务逻辑。
"""
import json
import logging
import requests
from core.config import settings

logger = logging.getLogger(__name__)

def get_api_key() -> str:
    key = settings.LLM_API_KEY
    if not key:
        raise RuntimeError("LLM_API_KEY environment variable not set")
    return key

async def request_llm_json(system_prompt: str, user_prompt: str, max_tokens: int = 600, temperature: float = 0.2) -> dict:
    """
    通用 LLM 请求方法，强制返回 JSON 解析后的字典。
    """
    headers = {
        "Authorization": f"Bearer {get_api_key()}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": settings.LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": temperature
    }

    try:
        resp = requests.post(settings.LLM_API_URL, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()

        raw_content = resp.json()["choices"][0]["message"]["content"].strip()

        # 剥离大模型可能带上的 Markdown 代码块标识符
        if raw_content.startswith("```"):
            raw_content = raw_content.split("```")[1]
            if raw_content.startswith("json"):
                raw_content = raw_content[4:]

        return json.loads(raw_content.strip())

    except Exception as e:
        logger.error(f"LLM API Call failed: {e}")
        # 抛出异常由上层 Service 处理默认兜底值
        raise