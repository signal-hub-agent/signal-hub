"""
DeepSeek LLM Analysis Service 集成模块
Triggered when a user queries an address profile (Cache Miss).
Combines on-chain financial metrics with token signal scores.
"""
import os
import json
import logging
import requests
from datetime import datetime
from typing import Optional
from core.config import settings

# 复用后端已实现的全局数据库连接
from core.db_clickhouse import get_ch_client

logger = logging.getLogger(__name__)

DEEPSEEK_API_URL = settings.LLM_API_URL
DEEPSEEK_MODEL   = settings.LLM_MODEL

def get_api_key() -> str:
    key = settings.LLM_API_KEY
    if not key:
        raise RuntimeError("DEEPSEEK_API_KEY environment variable not set")
    return key

# ================= 原样迁移的交易明细获取 =================
def get_recent_trades(address: str, limit: int = 10) -> list:
    client = get_ch_client()
    addr = address.lower()
    try:
        query = """
            SELECT
                toDateTime(block_timestamp/1000) AS trade_time,
                token_in_symbol, token_out_symbol, token_in_amount, token_out_amount, amount_usd, tx_hash
            FROM signal_hub.clean_swaps
            WHERE lower(trader_address) = {addr:String} AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
            ORDER BY block_timestamp DESC LIMIT {limit:UInt32}
        """
        rows = client.query(query, parameters={"addr": addr, "limit": limit}).result_rows
        trades = []
        for row in rows:
            stables = {"USDT", "USDC", "DAI", "USDE", "MUSD"}
            direction = "buy" if row[1].upper() in stables else "sell"
            token = row[2] if direction == "buy" else row[1]
            trades.append({"time": str(row[0]), "token": token, "direction": direction, "amount_usd": round(float(row[5]), 2), "tx_hash": row[6]})
        return trades
    except Exception as e:
        logger.warning("Failed to fetch recent trades for %s: %s", addr, str(e))
        return []

# ================= 修复：从 ClickHouse 查 SFS，而不是现场跑 Pandas =================
def enrich_trades_with_sfs(trades: list) -> list:
    client = get_ch_client()
    enriched = []
    seen_tokens = {}

    for trade in trades:
        if trade["direction"] != "buy":
            enriched.append(trade)
            continue

        token = trade["token"].upper()
        if token not in seen_tokens:
            try:
                # 不再调 run_technical_analysis，而是直接读取离线计算好的结果
                query = """
                    SELECT tech_score, ma_trend_type, macd_position, rsi_zone 
                    FROM signal_hub.gold_token_metrics_1h 
                    WHERE upper(token_symbol) = {token:String} 
                    ORDER BY calc_time DESC LIMIT 1
                """
                row = client.query(query, parameters={"token": token}).first_row
                if row:
                    seen_tokens[token] = {
                        "sfs_score":  int(row[0]) if row[0] is not None else None,
                        "trend_type": row[1] or "unknown",
                        "macd_pos":   row[2] or "unknown",
                        "rsi_zone":   row[3] or "unknown",
                        "divergence": None,
                    }
                else:
                    raise Exception("No pre-computed SFS found in gold table")
            except Exception as e:
                logger.debug("Failed to fetch SFS from CH for %s: %s", token, str(e))
                seen_tokens[token] = {
                    "sfs_score":  None, "trend_type": "unknown",
                    "macd_pos":   "unknown", "rsi_zone":   "unknown", "divergence": None,
                }
        trade["token_signal"] = seen_tokens[token]
        enriched.append(trade)
    return enriched

def calc_trade_quality_stats(enriched_trades: list) -> dict:
    buy_trades_with_sfs = [t for t in enriched_trades if t["direction"] == "buy" and t.get("token_signal", {}).get("sfs_score") is not None]
    if not buy_trades_with_sfs:
        return {"avg_entry_sfs": None, "high_sfs_ratio": None, "total_evaluated": 0, "explanation": "Insufficient data"}
    scores = [t["token_signal"]["sfs_score"] for t in buy_trades_with_sfs]
    high_ratio = sum(1 for s in scores if s >= 70) / len(scores)
    return {"avg_entry_sfs": round(sum(scores) / len(scores), 1), "high_sfs_ratio": round(high_ratio, 2), "total_evaluated": len(scores), "explanation": f"{int(high_ratio*100)}% >= 70 SFS"}

def build_llm_payload(address: str, financials: dict, enriched_trades: list, trade_quality: dict, user_frequency: str = "mid_freq") -> dict:
    avg_trades = financials.get("total_trades_30d", 0) / max(financials.get("active_days_30d", 1), 1)
    if avg_trades >= 10: freq_type = "high_freq"
    elif avg_trades >= 2: freq_type = "mid_freq"
    else: freq_type = "low_freq"

    top_buys = [
        {
            "time": t["time"], "token": t["token"], "direction": t["direction"], "amount_usd": t["amount_usd"],
            "token_sfs": t.get("token_signal", {}).get("sfs_score"),
            "trend_type": t.get("token_signal", {}).get("trend_type"),
            "macd": t.get("token_signal", {}).get("macd_pos"),
            "rsi_zone": t.get("token_signal", {}).get("rsi_zone"),
        }
        for t in enriched_trades if t["direction"] == "buy"
    ][:5]

    return {
        "query_type": "address_profile", "address": address,
        "financials": {
            "profit_loss_ratio": financials.get("profit_loss_ratio"), "sharpe_ratio": financials.get("sharpe_ratio"),
            "win_rate": financials.get("win_rate"), "max_drawdown": financials.get("max_drawdown"),
            "account_growth_30d": financials.get("account_growth_30d"), "trade_count_30d": financials.get("total_trades_30d"),
            "avg_trades_per_day": round(avg_trades, 2), "frequency_type": freq_type, "total_volume_usd": financials.get("total_volume_usd"),
        },
        "recent_buy_trades_with_signal": top_buys, "trade_quality_stats": trade_quality,
        "style_match": {"user_frequency": user_frequency, "address_frequency": freq_type, "is_match": user_frequency == freq_type},
        "mev_flags": {"is_mev_bot": False}
    }

SYSTEM_PROMPT = """You are an on-chain trading behavior analyst for the Mantle DeFi ecosystem.
Your job is to write a concise, data-driven address profile report for copy-traders.

Output strictly as JSON with these fields:
{
  "profile_report": "~150 word natural language analysis in English",
  "pre_rating": "green / yellow / red",
  "style_label": "e.g. Mid-Frequency Swing Trader",
  "style_match": true / false,
  "copy_suggestion": "one sentence recommendation",
  "key_metrics_summary": "Win Rate X% | P/L Ratio Y | Avg Entry SFS Z"
}

Rules for profile_report:
1. Open with one sentence defining the trader's style and reliability
2. Cite specific numbers: win rate, P/L ratio, max drawdown
3. Evaluate entry timing quality using the trade_quality_stats field
4. Assess style match with the user's frequency
5. Note any risks (high drawdown, low trade count, poor timing)
6. End with pre_rating justification
7. Never use absolute language like "guaranteed" or "will definitely"
8. Output must be valid JSON only — no markdown, no extra text"""

async def generate_address_profile(address: str, financials: dict, user_frequency: str = "mid_freq") -> dict:
    logger.info("Generating address profile for %s", address)
    trades = get_recent_trades(address, limit=15)
    enriched = enrich_trades_with_sfs(trades)
    trade_quality = calc_trade_quality_stats(enriched)
    payload = build_llm_payload(address=address, financials=financials, enriched_trades=enriched, trade_quality=trade_quality, user_frequency=user_frequency)

    headers = {"Authorization": f"Bearer {get_api_key()}", "Content-Type": "application/json"}
    user_prompt = f"Generate an address profile report for this wallet data:\n\n{json.dumps(payload, indent=2, default=str)}\n\nReturn valid JSON only."
    body = {"model": DEEPSEEK_MODEL, "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_prompt}], "max_tokens": 600, "temperature": 0.2}

    try:
        resp = requests.post(DEEPSEEK_API_URL, headers=headers, json=body, timeout=30)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        llm_result = json.loads(raw.strip())
    except Exception as e:
        logger.error("LLM call failed for %s: %s", address, str(e))
        llm_result = {"profile_report": "Analysis temporarily unavailable.", "pre_rating": "yellow", "style_label": "Unknown", "style_match": False, "copy_suggestion": "Please try again later.", "key_metrics_summary": ""}

    return {
        "address": address, "llm_analysis": llm_result, "trade_quality": trade_quality,
        "recent_trades": enriched[:5], "llm_input_data": payload, "generated_at": datetime.utcnow().isoformat(),
    }

def calc_copy_signal_score(address_score: int, token_sfs: int, amount_usd: float, avg_trade_amount: float) -> dict:
    ratio = amount_usd / avg_trade_amount if avg_trade_amount > 0 else 1.0
    if ratio >= 1.5: amount_signal = 100; amount_label = "Unusually large — high conviction signal"
    elif ratio >= 1.0: amount_signal = 70; amount_label = "Above average size"
    elif ratio >= 0.5: amount_signal = 40; amount_label = "Normal size"
    else: amount_signal = 10; amount_label = "Small test trade — low conviction"

    copy_score = round(address_score * 0.4 + token_sfs * 0.4 + amount_signal * 0.2)
    color = "green" if copy_score >= 80 else ("yellow" if copy_score >= 60 else "red")
    return {"copy_score": copy_score, "color": color, "address_score": address_score, "token_sfs": token_sfs, "amount_signal": amount_signal, "amount_label": amount_label, "amount_ratio": round(ratio, 2)}

# ================= Token 信号简报生成 =================
TOKEN_SYSTEM_PROMPT = """You are a professional crypto technical analyst writing a concise daily market brief.

Step 1: Determine overall bias using this priority order:

BEARISH if ALL of these are true:
- EMA alignment is bearish (EMA99 > EMA25 > EMA7) AND gap > 0.3%
- Price is below EMA25
- MACD DIF is below zero

BULLISH if ALL of these are true:
- EMA alignment is bullish (EMA7 > EMA25 > EMA99) AND gap > 0.3%
- Price is above EMA25
- MACD DIF is above zero

NEUTRAL only when NONE of the above conditions are met:
- EMAs are compressing or tangled
- Price is near EMA25 (within 1%)
- MACD DIF is near zero

Step 2: Output strictly as JSON.
Format:
{
  "bias": "BULLISH" | "BEARISH" | "NEUTRAL",
  "summary": "One sentence overall summary",
  "bullish_factors": ["Point 1", "Point 2"],
  "bearish_factors": ["Point 1", "Point 2"],
  "support_levels": [1.23, 1.20],
  "resistance_levels": [1.30, 1.35]
}

Rules:
- Each bullet point is ONE sentence with specific values
- Support/Resistance must use actual values from the data
- Never say "will rise" or "guaranteed"
"""

async def generate_token_signal_report(token_data: dict) -> str:
    logger.info("Calling DeepSeek for token signal analysis...")
    headers = {"Authorization": f"Bearer {get_api_key()}", "Content-Type": "application/json"}
    prompt = f"Write a technical brief for this token data.\nFollow the exact format from your instructions.\n\nData:\n{json.dumps(token_data, indent=2, default=str)}"
    payload = {"model": DEEPSEEK_MODEL, "messages": [{"role": "system", "content": TOKEN_SYSTEM_PROMPT}, {"role": "user", "content": prompt}], "max_tokens": 500, "temperature": 0.2}

    try:
        resp = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        return raw.strip()
    except Exception as e:
        logger.error(f"Failed to generate token report: {e}")
        return json.dumps({"summary": "Technical analysis temporarily unavailable.", "bias": "NEUTRAL"})