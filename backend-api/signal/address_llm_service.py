"""
Address LLM Analysis Service
Triggered when a user queries an address profile.
Combines on-chain financial metrics with token signal scores
to generate a comprehensive address portrait report.

Report is cached for 24 hours — real-time push does NOT call LLM.
"""
import os
import json
import logging
import requests
from datetime import datetime
from typing import Optional
import clickhouse_connect

from technical_analysis import run_technical_analysis

logger = logging.getLogger(__name__)

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL   = "deepseek-chat"


def get_api_key() -> str:
    key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not key:
        raise RuntimeError("DEEPSEEK_API_KEY environment variable not set")
    return key


def get_ch_client():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='123456',
    )


# ─────────────────────────────────────────────
# Step 1: Fetch recent trades for an address
# ─────────────────────────────────────────────

def get_recent_trades(address: str, limit: int = 10) -> list:
    """
    Fetches the most recent swaps for an address from clean_swaps.
    Returns a list of trade dicts with token symbols, amounts, and direction.
    """
    client = get_ch_client()
    addr = address.lower()

    try:
        query = """
            SELECT
                toDateTime(block_timestamp/1000) AS trade_time,
                token_in_symbol,
                token_out_symbol,
                token_in_amount,
                token_out_amount,
                amount_usd,
                tx_hash
            FROM signal_hub.clean_swaps
            WHERE lower(trader_address) = {addr:String}
              AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
            ORDER BY block_timestamp DESC
            LIMIT {limit:UInt32}
        """
        rows = client.query(query, parameters={"addr": addr, "limit": limit}).result_rows

        trades = []
        for row in rows:
            trade_time    = row[0]
            token_in      = row[1]
            token_out     = row[2]
            amount_usd    = float(row[5])
            tx_hash       = row[6]

            # Determine direction: if token_in is stablecoin → BUY, else SELL
            stables = {"USDT", "USDC", "DAI", "USDE", "MUSD"}
            if token_in.upper() in stables:
                direction = "buy"
                token     = token_out
            else:
                direction = "sell"
                token     = token_in

            trades.append({
                "time":       str(trade_time),
                "token":      token,
                "direction":  direction,
                "amount_usd": round(amount_usd, 2),
                "tx_hash":    tx_hash,
            })

        return trades

    except Exception as e:
        logger.warning("Failed to fetch recent trades for %s: %s", addr, str(e))
        return []


# ─────────────────────────────────────────────
# Step 2: Enrich trades with token SFS scores
# ─────────────────────────────────────────────

def enrich_trades_with_sfs(trades: list) -> list:
    """
    For each trade, fetch the current token signal score (SFS).
    Adds token_sfs and a brief signal summary to each trade dict.
    Only enriches BUY trades (those are the ones we evaluate timing quality on).
    """
    enriched = []
    seen_tokens = {}  # Cache to avoid re-analyzing same token

    for trade in trades:
        if trade["direction"] != "buy":
            enriched.append(trade)
            continue

        token = trade["token"].upper()

        if token not in seen_tokens:
            try:
                result = run_technical_analysis(token)
                seen_tokens[token] = {
                    "sfs_score":  result.tech_score,
                    "trend_type": result.ma.trend_type,
                    "macd_pos":   result.macd.dif_dea_position,
                    "rsi_zone":   result.rsi.zone,
                    "divergence": result.macd.divergence,
                }
            except Exception:
                seen_tokens[token] = {
                    "sfs_score":  None,
                    "trend_type": "unknown",
                    "macd_pos":   "unknown",
                    "rsi_zone":   "unknown",
                    "divergence": None,
                }

        trade["token_signal"] = seen_tokens[token]
        enriched.append(trade)

    return enriched


# ─────────────────────────────────────────────
# Step 3: Calculate trade quality stats
# ─────────────────────────────────────────────

def calc_trade_quality_stats(enriched_trades: list) -> dict:
    """
    Summarizes the signal quality at entry for all buy trades.
    Returns avg SFS at entry and the ratio of high-quality entries (SFS >= 70).
    """
    buy_trades_with_sfs = [
        t for t in enriched_trades
        if t["direction"] == "buy"
        and t.get("token_signal", {}).get("sfs_score") is not None
    ]

    if not buy_trades_with_sfs:
        return {
            "avg_entry_sfs":   None,
            "high_sfs_ratio":  None,
            "total_evaluated": 0,
            "explanation":     "Insufficient data to evaluate entry timing quality.",
        }

    scores = [t["token_signal"]["sfs_score"] for t in buy_trades_with_sfs]
    avg_sfs      = sum(scores) / len(scores)
    high_quality = sum(1 for s in scores if s >= 70)
    high_ratio   = high_quality / len(scores)

    return {
        "avg_entry_sfs":   round(avg_sfs, 1),
        "high_sfs_ratio":  round(high_ratio, 2),
        "total_evaluated": len(scores),
        "explanation":     (
            f"{int(high_ratio*100)}% of buy entries occurred when token SFS >= 70, "
            f"average entry SFS: {avg_sfs:.1f}"
        ),
    }


# ─────────────────────────────────────────────
# Step 4: Build LLM input payload
# ─────────────────────────────────────────────

def build_llm_payload(
    address: str,
    financials: dict,
    enriched_trades: list,
    trade_quality: dict,
    user_frequency: str = "mid_freq",
) -> dict:
    """
    Assembles the full structured input for the LLM prompt.
    Mirrors the schema defined in the product spec.
    """
    # Determine address frequency label
    avg_trades = financials.get("total_trades_30d", 0) / max(financials.get("active_days_30d", 1), 1)
    if avg_trades >= 10:
        freq_type = "high_freq"
    elif avg_trades >= 2:
        freq_type = "mid_freq"
    else:
        freq_type = "low_freq"

    # Simplify trade list for LLM (top 5 buy trades only)
    top_buys = [
        {
            "time":       t["time"],
            "token":      t["token"],
            "direction":  t["direction"],
            "amount_usd": t["amount_usd"],
            "token_sfs":  t.get("token_signal", {}).get("sfs_score"),
            "trend_type": t.get("token_signal", {}).get("trend_type"),
            "macd":       t.get("token_signal", {}).get("macd_pos"),
            "rsi_zone":   t.get("token_signal", {}).get("rsi_zone"),
        }
        for t in enriched_trades
        if t["direction"] == "buy"
    ][:5]

    return {
        "query_type": "address_profile",
        "address": address,
        "financials": {
            "profit_loss_ratio":  financials.get("profit_loss_ratio"),
            "sharpe_ratio":       financials.get("sharpe_ratio"),
            "win_rate":           financials.get("win_rate"),
            "max_drawdown":       financials.get("max_drawdown"),
            "account_growth_30d": financials.get("account_growth_30d"),
            "trade_count_30d":    financials.get("total_trades_30d"),
            "avg_trades_per_day": round(avg_trades, 2),
            "frequency_type":     freq_type,
            "total_volume_usd":   financials.get("total_volume_usd"),
        },
        "recent_buy_trades_with_signal": top_buys,
        "trade_quality_stats": trade_quality,
        "style_match": {
            "user_frequency":    user_frequency,
            "address_frequency": freq_type,
            "is_match":          user_frequency == freq_type,
        },
        "mev_flags": {
            "is_mev_bot": False  # TODO: connect MEV bot detection
        },
    }


# ─────────────────────────────────────────────
# Step 5: Call DeepSeek LLM
# ─────────────────────────────────────────────

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
   - If avg_entry_sfs >= 70: praise timing discipline
   - If avg_entry_sfs < 50: flag as reactive/momentum chaser
4. Assess style match with the user's frequency
5. Note any risks (high drawdown, low trade count, poor timing)
6. End with pre_rating justification
7. Never use absolute language like "guaranteed" or "will definitely"
8. Output must be valid JSON only — no markdown, no extra text"""


def call_llm(payload: dict) -> dict:
    """Calls DeepSeek API and returns parsed JSON response."""
    headers = {
        "Authorization": f"Bearer {get_api_key()}",
        "Content-Type":  "application/json",
    }

    user_prompt = f"""Generate an address profile report for this wallet data:

{json.dumps(payload, indent=2, default=str)}

Return valid JSON only."""

    body = {
        "model":       DEEPSEEK_MODEL,
        "messages":    [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_prompt},
        ],
        "max_tokens":  600,
        "temperature": 0.2,
    }

    resp = requests.post(DEEPSEEK_API_URL, headers=headers, json=body, timeout=30)
    resp.raise_for_status()

    raw = resp.json()["choices"][0]["message"]["content"].strip()

    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    return json.loads(raw)


# ─────────────────────────────────────────────
# Main Entry: Full Address Profile
# ─────────────────────────────────────────────

def generate_address_profile(
    address: str,
    financials: dict,
    user_frequency: str = "mid_freq",
) -> dict:
    """
    Full pipeline: fetch trades → enrich with SFS → build payload → call LLM.

    Args:
        address:        Wallet address to analyze
        financials:     Pre-computed metrics from detective_service (win_rate, etc.)
        user_frequency: The querying user's trading frequency for style matching

    Returns:
        dict with profile_report, pre_rating, style_label, copy_suggestion, etc.
    """
    logger.info("Generating address profile for %s", address)

    # 1. Get recent trades
    trades = get_recent_trades(address, limit=15)

    # 2. Enrich buy trades with token signal scores
    enriched = enrich_trades_with_sfs(trades)

    # 3. Calculate entry timing quality
    trade_quality = calc_trade_quality_stats(enriched)

    # 4. Build structured LLM input
    payload = build_llm_payload(
        address=address,
        financials=financials,
        enriched_trades=enriched,
        trade_quality=trade_quality,
        user_frequency=user_frequency,
    )

    # 5. Call LLM
    try:
        llm_result = call_llm(payload)
    except Exception as e:
        logger.error("LLM call failed for %s: %s", address, str(e))
        llm_result = {
            "profile_report":    "Analysis temporarily unavailable.",
            "pre_rating":        "yellow",
            "style_label":       "Unknown",
            "style_match":       False,
            "copy_suggestion":   "Please try again later.",
            "key_metrics_summary": "",
        }

    # 6. Merge LLM result with raw data
    return {
        "address":        address,
        "llm_analysis":   llm_result,
        "trade_quality":  trade_quality,
        "recent_trades":  enriched[:5],   # Return top 5 for frontend display
        "llm_input_data": payload,         # Useful for debugging
        "generated_at":   datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────
# Signal Push Score (no LLM, pure calculation)
# ─────────────────────────────────────────────

def calc_copy_signal_score(
    address_score: int,
    token_sfs: int,
    amount_usd: float,
    avg_trade_amount: float,
) -> dict:
    """
    Real-time copy-trade signal scoring — NO LLM, pure math.
    Called when a subscribed address makes a trade.

    Formula:
        copy_score = address_score * 0.4 + token_sfs * 0.4 + amount_signal * 0.2

    Amount signal:
        > 1.5x avg  → 100 (unusually large, high conviction)
        1.0-1.5x    → 70  (above average)
        0.5-1.0x    → 40  (normal)
        < 0.5x      → 10  (small test trade)
    """
    # Amount signal score
    if avg_trade_amount > 0:
        ratio = amount_usd / avg_trade_amount
    else:
        ratio = 1.0

    if ratio >= 1.5:
        amount_signal = 100
        amount_label  = "Unusually large — high conviction signal"
    elif ratio >= 1.0:
        amount_signal = 70
        amount_label  = "Above average size"
    elif ratio >= 0.5:
        amount_signal = 40
        amount_label  = "Normal size"
    else:
        amount_signal = 10
        amount_label  = "Small test trade — low conviction"

    copy_score = round(
        address_score * 0.4 +
        token_sfs     * 0.4 +
        amount_signal * 0.2
    )

    if copy_score >= 80:
        color = "green"
    elif copy_score >= 60:
        color = "yellow"
    else:
        color = "red"

    return {
        "copy_score":     copy_score,
        "color":          color,
        "address_score":  address_score,
        "token_sfs":      token_sfs,
        "amount_signal":  amount_signal,
        "amount_label":   amount_label,
        "amount_ratio":   round(ratio, 2),
    }


# ── Quick test ──
if __name__ == "__main__":
    test_addr = "0x0f95092b72aeb3feb8ea6c0dd96d141afd91c94e"

    # Mock financials (in production these come from detective_service)
    mock_financials = {
        "win_rate":           62.5,
        "profit_loss_ratio":  1.9,
        "sharpe_ratio":       1.4,
        "max_drawdown":       -18.2,
        "account_growth_30d": 22.1,
        "total_trades_30d":   38,
        "active_days_30d":    21,
        "total_volume_usd":   145000,
    }

    print(f"\n{'='*60}")
    print(f"  Address Profile: {test_addr[:20]}...")
    print(f"{'='*60}")

    result = generate_address_profile(
        address=test_addr,
        financials=mock_financials,
        user_frequency="mid_freq",
    )

    print("\n[LLM Analysis]")
    print(json.dumps(result["llm_analysis"], indent=2))

    print("\n[Trade Quality]")
    print(json.dumps(result["trade_quality"], indent=2))

    print("\n[Copy Signal Score Example]")
    score = calc_copy_signal_score(
        address_score=82,
        token_sfs=74,
        amount_usd=5200,
        avg_trade_amount=4800,
    )
    print(json.dumps(score, indent=2))