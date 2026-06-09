"""
Address Detective - Core Calculation Logic
Fills in the TODO sections of the teammate's detective/service.py
Query priority: gold_address_financials_daily -> clean_swaps -> fallback_probe
"""
import logging
from datetime import datetime
from typing import Optional
import clickhouse_connect

logger = logging.getLogger(__name__)


def get_ch_client():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='123456',
    )


# ─────────────────────────────────────────────
# Core Address Analysis
# ─────────────────────────────────────────────

def analyze_address_metrics(address: str) -> dict:
    """
    Builds a complete trading profile for a given wallet address.
    Returns a dict aligned with the teammate's AddressDetailResponse schema.

    Query priority:
    1. gold_address_financials_daily  (pre-computed, fastest)
    2. clean_swaps                    (real-time calculation, slower)
    3. fallback                       (no DEX history found)
    """
    client = get_ch_client()
    addr = address.lower()

    # Level 1: Try the pre-computed Gold layer first
    try:
        query_gold = """
            SELECT
                total_trades_30d,
                win_rate,
                profit_loss_ratio,
                sharpe_ratio,
                max_drawdown,
                total_volume_usd,
                active_days_30d,
                calc_date
            FROM signal_hub.gold_address_financials_daily
            WHERE lower(trader_address) = {addr:String}
            ORDER BY calc_date DESC
            LIMIT 1
        """
        row = client.query(query_gold, parameters={"addr": addr}).first_row

        if row and row[0] is not None and row[0] > 0:
            logger.info("Gold layer hit for address %s", addr)
            return _build_response_from_gold(addr, row)
    except Exception as e:
        logger.warning("Gold layer query failed for %s: %s", addr, str(e))

    # Level 2: Fall back to real-time calculation from clean_swaps
    try:
        swap_count = _get_swap_count(client, addr)
        if swap_count > 0:
            logger.info("Silver layer: %d swaps found for %s", swap_count, addr)
            return _calc_from_clean_swaps(client, addr, swap_count)
    except Exception as e:
        logger.warning("Silver layer query failed for %s: %s", addr, str(e))

    # Level 3: No DEX history — return degraded fallback response
    logger.info("No DEX history for %s, returning fallback response", addr)
    return _build_fallback_response(addr)


def _build_response_from_gold(addr: str, row) -> dict:
    """Constructs the full response from a pre-computed Gold layer row."""
    total_trades = int(row[0])
    win_rate     = float(row[1])
    pl_ratio     = float(row[2])
    sharpe       = float(row[3])
    max_drawdown = float(row[4])
    total_volume = float(row[5])
    active_days  = int(row[6])

    tags            = _calc_style_tags(total_trades, active_days, total_volume, win_rate)
    composite_score = _calc_composite_score(win_rate, pl_ratio, sharpe, max_drawdown, total_trades)
    risk_level, risk_score, risk_flags = _calc_risk(win_rate, max_drawdown, total_trades)

    return {
        "address": addr,
        "tags": tags,
        "composite_score": composite_score,
        "is_dex_trader": True,
        "metrics": {
            "win_rate": round(win_rate, 2),
            "profit_loss_ratio": round(pl_ratio, 2),
            "sharpe_ratio": round(sharpe, 2),
            "max_drawdown": round(max_drawdown, 2),
            "account_growth_30d": round(win_rate * pl_ratio / 10, 2),
            "total_trades_30d": total_trades,
            "total_volume_usd": round(total_volume, 2),
            "active_days_30d": active_days,
        },
        "risk": {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "flags": risk_flags,
        },
        "data_source": "gold_precomputed",
        "last_active": datetime.utcnow().isoformat(),
    }


def _get_swap_count(client, addr: str) -> int:
    """Returns the number of swaps for an address in the last 30 days."""
    query = """
        SELECT count()
        FROM signal_hub.clean_swaps
        WHERE lower(trader_address) = {addr:String}
          AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
    """
    row = client.query(query, parameters={"addr": addr}).first_row
    return int(row[0] or 0)


def _calc_from_clean_swaps(client, addr: str, swap_count: int) -> dict:
    """
    Real-time PnL calculation directly from signal_hub.clean_swaps.
    Uses token_in_amount, token_out_amount, and tx_fee_mnt for accurate results.

    Win/loss logic:
    - For each non-stablecoin token: (total sell USD) - (total buy USD) = PnL
    - Positive PnL -> win, Negative PnL -> loss
    """
    # Basic activity stats
    query_basic = """
        SELECT
            COUNT(DISTINCT toDate(toDateTime(block_timestamp/1000))) AS active_days,
            SUM(amount_usd) AS total_volume,
            MAX(toDateTime(block_timestamp/1000)) AS last_active
        FROM signal_hub.clean_swaps
        WHERE lower(trader_address) = {addr:String}
          AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
    """
    row = client.query(query_basic, parameters={"addr": addr}).first_row
    active_days  = int(row[0] or 0)
    total_volume = float(row[1] or 0)
    last_active  = row[2]

    # Sell-side: aggregate USD value of each non-stablecoin token sold
    query_sell = """
        SELECT token_out_symbol, SUM(amount_usd) AS sell_usd
        FROM signal_hub.clean_swaps
        WHERE lower(trader_address) = {addr:String}
          AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
          AND token_out_symbol NOT IN ('USDT','USDC','DAI','USDE')
        GROUP BY token_out_symbol
    """
    sell_rows = client.query(query_sell, parameters={"addr": addr}).result_rows

    # Buy-side: aggregate USD value of each non-stablecoin token bought
    query_buy = """
        SELECT token_in_symbol, SUM(amount_usd) AS buy_usd
        FROM signal_hub.clean_swaps
        WHERE lower(trader_address) = {addr:String}
          AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
          AND token_in_symbol NOT IN ('USDT','USDC','DAI','USDE')
        GROUP BY token_in_symbol
    """
    buy_rows = client.query(query_buy, parameters={"addr": addr}).result_rows

    # Calculate per-token PnL
    sell_map = {r[0]: float(r[1]) for r in sell_rows}
    buy_map  = {r[0]: float(r[1]) for r in buy_rows}

    wins = 0
    losses = 0
    total_profit = 0.0
    total_loss   = 0.0

    all_tokens = set(sell_map.keys()) | set(buy_map.keys())
    for token in all_tokens:
        sell_usd = sell_map.get(token, 0)
        buy_usd  = buy_map.get(token, 0)
        pnl = sell_usd - buy_usd
        if pnl > 0:
            wins += 1
            total_profit += pnl
        elif pnl < 0:
            losses += 1
            total_loss += abs(pnl)

    total_token_trades = wins + losses
    win_rate     = (wins / total_token_trades * 100) if total_token_trades > 0 else 0.0
    pl_ratio     = (total_profit / total_loss) if total_loss > 0 else total_profit
    sharpe       = min(pl_ratio * win_rate / 100, 3.0)   # Approximation
    max_drawdown = -(total_loss / max(total_volume, 1) * 100)
    growth_30d   = (total_profit - total_loss) / max(total_volume, 1) * 100

    tags            = _calc_style_tags(swap_count, active_days, total_volume, win_rate)
    composite_score = _calc_composite_score(win_rate, pl_ratio, sharpe, max_drawdown, swap_count)
    risk_level, risk_score, risk_flags = _calc_risk(win_rate, max_drawdown, swap_count)

    return {
        "address": addr,
        "tags": tags,
        "composite_score": composite_score,
        "is_dex_trader": True,
        "metrics": {
            "win_rate": round(win_rate, 2),
            "profit_loss_ratio": round(pl_ratio, 2),
            "sharpe_ratio": round(sharpe, 2),
            "max_drawdown": round(max_drawdown, 2),
            "account_growth_30d": round(growth_30d, 2),
            "total_trades_30d": swap_count,
            "total_volume_usd": round(total_volume, 2),
            "active_days_30d": active_days,
        },
        "risk": {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "flags": risk_flags,
        },
        "data_source": "clean_swaps_realtime",
        "last_active": str(last_active),
    }


def _build_fallback_response(addr: str) -> dict:
    """Returns a degraded response when no DEX history is found."""
    return {
        "address": addr,
        "tags": ["Non-DEX User", "On-Chain Holder"],
        "composite_score": 0,
        "is_dex_trader": False,
        "metrics": None,
        "risk": {
            "risk_level": "YELLOW",
            "risk_score": 5,
            "flags": ["No DEX trading history found in the last 30 days."],
        },
        "data_source": "fallback",
        "last_active": None,
    }


# ─────────────────────────────────────────────
# Scoring & Tagging Helpers
# ─────────────────────────────────────────────

def _calc_style_tags(trades: int, active_days: int, volume: float, win_rate: float) -> list:
    """Generates human-readable style tags based on trading behavior."""
    tags = []

    # Trading frequency
    avg = trades / max(active_days, 1)
    if avg >= 10:
        tags.append("High-Frequency")
    elif avg >= 2:
        tags.append("Mid-Frequency")
    else:
        tags.append("Low-Frequency")

    # Position size
    if volume >= 1_000_000:
        tags.append("Whale")
    elif volume >= 100_000:
        tags.append("Mid-Size Trader")
    else:
        tags.append("Retail")

    # Performance
    if win_rate >= 60:
        tags.append("High Win Rate")
    elif win_rate >= 40:
        tags.append("Balanced")
    else:
        tags.append("High Risk")

    return tags


def _calc_composite_score(
    win_rate: float,
    pl_ratio: float,
    sharpe: float,
    max_drawdown: float,
    trade_count: int,
) -> int:
    """
    Composite copy-trade score: 0-100
    Weights: Win Rate 40% | P/L Ratio 30% | Sharpe 20% | Drawdown 10%
    Confidence multiplier applied when trade count < 10.
    """
    wr_score     = min(win_rate / 100 * 40, 40)          # 0-40
    pl_score     = min(pl_ratio / 2.0 * 30, 30)          # 0-30, capped at ratio 2.0
    sharpe_score = min(sharpe / 1.5 * 20, 20)            # 0-20, capped at Sharpe 1.5
    dd_score     = max(10 + max_drawdown / 2, 0)          # 0-10, max_drawdown is negative

    # Low trade count = low statistical confidence
    confidence = min(trade_count / 10, 1.0)

    raw = (wr_score + pl_score + sharpe_score + dd_score) * confidence
    return min(int(raw), 100)


def _calc_risk(win_rate: float, max_drawdown: float, trade_count: int) -> tuple:
    """
    Returns (risk_level, risk_score, risk_flags).
    risk_level: GREEN / YELLOW / RED
    risk_score: 0-10 (higher = riskier)
    """
    flags      = []
    risk_score = 0

    if trade_count < 5:
        flags.append("Insufficient trade history (< 5 trades)")
        risk_score += 3

    if win_rate < 30:
        flags.append(f"Low win rate ({win_rate:.1f}%)")
        risk_score += 3

    if max_drawdown < -30:
        flags.append(f"High max drawdown ({max_drawdown:.1f}%)")
        risk_score += 3

    if not flags:
        flags.append("No significant risk flags detected")

    risk_score = min(risk_score, 10)

    if risk_score <= 3:
        risk_level = "GREEN"
    elif risk_score <= 6:
        risk_level = "YELLOW"
    else:
        risk_level = "RED"

    return risk_level, risk_score, flags


# ─────────────────────────────────────────────
# Top Traders Leaderboard
# ─────────────────────────────────────────────

def get_top_traders(limit: int = 100) -> list:
    """
    Fetches the top traders from gold_address_financials_daily,
    ranked by composite score. Requires at least 5 trades in 30 days.
    """
    client = get_ch_client()

    try:
        query = """
            SELECT
                trader_address,
                win_rate,
                profit_loss_ratio,
                sharpe_ratio,
                max_drawdown,
                total_trades_30d,
                total_volume_usd,
                active_days_30d
            FROM signal_hub.gold_address_financials_daily
            WHERE calc_date = (
                SELECT max(calc_date)
                FROM signal_hub.gold_address_financials_daily
            )
            AND total_trades_30d >= 5
            ORDER BY win_rate DESC, profit_loss_ratio DESC
            LIMIT {limit:UInt32}
        """
        rows = client.query(query, parameters={"limit": limit}).result_rows

        result = []
        for row in rows:
            addr        = row[0]
            win_rate    = float(row[1])
            pl_ratio    = float(row[2])
            sharpe      = float(row[3])
            max_dd      = float(row[4])
            trades      = int(row[5])
            volume      = float(row[6])
            active_days = int(row[7])

            score = _calc_composite_score(win_rate, pl_ratio, sharpe, max_dd, trades)
            tags  = _calc_style_tags(trades, active_days, volume, win_rate)

            result.append({
                "address": addr,
                "composite_score": score,
                "tags": tags,
                "win_rate": round(win_rate, 2),
                "total_volume_30d": round(volume, 2),
            })

        result.sort(key=lambda x: x["composite_score"], reverse=True)
        return result

    except Exception as e:
        logger.error("Failed to get top traders: %s", str(e))
        return []


# ── Quick test ──
if __name__ == "__main__":
    import json

    test_addr = "0x0f95092b72aeb3feb8ea6c0dd96d141afd91c94e"
    print(f"\n{'='*55}")
    print(f"  Address: {test_addr[:20]}...")
    print(f"{'='*55}")
    result = analyze_address_metrics(test_addr)
    print(json.dumps(result, indent=2, default=str))

    print(f"\n{'='*55}")
    print("  Top Traders (limit 5)")
    print(f"{'='*55}")
    top = get_top_traders(limit=5)
    print(json.dumps(top, indent=2, default=str))