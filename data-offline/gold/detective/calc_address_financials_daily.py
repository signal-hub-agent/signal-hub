"""
Offline Batch Job: Daily Address Financial Metrics Calculation
Extracts data from clean_swaps, performs quantitative calculations,
and writes to gold_address_financials_daily.
"""
import os
import sys
import logging
from datetime import datetime

# 🌟 动态将 data-offline 目录加入系统路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)

# 🌟 使用项目中统一的数据库连接配置
from config.database import get_clickhouse_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= Business Logic (Unchanged) =================

def _calc_style_tags(trades: int, active_days: int, volume: float, win_rate: float) -> list:
    tags = []
    avg = trades / max(active_days, 1)
    if avg >= 10: tags.append("High-Frequency")
    elif avg >= 2: tags.append("Mid-Frequency")
    else: tags.append("Low-Frequency")

    if volume >= 1_000_000: tags.append("Whale")
    elif volume >= 100_000: tags.append("Mid-Size Trader")
    else: tags.append("Retail")

    if win_rate >= 60: tags.append("High Win Rate")
    elif win_rate >= 40: tags.append("Balanced")
    else: tags.append("High Risk")
    return tags

def _calc_composite_score(win_rate: float, pl_ratio: float, sharpe: float, max_drawdown: float, trade_count: int) -> int:
    wr_score     = min(win_rate / 100 * 40, 40)
    pl_score     = min(pl_ratio / 2.0 * 30, 30)
    sharpe_score = min(sharpe / 1.5 * 20, 20)
    dd_score     = max(10 + max_drawdown / 2, 0)
    confidence = min(trade_count / 10, 1.0)
    raw = (wr_score + pl_score + sharpe_score + dd_score) * confidence
    return min(int(raw), 100)

def _calc_risk(win_rate: float, max_drawdown: float, trade_count: int) -> tuple:
    flags = []
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
    if risk_score <= 3: risk_level = "GREEN"
    elif risk_score <= 6: risk_level = "YELLOW"
    else: risk_level = "RED"
    return risk_level, risk_score, flags

# ================= Calculation from Silver Layer =================

def _get_swap_count(client, addr: str) -> int:
    query = """
        SELECT count() FROM signal_hub.clean_swaps
        WHERE lower(trader_address) = %(addr)s AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
    """
    rows = client.execute(query, {'addr': addr})
    return int(rows[0][0]) if rows else 0

def _calc_from_clean_swaps(client, addr: str, swap_count: int) -> dict:
    query_basic = """
        SELECT COUNT(DISTINCT toDate(toDateTime(block_timestamp/1000))) AS active_days,
               SUM(amount_usd) AS total_volume, MAX(toDateTime(block_timestamp/1000)) AS last_active
        FROM signal_hub.clean_swaps
        WHERE lower(trader_address) = %(addr)s AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
    """
    basic_rows = client.execute(query_basic, {'addr': addr})
    row = basic_rows[0] if basic_rows else (0, 0, None)
    active_days  = int(row[0] or 0)
    total_volume = float(row[1] or 0)

    query_sell = """
        SELECT token_out_symbol, SUM(amount_usd) AS sell_usd FROM signal_hub.clean_swaps
        WHERE lower(trader_address) = %(addr)s AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
          AND token_out_symbol NOT IN ('USDT','USDC','DAI','USDE') GROUP BY token_out_symbol
    """
    sell_rows = client.execute(query_sell, {'addr': addr})

    query_buy = """
        SELECT token_in_symbol, SUM(amount_usd) AS buy_usd FROM signal_hub.clean_swaps
        WHERE lower(trader_address) = %(addr)s AND toDateTime(block_timestamp/1000) >= now() - INTERVAL 30 DAY
          AND token_in_symbol NOT IN ('USDT','USDC','DAI','USDE') GROUP BY token_in_symbol
    """
    buy_rows = client.execute(query_buy, {'addr': addr})

    sell_map = {r[0]: float(r[1]) for r in sell_rows}
    buy_map  = {r[0]: float(r[1]) for r in buy_rows}

    wins, losses, total_profit, total_loss = 0, 0, 0.0, 0.0
    all_tokens = set(sell_map.keys()) | set(buy_map.keys())
    for token in all_tokens:
        pnl = sell_map.get(token, 0) - buy_map.get(token, 0)
        if pnl > 0:
            wins += 1; total_profit += pnl
        elif pnl < 0:
            losses += 1; total_loss += abs(pnl)

    total_token_trades = wins + losses
    win_rate     = (wins / total_token_trades * 100) if total_token_trades > 0 else 0.0
    pl_ratio     = (total_profit / total_loss) if total_loss > 0 else total_profit
    sharpe       = min(pl_ratio * win_rate / 100, 3.0)
    max_drawdown = -(total_loss / max(total_volume, 1) * 100)
    growth_30d   = (total_profit - total_loss) / max(total_volume, 1) * 100

    tags            = _calc_style_tags(swap_count, active_days, total_volume, win_rate)
    composite_score = _calc_composite_score(win_rate, pl_ratio, sharpe, max_drawdown, swap_count)
    risk_level, risk_score, risk_flags = _calc_risk(win_rate, max_drawdown, swap_count)

    return {
        "win_rate": win_rate, "profit_loss_ratio": pl_ratio, "sharpe_ratio": sharpe,
        "max_drawdown": max_drawdown, "account_growth_30d": growth_30d, "total_trades_30d": swap_count,
        "total_volume_usd": total_volume, "active_days_30d": active_days,
        "tags": tags, "composite_score": composite_score,
        "risk_level": risk_level, "risk_score": risk_score, "risk_flags": risk_flags
    }

# ================= Persistence to Gold Layer =================

def compute_and_save_address(client, address: str):
    addr = address.lower()
    swap_count = _get_swap_count(client, addr)
    if swap_count == 0:
        logger.debug("No swaps found for %s, skipping.", addr)
        return

    metrics = _calc_from_clean_swaps(client, addr, swap_count)

    insert_query = """
        INSERT INTO signal_hub.gold_address_financials_daily (
            trader_address, calc_date, total_trades_30d, win_rate, 
            profit_loss_ratio, sharpe_ratio, max_drawdown, total_volume_usd, 
            active_days_30d, account_growth_30d, style_tags, risk_level, 
            risk_score, risk_flags, composite_score
        ) VALUES
    """

    row = (
        addr, datetime.utcnow().date(), metrics["total_trades_30d"], metrics["win_rate"],
        metrics["profit_loss_ratio"], metrics["sharpe_ratio"], metrics["max_drawdown"],
        metrics["total_volume_usd"], metrics["active_days_30d"], metrics["account_growth_30d"],
        metrics["tags"], metrics["risk_level"], metrics["risk_score"], metrics["risk_flags"],
        metrics["composite_score"]
    )

    try:
        # clickhouse-driver 插入数据的方式
        client.execute(insert_query, [row])
        logger.info("Successfully computed and saved gold metrics for %s", addr)
    except Exception as e:
        logger.error("Failed to insert metrics for %s. Error: %s", addr, str(e))

def run_daily_batch():
    """
    Main job: Find active addresses from the last 24 hours and calculate metrics.
    """
    client = get_clickhouse_client()
    logger.info("Starting batch processing for daily address financials...")

    # Fetch distinct active addresses (Limit to 50 for initial testing)
    query = """
        SELECT DISTINCT lower(trader_address) 
        FROM signal_hub.clean_swaps 
        WHERE toDateTime(block_timestamp/1000) >= now() - INTERVAL 1 DAY
    """
    rows = client.execute(query)
    addresses = [row[0] for row in rows]

    logger.info("Found %d active addresses to process.", len(addresses))

    for i, addr in enumerate(addresses):
        logger.info("Processing address %d/%d: %s", i + 1, len(addresses), addr)
        compute_and_save_address(client, addr)

    logger.info("Daily batch processing completed successfully.")

if __name__ == "__main__":
    run_daily_batch()