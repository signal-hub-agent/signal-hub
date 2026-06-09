"""
Data Layer - ClickHouse Query Module
Aligned with teammate's table structure.

Query priority per metric:
  1. signal_hub.gold_token_metrics_1h   (pre-computed, fastest)
  2. signal_hub.clean_swaps             (real-time, teammate's silver layer)
  3. web3_data.dune_swaps               (temporary fallback, Dune historical data)
"""
import clickhouse_connect
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────

def get_ch_client():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='123456',
    )


# ─────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────

@dataclass
class VolumeTrend:
    volume_24h: float
    volume_7d_avg: float
    ratio: float
    score: int
    data_points_24h: int
    is_real_data: bool = True

    def get_trend_label(self) -> str:
        if self.ratio >= 1.5:
            return "Volume surge"
        elif self.ratio >= 1.0:
            return "Volume expanding"
        elif self.ratio >= 0.5:
            return "Volume normal"
        else:
            return "Volume shrinking"


@dataclass
class MevToxicity:
    total_txs: int
    suspicious_txs: int
    toxicity_pct: float
    score: int
    is_real_data: bool = True
    note: str = "Preliminary detection: same-block buy/sell pairing"

    def get_toxicity_label(self) -> str:
        if self.toxicity_pct < 10:
            return "Low MEV"
        elif self.toxicity_pct < 30:
            return "Moderate MEV"
        else:
            return "High MEV"


# ─────────────────────────────────────────────
# Volume Trend
# ─────────────────────────────────────────────

def calc_volume_trend(token_symbol: str) -> VolumeTrend:
    """
    Calculates 24h volume trend score.
    Tries Gold layer first, then Silver (clean_swaps), then Dune fallback.
    """
    client = get_ch_client()
    token = token_symbol.upper()

    # Level 1: Gold layer pre-computed result
    try:
        row = client.query("""
            SELECT volume_24h_usd, volume_7d_avg_usd, volume_ratio, tx_count_24h
            FROM signal_hub.gold_token_metrics_1h
            WHERE upper(token_symbol) = {token:String}
            ORDER BY calc_time DESC
            LIMIT 1
        """, parameters={"token": token}).first_row

        if row and row[0] and float(row[0]) > 0:
            ratio = float(row[2])
            return VolumeTrend(
                volume_24h=float(row[0]),
                volume_7d_avg=float(row[1]),
                ratio=ratio,
                score=_volume_score(ratio),
                data_points_24h=int(row[3]),
            )
    except Exception as e:
        logger.debug("Gold layer miss for volume [%s]: %s", token, e)

    # Level 2: Silver layer clean_swaps
    try:
        latest = _get_latest_clean_swaps_time(client)
        if latest:
            return _volume_from_clean_swaps(client, token, latest)
    except Exception as e:
        logger.debug("Silver layer miss for volume [%s]: %s", token, e)

    # Level 3: Dune historical fallback
    return _volume_from_dune_swaps(client, token)


def _volume_score(ratio: float) -> int:
    if ratio >= 1.5:
        return 15
    elif ratio >= 1.0:
        return 10
    elif ratio >= 0.5:
        return 5
    return 0


def _get_latest_clean_swaps_time(client):
    row = client.query(
        "SELECT max(toDateTime(block_timestamp/1000)) FROM signal_hub.clean_swaps"
    ).first_row
    return row[0] if row else None


def _volume_from_clean_swaps(client, token: str, latest) -> VolumeTrend:
    """Calculates volume from signal_hub.clean_swaps (silver layer)."""
    r24 = client.query("""
        SELECT SUM(amount_usd), COUNT(DISTINCT tx_hash)
        FROM signal_hub.clean_swaps
        WHERE toDateTime(block_timestamp/1000) >= {latest:DateTime} - INTERVAL 24 HOUR
          AND (upper(token_in_symbol) = {token:String}
               OR upper(token_out_symbol) = {token:String})
    """, parameters={"token": token, "latest": latest}).first_row

    r7d = client.query("""
        SELECT SUM(amount_usd) / 7
        FROM signal_hub.clean_swaps
        WHERE toDateTime(block_timestamp/1000) >= {latest:DateTime} - INTERVAL 8 DAY
          AND toDateTime(block_timestamp/1000) <  {latest:DateTime} - INTERVAL 1 DAY
          AND (upper(token_in_symbol) = {token:String}
               OR upper(token_out_symbol) = {token:String})
    """, parameters={"token": token, "latest": latest}).first_row

    volume_24h    = float(r24[0] or 0)
    tx_count      = int(r24[1] or 0)
    volume_7d_avg = float(r7d[0] or 0)
    ratio         = volume_24h / volume_7d_avg if volume_7d_avg > 0 else 0.0

    return VolumeTrend(
        volume_24h=volume_24h,
        volume_7d_avg=volume_7d_avg,
        ratio=ratio,
        score=_volume_score(ratio),
        data_points_24h=tx_count,
    )


def _volume_from_dune_swaps(client, token: str) -> VolumeTrend:
    """Temporary fallback using web3_data.dune_swaps (Dune historical data)."""
    try:
        latest = client.query(
            "SELECT max(block_time) FROM web3_data.dune_swaps"
        ).first_row[0]

        r24 = client.query("""
            SELECT SUM(amount_usd), COUNT(DISTINCT tx_hash)
            FROM web3_data.dune_swaps
            WHERE block_time >= {latest:DateTime} - INTERVAL 24 HOUR
              AND (upper(token_bought_symbol) = {token:String}
                   OR upper(token_sold_symbol) = {token:String})
        """, parameters={"token": token, "latest": latest}).first_row

        r7d = client.query("""
            SELECT SUM(amount_usd) / 7
            FROM web3_data.dune_swaps
            WHERE block_time >= {latest:DateTime} - INTERVAL 8 DAY
              AND block_time < {latest:DateTime} - INTERVAL 1 DAY
              AND (upper(token_bought_symbol) = {token:String}
                   OR upper(token_sold_symbol) = {token:String})
        """, parameters={"token": token, "latest": latest}).first_row

        volume_24h    = float(r24[0] or 0)
        tx_count      = int(r24[1] or 0)
        volume_7d_avg = float(r7d[0] or 0)
        ratio         = volume_24h / volume_7d_avg if volume_7d_avg > 0 else 0.0

        return VolumeTrend(
            volume_24h=volume_24h,
            volume_7d_avg=volume_7d_avg,
            ratio=ratio,
            score=_volume_score(ratio),
            data_points_24h=tx_count,
        )
    except Exception:
        return VolumeTrend(
            volume_24h=0, volume_7d_avg=0, ratio=0,
            score=5, data_points_24h=0, is_real_data=False,
        )


# ─────────────────────────────────────────────
# MEV Toxicity
# ─────────────────────────────────────────────

def calc_mev_toxicity(token_symbol: str) -> MevToxicity:
    """
    Calculates MEV sandwich attack ratio for a token.
    Tries Gold layer first, then Silver (clean_swaps), then Dune fallback.
    """
    client = get_ch_client()
    token = token_symbol.upper()

    # Level 1: Gold layer
    try:
        row = client.query("""
            SELECT mev_total_txs, mev_suspicious_txs, mev_toxicity_pct
            FROM signal_hub.gold_token_metrics_1h
            WHERE upper(token_symbol) = {token:String}
            ORDER BY calc_time DESC
            LIMIT 1
        """, parameters={"token": token}).first_row

        if row and row[0] and int(row[0]) > 0:
            pct = float(row[2])
            return MevToxicity(
                total_txs=int(row[0]),
                suspicious_txs=int(row[1]),
                toxicity_pct=pct,
                score=_mev_score(pct),
                note="Source: gold_token_metrics_1h (pre-computed)",
            )
    except Exception as e:
        logger.debug("Gold layer miss for MEV [%s]: %s", token, e)

    # Level 2: Silver layer clean_swaps
    try:
        return _mev_from_clean_swaps(client, token)
    except Exception as e:
        logger.debug("Silver layer miss for MEV [%s]: %s", token, e)

    # Level 3: Dune fallback
    return _mev_from_dune_swaps(client, token)


def _mev_score(pct: float) -> int:
    if pct < 10:
        return 25
    elif pct < 30:
        return 15
    return 0


def _mev_from_clean_swaps(client, token: str) -> MevToxicity:
    """
    Detects MEV using signal_hub.clean_swaps.
    Higher accuracy: uses pool_address to detect same-pool sandwich attacks.
    """
    latest = _get_latest_clean_swaps_time(client)

    total = int(client.query("""
        SELECT COUNT(DISTINCT tx_hash)
        FROM signal_hub.clean_swaps
        WHERE toDateTime(block_timestamp/1000) >= {latest:DateTime} - INTERVAL 24 HOUR
          AND (upper(token_in_symbol) = {token:String}
               OR upper(token_out_symbol) = {token:String})
    """, parameters={"token": token, "latest": latest}).first_row[0] or 0)

    if total == 0:
        return MevToxicity(total_txs=0, suspicious_txs=0, toxicity_pct=0.0, score=25)

    # Same tx + same trader + same pool: bought AND sold the token -> suspicious MEV
    suspicious = int(client.query("""
        SELECT COUNT(DISTINCT tx_hash)
        FROM (
            SELECT tx_hash, trader_address, pool_address,
                countIf(upper(token_out_symbol) = {token:String}) AS buy_count,
                countIf(upper(token_in_symbol)  = {token:String}) AS sell_count
            FROM signal_hub.clean_swaps
            WHERE toDateTime(block_timestamp/1000) >= {latest:DateTime} - INTERVAL 24 HOUR
              AND (upper(token_in_symbol) = {token:String}
                   OR upper(token_out_symbol) = {token:String})
            GROUP BY tx_hash, trader_address, pool_address
            HAVING buy_count > 0 AND sell_count > 0
        ) t
    """, parameters={"token": token, "latest": latest}).first_row[0] or 0)

    pct = (suspicious / total) * 100
    return MevToxicity(
        total_txs=total,
        suspicious_txs=suspicious,
        toxicity_pct=pct,
        score=_mev_score(pct),
        note="Source: clean_swaps — same-block same-pool buy/sell pairing",
    )


def _mev_from_dune_swaps(client, token: str) -> MevToxicity:
    """Temporary MEV fallback using web3_data.dune_swaps."""
    try:
        latest = client.query(
            "SELECT max(block_time) FROM web3_data.dune_swaps"
        ).first_row[0]

        total = int(client.query("""
            SELECT COUNT(DISTINCT tx_hash)
            FROM web3_data.dune_swaps
            WHERE block_time >= {latest:DateTime} - INTERVAL 24 HOUR
              AND (upper(token_bought_symbol) = {token:String}
                   OR upper(token_sold_symbol) = {token:String})
        """, parameters={"token": token, "latest": latest}).first_row[0] or 0)

        if total == 0:
            return MevToxicity(total_txs=0, suspicious_txs=0, toxicity_pct=0.0, score=25)

        suspicious = int(client.query("""
            SELECT COUNT(DISTINCT tx_hash)
            FROM (
                SELECT tx_hash, trader_address,
                    countIf(upper(token_bought_symbol) = {token:String}) AS buy_count,
                    countIf(upper(token_sold_symbol)   = {token:String}) AS sell_count
                FROM web3_data.dune_swaps
                WHERE block_time >= {latest:DateTime} - INTERVAL 24 HOUR
                  AND (upper(token_bought_symbol) = {token:String}
                       OR upper(token_sold_symbol) = {token:String})
                GROUP BY tx_hash, trader_address
                HAVING buy_count > 0 AND sell_count > 0
            ) t
        """, parameters={"token": token, "latest": latest}).first_row[0] or 0)

        pct = (suspicious / total) * 100
        return MevToxicity(
            total_txs=total,
            suspicious_txs=suspicious,
            toxicity_pct=pct,
            score=_mev_score(pct),
        )
    except Exception:
        return MevToxicity(
            total_txs=0, suspicious_txs=0,
            toxicity_pct=0.0, score=15, is_real_data=False,
        )


# ─────────────────────────────────────────────
# Token List
# ─────────────────────────────────────────────

def get_available_tokens() -> list:
    """Returns a list of tokens that have data in the database."""
    client = get_ch_client()

    # Try Gold layer first
    try:
        rows = client.query("""
            SELECT DISTINCT token_symbol
            FROM signal_hub.gold_token_metrics_1h
            ORDER BY token_symbol LIMIT 50
        """).result_rows
        tokens = [r[0] for r in rows if r[0]]
        if tokens:
            return tokens
    except Exception:
        pass

    # Fallback to dune_swaps
    try:
        rows = client.query("""
            SELECT upper(token_bought_symbol) AS token, COUNT() AS cnt
            FROM web3_data.dune_swaps
            GROUP BY token ORDER BY cnt DESC LIMIT 50
        """).result_rows
        return [r[0] for r in rows if r[0]]
    except Exception:
        return []