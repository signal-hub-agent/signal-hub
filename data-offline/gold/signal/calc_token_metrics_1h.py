"""
代币成交量与 MEV 污染度指标计算模块
负责提供基础量化指标的实时/离线计算支持
"""
import logging
from dataclasses import dataclass
from config.database import get_clickhouse_client

logger = logging.getLogger(__name__)

# ================= 原样迁移的数据契约 =================
@dataclass
class VolumeTrend:
    volume_24h: float; volume_7d_avg: float; ratio: float; score: int
    data_points_24h: int; is_real_data: bool = True
    def get_trend_label(self) -> str:
        if self.ratio >= 1.5: return "Volume surge"
        elif self.ratio >= 1.0: return "Volume expanding"
        elif self.ratio >= 0.5: return "Volume normal"
        else: return "Volume shrinking"

@dataclass
class MevToxicity:
    total_txs: int; suspicious_txs: int; toxicity_pct: float; score: int
    is_real_data: bool = True; note: str = "Preliminary detection"
    def get_toxicity_label(self) -> str:
        if self.toxicity_pct < 10: return "Low MEV"
        elif self.toxicity_pct < 30: return "Moderate MEV"
        else: return "High MEV"

# ================= 辅助工具：代币别名扩展 =================
def _get_search_tokens(token_symbol: str) -> list:
    """自动将原生币扩展搜索其 Wrapped 包装版本"""
    token = token_symbol.upper()
    tokens = [token]
    # 如果查的是原生币，把 W 前缀的链上代币也算作它的流量
    if token in ['BTC', 'ETH', 'MNT', 'SOL', 'BNB']:
        tokens.append(f"W{token}")
    return tokens

# ================= Volume 计算 =================
def calc_volume_trend(token_symbol: str, force_recalc: bool = False) -> VolumeTrend:
    client = get_clickhouse_client()
    token = token_symbol.upper()

    if not force_recalc:
        try:
            rows = client.execute("""
                SELECT volume_24h_usd, volume_7d_avg_usd, volume_ratio, tx_count_24h
                FROM signal_hub.gold_token_metrics_1h
                WHERE upper(token_symbol) = %(token)s ORDER BY calc_time DESC LIMIT 1
            """, {'token': token})
            if rows and rows[0][0] and float(rows[0][0]) > 0:
                return VolumeTrend(float(rows[0][0]), float(rows[0][1]), float(rows[0][2]), _volume_score(float(rows[0][2])), int(rows[0][3]))
        except Exception:
            pass

    try:
        return _volume_from_clean_swaps(client, token)
    except Exception as e:
        # 🌟 修复：改为 error 级别，一旦崩溃立刻能看到详细日志
        logger.error(f"❌ Silver layer volume calculation failed for {token}: {e}")

    return _volume_from_dune_swaps(client, token)

def _volume_score(ratio: float) -> int:
    if ratio >= 1.5: return 15
    elif ratio >= 1.0: return 10
    elif ratio >= 0.5: return 5
    return 0

def _volume_from_clean_swaps(client, token: str) -> VolumeTrend:
    search_tokens = _get_search_tokens(token)

    # 🌟 修复：使用 ClickHouse 内部的 WITH 直接查最新时间，彻底避免 Python DateTime 注入报错
    r24 = client.execute("""
        WITH (SELECT max(toDateTime(block_timestamp/1000)) FROM signal_hub.clean_swaps) AS latest_time
        SELECT SUM(amount_usd), COUNT(DISTINCT tx_hash)
        FROM signal_hub.clean_swaps
        WHERE toDateTime(block_timestamp/1000) >= latest_time - INTERVAL 24 HOUR
          AND (upper(token_in_symbol) IN %(tokens)s OR upper(token_out_symbol) IN %(tokens)s)
    """, {'tokens': search_tokens})[0]

    r7d = client.execute("""
        WITH (SELECT max(toDateTime(block_timestamp/1000)) FROM signal_hub.clean_swaps) AS latest_time
        SELECT SUM(amount_usd) / 7
        FROM signal_hub.clean_swaps
        WHERE toDateTime(block_timestamp/1000) >= latest_time - INTERVAL 8 DAY
          AND toDateTime(block_timestamp/1000) <  latest_time - INTERVAL 1 DAY
          AND (upper(token_in_symbol) IN %(tokens)s OR upper(token_out_symbol) IN %(tokens)s)
    """, {'tokens': search_tokens})[0]

    vol_24h = float(r24[0] or 0)
    tx_cnt = int(r24[1] or 0)
    vol_7d = float(r7d[0] or 0)
    ratio = vol_24h / vol_7d if vol_7d > 0 else 0.0
    return VolumeTrend(vol_24h, vol_7d, ratio, _volume_score(ratio), tx_cnt)

def _volume_from_dune_swaps(client, token: str) -> VolumeTrend:
    try:
        search_tokens = _get_search_tokens(token)
        r24 = client.execute("""
            WITH (SELECT max(block_time) FROM web3_data.dune_swaps) AS latest_time
            SELECT SUM(amount_usd), COUNT(DISTINCT tx_hash) FROM web3_data.dune_swaps
            WHERE block_time >= latest_time - INTERVAL 24 HOUR
              AND (upper(token_bought_symbol) IN %(tokens)s OR upper(token_sold_symbol) IN %(tokens)s)
        """, {'tokens': search_tokens})[0]

        r7d = client.execute("""
            WITH (SELECT max(block_time) FROM web3_data.dune_swaps) AS latest_time
            SELECT SUM(amount_usd) / 7 FROM web3_data.dune_swaps
            WHERE block_time >= latest_time - INTERVAL 8 DAY AND block_time < latest_time - INTERVAL 1 DAY
              AND (upper(token_bought_symbol) IN %(tokens)s OR upper(token_sold_symbol) IN %(tokens)s)
        """, {'tokens': search_tokens})[0]

        vol_24h = float(r24[0] or 0); tx_cnt = int(r24[1] or 0); vol_7d = float(r7d[0] or 0)
        ratio = vol_24h / vol_7d if vol_7d > 0 else 0.0
        return VolumeTrend(vol_24h, vol_7d, ratio, _volume_score(ratio), tx_cnt)
    except Exception:
        return VolumeTrend(0, 0, 0, 5, 0, False)

# ================= MEV 计算 =================
def calc_mev_toxicity(token_symbol: str, force_recalc: bool = False) -> MevToxicity:
    client = get_clickhouse_client()
    token = token_symbol.upper()

    if not force_recalc:
        try:
            rows = client.execute("""
                SELECT mev_total_txs, mev_suspicious_txs, mev_toxicity_pct
                FROM signal_hub.gold_token_metrics_1h
                WHERE upper(token_symbol) = %(token)s ORDER BY calc_time DESC LIMIT 1
            """, {'token': token})
            if rows and rows[0][0] and int(rows[0][0]) > 0:
                return MevToxicity(int(rows[0][0]), int(rows[0][1]), float(rows[0][2]), _mev_score(float(rows[0][2])))
        except Exception:
            pass

    try:
        return _mev_from_clean_swaps(client, token)
    except Exception as e:
        logger.error(f"❌ Silver layer MEV calculation failed for {token}: {e}")

    return _mev_from_dune_swaps(client, token)

def _mev_score(pct: float) -> int:
    if pct < 10: return 25
    elif pct < 30: return 15
    return 0

def _mev_from_clean_swaps(client, token: str) -> MevToxicity:
    search_tokens = _get_search_tokens(token)

    total = int(client.execute("""
        WITH (SELECT max(toDateTime(block_timestamp/1000)) FROM signal_hub.clean_swaps) AS latest_time
        SELECT COUNT(DISTINCT tx_hash) FROM signal_hub.clean_swaps
        WHERE toDateTime(block_timestamp/1000) >= latest_time - INTERVAL 24 HOUR
          AND (upper(token_in_symbol) IN %(tokens)s OR upper(token_out_symbol) IN %(tokens)s)
    """, {'tokens': search_tokens})[0][0] or 0)

    if total == 0: return MevToxicity(0, 0, 0.0, 25)

    # 去除了 pool_address 的聚合，防止数据库没有这个字段导致崩溃
    suspicious = int(client.execute("""
        WITH (SELECT max(toDateTime(block_timestamp/1000)) FROM signal_hub.clean_swaps) AS latest_time
        SELECT COUNT(DISTINCT tx_hash) FROM (
            SELECT tx_hash,
                   countIf(upper(token_out_symbol) IN %(tokens)s) AS buy_count,
                   countIf(upper(token_in_symbol) IN %(tokens)s) AS sell_count
            FROM signal_hub.clean_swaps
            WHERE toDateTime(block_timestamp/1000) >= latest_time - INTERVAL 24 HOUR
              AND (upper(token_in_symbol) IN %(tokens)s OR upper(token_out_symbol) IN %(tokens)s)
            GROUP BY tx_hash, trader_address HAVING buy_count > 0 AND sell_count > 0
        ) t
    """, {'tokens': search_tokens})[0][0] or 0)

    pct = (suspicious / total) * 100
    return MevToxicity(total, suspicious, pct, _mev_score(pct))

def _mev_from_dune_swaps(client, token: str) -> MevToxicity:
    try:
        search_tokens = _get_search_tokens(token)
        total = int(client.execute("""
            WITH (SELECT max(block_time) FROM web3_data.dune_swaps) AS latest_time
            SELECT COUNT(DISTINCT tx_hash) FROM web3_data.dune_swaps
            WHERE block_time >= latest_time - INTERVAL 24 HOUR
              AND (upper(token_bought_symbol) IN %(tokens)s OR upper(token_sold_symbol) IN %(tokens)s)
        """, {'tokens': search_tokens})[0][0] or 0)

        if total == 0: return MevToxicity(0, 0, 0.0, 25)

        suspicious = int(client.execute("""
            WITH (SELECT max(block_time) FROM web3_data.dune_swaps) AS latest_time
            SELECT COUNT(DISTINCT tx_hash) FROM (
                SELECT tx_hash,
                    countIf(upper(token_bought_symbol) IN %(tokens)s) AS buy_count,
                    countIf(upper(token_sold_symbol) IN %(tokens)s) AS sell_count
                FROM web3_data.dune_swaps
                WHERE block_time >= latest_time - INTERVAL 24 HOUR
                  AND (upper(token_bought_symbol) IN %(tokens)s OR upper(token_sold_symbol) IN %(tokens)s)
                GROUP BY tx_hash, trader_address HAVING buy_count > 0 AND sell_count > 0
            ) t
        """, {'tokens': search_tokens})[0][0] or 0)

        pct = (suspicious / total) * 100
        return MevToxicity(total, suspicious, pct, _mev_score(pct))
    except Exception:
        return MevToxicity(0, 0, 0.0, 15, False)