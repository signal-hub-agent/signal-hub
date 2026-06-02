"""
信号参谋 - ClickHouse 数据查询层
负责成交量趋势 和 MEV污染度 的真实数据计算
"""
import clickhouse_connect
from models import VolumeTrend, MevToxicity


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='123456',
        database='web3_data',
    )


def get_latest_time(client):
    """拿数据库里最新的 block_time，作为'当前时间'基准（避免数据断档问题）"""
    row = client.query("SELECT max(block_time) FROM web3_data.dune_swaps").first_row
    return row[0]  # datetime 对象


# ─────────────────────────────────────────────
# 维度1：成交量趋势
# ─────────────────────────────────────────────

def calc_volume_trend(token_symbol: str) -> VolumeTrend:
    client = get_clickhouse_client()
    token = token_symbol.upper()
    latest = get_latest_time(client)

    # 近24h成交量 + 交易笔数
    query_24h = """
        SELECT SUM(amount_usd), COUNT()
        FROM web3_data.dune_swaps
        WHERE
            block_time >= {latest:DateTime} - INTERVAL 24 HOUR
            AND (
                upper(token_bought_symbol) = {token:String}
                OR upper(token_sold_symbol) = {token:String}
            )
    """
    row = client.query(query_24h, parameters={"token": token, "latest": latest}).first_row
    volume_24h = float(row[0] or 0)
    tx_count   = int(row[1] or 0)

    # 近7日日均（不含最近24h）
    query_7d = """
        SELECT SUM(amount_usd) / 7
        FROM web3_data.dune_swaps
        WHERE
            block_time >= {latest:DateTime} - INTERVAL 8 DAY
            AND block_time <  {latest:DateTime} - INTERVAL 1 DAY
            AND (
                upper(token_bought_symbol) = {token:String}
                OR upper(token_sold_symbol) = {token:String}
            )
    """
    row7 = client.query(query_7d, parameters={"token": token, "latest": latest}).first_row
    volume_7d_avg = float(row7[0] or 0)

    # 打分
    if volume_7d_avg == 0:
        ratio = 0.0
        score = 5
    else:
        ratio = volume_24h / volume_7d_avg
        if ratio >= 1.5:
            score = 15
        elif ratio >= 1.0:
            score = 10
        elif ratio >= 0.5:
            score = 5
        else:
            score = 0

    return VolumeTrend(
        volume_24h=volume_24h,
        volume_7d_avg=volume_7d_avg,
        ratio=ratio,
        score=score,
        data_points_24h=tx_count,
    )


# ─────────────────────────────────────────────
# 维度2：MEV污染度（初版）
# ─────────────────────────────────────────────

def calc_mev_toxicity(token_symbol: str) -> MevToxicity:
    client = get_clickhouse_client()
    token = token_symbol.upper()
    latest = get_latest_time(client)

    # 近24h总交易笔数
    query_total = """
        SELECT COUNT(DISTINCT tx_hash)
        FROM web3_data.dune_swaps
        WHERE
            block_time >= {latest:DateTime} - INTERVAL 24 HOUR
            AND (
                upper(token_bought_symbol) = {token:String}
                OR upper(token_sold_symbol) = {token:String}
            )
    """
    row_total = client.query(query_total, parameters={"token": token, "latest": latest}).first_row
    total_txs = int(row_total[0] or 0)

    if total_txs == 0:
        return MevToxicity(total_txs=0, suspicious_txs=0, toxicity_pct=0.0, score=25)

    # 疑似MEV：同一tx_hash + 同一trader，既买又卖
    query_mev = """
        SELECT COUNT(DISTINCT tx_hash)
        FROM (
            SELECT
                tx_hash,
                trader_address,
                countIf(upper(token_bought_symbol) = {token:String}) AS buy_count,
                countIf(upper(token_sold_symbol)   = {token:String}) AS sell_count
            FROM web3_data.dune_swaps
            WHERE
                block_time >= {latest:DateTime} - INTERVAL 24 HOUR
                AND (
                    upper(token_bought_symbol) = {token:String}
                    OR upper(token_sold_symbol) = {token:String}
                )
            GROUP BY tx_hash, trader_address
            HAVING buy_count > 0 AND sell_count > 0
        ) t
    """
    row_mev = client.query(query_mev, parameters={"token": token, "latest": latest}).first_row
    suspicious_txs = int(row_mev[0] or 0)

    toxicity_pct = (suspicious_txs / total_txs) * 100

    if toxicity_pct < 10:
        score = 25
    elif toxicity_pct < 30:
        score = 15
    else:
        score = 0

    return MevToxicity(
        total_txs=total_txs,
        suspicious_txs=suspicious_txs,
        toxicity_pct=toxicity_pct,
        score=score,
    )


# ─────────────────────────────────────────────
# 工具：查数据库里实际有哪些代币
# ─────────────────────────────────────────────

def get_available_tokens() -> list:
    client = get_clickhouse_client()
    query = """
        SELECT upper(token_bought_symbol) AS token, COUNT() AS cnt
        FROM web3_data.dune_swaps
        GROUP BY token
        ORDER BY cnt DESC
        LIMIT 50
    """
    result = client.query(query)
    return [row[0] for row in result.result_rows if row[0]]