import os
import sys
import subprocess
from prefect import flow, task

# 获取当前运行的 Python 解释器路径，防止虚拟环境冲突
PYTHON_CMD = sys.executable

# ==========================================
# 核心 Task：通用脚本执行器
# ==========================================
@task(retries=2, retry_delay_seconds=30)
def run_script(script_path: str):
    """
    通用执行器：调用 Python 脚本。如果失败会自动重试 2 次。
    """
    print(f"🚀 [Task] 准备执行: {script_path}")

    # 路径校验，确保及时报错
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"❌ 找不到脚本文件: {script_path}")

    # 执行脚本
    subprocess.run([PYTHON_CMD, script_path], check=True)
    print(f"✅ [Task] 执行成功: {script_path}")


# ==========================================
# Flow 1：15分钟级流水线 (15m)
# ==========================================
@flow(name="Signal Hub - 15m Pipeline", log_prints=True)
def pipeline_15m():
    print("--- 启动 15m 级高频数据流水线 ---")

    # 严格按照从上游到下游的 DAG 依赖顺序执行
    # 1. 抓取原始 K 线 (假设你的 fetch 脚本在 bronze 目录)
    run_script("bronze/signal/fetch_bybit_klines_15m.py")

    # 2. 清洗 K 线 (你红框圈出的重点)
    run_script("silver/signal/clean_klines_15m.py")

    # 3. 计算技术指标
    run_script("gold/signal/calc_tech_indicators_15m.py")


# ==========================================
# Flow 2：1小时级流水线 (1h)
# ==========================================
@flow(name="Signal Hub - 1h Pipeline", log_prints=True)
def pipeline_1h():
    print("--- 启动 1h 级代币分析流水线 ---")

    # 目前仅有计算代币指标，后续可在此增加 1h 级别的其他脚本
    run_script("gold/signal/calc_token_metrics_1h.py")


# ==========================================
# Flow 3：每日级流水线 (daily)
# ==========================================
@flow(name="Signal Hub - Daily Pipeline", log_prints=True)
def pipeline_daily():
    print("--- 启动 Daily 级核心画像流水线 ---")

    # 严格的日级执行顺序
    # 1. 抓取并生成最新的池子字典
    run_script("bronze/core/fetch_pool_registry_daily.py")

    # 2. 每日离线 Swap 聚合清洗 (你红框圈出的重点)
    run_script("silver/core/process_swaps_daily.py")

    # 3. 计算每日聪明钱/巨鲸财务画像
    run_script("gold/detective/calc_address_financials_daily.py")


# ==========================================
# 本地测试入口
# ==========================================
if __name__ == "__main__":
    print("开始本地执行测试...")
    # 你可以按需注释掉不想跑的流程
    pipeline_15m()
    pipeline_1h()
    pipeline_daily()