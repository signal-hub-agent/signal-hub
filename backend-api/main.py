"""
信号参谋 - FastAPI 路由
对外暴露的 API 接口
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from models import TokenSignal, ContractSafety, TechnicalBias
from data_layer import calc_volume_trend, calc_mev_toxicity, get_available_tokens
from kline_service import calc_technical_bias
from llm_service import generate_llm_summary

app = FastAPI(title="信号参谋 API", version="0.1.0")

# 允许前端跨域访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "service": "signal-advisor"}


@app.get("/tokens")
def list_tokens():
    """返回数据库中有数据的代币列表，供前端下拉选择"""
    try:
        tokens = get_available_tokens()
        return {"tokens": tokens, "count": len(tokens)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signal/{token_symbol}")
def get_signal(token_symbol: str, skip_llm: bool = False):
    """
    核心接口：获取代币的跟单指数
    
    - token_symbol: 代币符号，如 ETH / USDT / SOL
    - skip_llm: 为 True 时跳过LLM调用（调试用）
    
    返回完整的 TokenSignal 分析结果
    """
    symbol = token_symbol.upper().strip()

    try:
        # 1. 成交量趋势（真实数据）
        volume_trend = calc_volume_trend(symbol)

        # 2. MEV污染度（真实数据，初版）
        mev_toxicity = calc_mev_toxicity(symbol)

        # 3. 合约安全（Mock）
        contract_safety = ContractSafety()

        # 4. 技术面（真实K线数据）
        technical_bias = calc_technical_bias(symbol)

        # 5. 汇总
        signal = TokenSignal(
            token_symbol=symbol,
            volume_trend=volume_trend,
            mev_toxicity=mev_toxicity,
            contract_safety=contract_safety,
            technical_bias=technical_bias,
        ).compute_total()

        # 6. LLM一句话总结
        if not skip_llm:
            try:
                signal.llm_summary = generate_llm_summary(signal)
            except Exception as llm_err:
                # LLM挂了不影响主流程
                signal.llm_summary = f"（AI总结生成失败：{str(llm_err)[:50]}）"

        return signal.to_dict()

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"分析 {symbol} 时出错：{str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)