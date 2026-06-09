"""
SignalHub - Signal Advisor API
Unified entry point for all signal and detective endpoints.
"""
import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from data_layer import calc_volume_trend, calc_mev_toxicity, get_available_tokens
from technical_analysis import run_technical_analysis
from llm_service import generate_report
from address_llm_service import generate_address_profile, calc_copy_signal_score
from detective_service import analyze_address_metrics, get_top_traders
from models import ContractSafety

app = FastAPI(
    title="SignalHub API",
    version="1.0.0",
    description="Copy-trade signal platform for the Mantle ecosystem.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# Request Schemas
# ─────────────────────────────────────────────

class AddressProfileRequest(BaseModel):
    address: str
    user_frequency: Optional[str] = "mid_freq"  # high_freq / mid_freq / low_freq


class CopySignalRequest(BaseModel):
    address_score: int
    token_sfs: int
    amount_usd: float
    avg_trade_amount: float


# ─────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "signalhub-api", "version": "1.0.0"}


# ─────────────────────────────────────────────
# Token Endpoints
# ─────────────────────────────────────────────

@app.get("/tokens")
def list_tokens():
    """Returns all tokens that have data in the database."""
    try:
        tokens = get_available_tokens()
        return {"tokens": tokens, "count": len(tokens)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signal/{token_symbol}")
def get_token_signal(
    token_symbol: str,
    skip_llm: bool = Query(False, description="Skip LLM report (debug mode)"),
):
    """
    Full token signal analysis.
    Returns technical indicators + on-chain metrics + LLM report + composite score.
    """
    symbol = token_symbol.upper().strip()

    try:
        # Technical analysis (Bybit klines)
        tech = run_technical_analysis(symbol)

        # Volume trend (ClickHouse)
        try:
            vt = calc_volume_trend(symbol)
            volume_data = {
                "volume_24h_usd":   round(vt.volume_24h, 2),
                "volume_7d_avg_usd": round(vt.volume_7d_avg, 2),
                "ratio":            round(vt.ratio, 2),
                "trend_label":      vt.get_trend_label(),
                "tx_count_24h":     vt.data_points_24h,
                "score":            vt.score,
                "max_score":        15,
                "is_real_data":     True,
            }
        except Exception as e:
            volume_data = {"score": 5, "max_score": 15, "is_real_data": False, "error": str(e)}

        # MEV toxicity (ClickHouse)
        try:
            mev = calc_mev_toxicity(symbol)
            mev_data = {
                "total_txs":      mev.total_txs,
                "suspicious_txs": mev.suspicious_txs,
                "toxicity_pct":   round(mev.toxicity_pct, 2),
                "toxicity_label": mev.get_toxicity_label(),
                "score":          mev.score,
                "max_score":      25,
                "is_real_data":   True,
                "note":           mev.note,
            }
        except Exception as e:
            mev_data = {"score": 15, "max_score": 25, "is_real_data": False, "error": str(e)}

        # Contract safety (mock — RPC integration pending)
        contract = ContractSafety()
        contract_data = {
            "score":      contract.score,
            "max_score":  30,
            "is_real_data": False,
            "status":     "Coming soon",
            "risk_items": contract.risk_items,
        }

        # Composite score
        total_score = (
            tech.tech_score +
            volume_data.get("score", 5) +
            mev_data.get("score", 15) +
            contract.score
        )

        if total_score >= 80:
            signal_color = "green"
        elif total_score >= 60:
            signal_color = "yellow"
        else:
            signal_color = "red"

        # LLM report
        llm_report = ""
        if not skip_llm:
            try:
                llm_report = generate_report(tech)
            except Exception as e:
                llm_report = f"Report generation failed: {str(e)[:100]}"

        return {
            "token":        symbol,
            "total_score":  total_score,
            "max_score":    100,
            "signal_color": signal_color,
            "llm_report":   llm_report,
            "score_breakdown": {
                "technical_analysis": {
                    "score":        tech.tech_score,
                    "max_score":    30,
                    "is_real_data": tech.is_real_data,
                },
                "volume_trend":    volume_data,
                "mev_toxicity":    mev_data,
                "contract_safety": contract_data,
            },
            "technical_detail": tech.to_dict(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to analyze {symbol}: {str(e)}")


@app.get("/technical/{token_symbol}")
def get_technical_only(token_symbol: str):
    """Fast endpoint — technical indicators only, no LLM."""
    symbol = token_symbol.upper().strip()
    try:
        result = run_technical_analysis(symbol)
        return result.to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# Address Detective Endpoints
# ─────────────────────────────────────────────

@app.get("/address/{address}/metrics")
def get_address_metrics(address: str):
    """
    Returns raw financial metrics for an address.
    Fast — no LLM, reads from gold layer or computes from clean_swaps.
    """
    try:
        result = analyze_address_metrics(address.lower())
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/address/profile")
def get_address_profile(request: AddressProfileRequest):
    """
    Full address portrait with LLM analysis.
    Triggered when a user searches an address — takes 2-3 seconds.
    Report should be cached by the frontend or Redis for 24h.

    Includes:
    - Financial metrics (win rate, P/L ratio, Sharpe, drawdown)
    - Recent trades enriched with token SFS scores
    - Entry timing quality evaluation
    - LLM-generated profile report and copy-trade recommendation
    - Style match against user's own trading frequency
    """
    try:
        # Step 1: Get financial metrics
        metrics = analyze_address_metrics(request.address)

        if not metrics.get("is_dex_trader"):
            return {
                "address":      request.address,
                "is_dex_trader": False,
                "message":      "No DEX trading history found for this address.",
                "llm_analysis": {
                    "profile_report":    "This address has no DEX trading history on Mantle in the last 30 days.",
                    "pre_rating":        "red",
                    "style_label":       "Non-DEX User",
                    "style_match":       False,
                    "copy_suggestion":   "Not recommended for copy-trading.",
                    "key_metrics_summary": "No data available",
                },
            }

        # Step 2: Generate LLM profile
        financials = metrics.get("metrics", {})
        profile = generate_address_profile(
            address=request.address,
            financials=financials,
            user_frequency=request.user_frequency,
        )

        return {
            "address":        request.address,
            "is_dex_trader":  True,
            "metrics":        financials,
            "tags":           metrics.get("tags", []),
            "risk":           metrics.get("risk", {}),
            "llm_analysis":   profile["llm_analysis"],
            "trade_quality":  profile["trade_quality"],
            "recent_trades":  profile["recent_trades"],
            "generated_at":   profile["generated_at"],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to profile {request.address}: {str(e)}")


@app.post("/address/copy-signal")
def get_copy_signal(request: CopySignalRequest):
    """
    Real-time copy-trade signal scoring — NO LLM, pure calculation.
    Called when a subscribed address makes a trade.
    Should respond in < 1 second.

    Formula:
        copy_score = address_score * 0.4 + token_sfs * 0.4 + amount_signal * 0.2
    """
    try:
        result = calc_copy_signal_score(
            address_score=request.address_score,
            token_sfs=request.token_sfs,
            amount_usd=request.amount_usd,
            avg_trade_amount=request.avg_trade_amount,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/address/top-traders")
def get_top_traders(limit: int = Query(20, le=100)):
    """
    Returns the top traders ranked by composite score.
    Reads from gold_address_financials_daily.
    """
    try:
        traders = get_top_traders(limit=limit)
        return {"traders": traders, "count": len(traders)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)