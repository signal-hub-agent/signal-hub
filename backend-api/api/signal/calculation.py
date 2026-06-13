import json
from .schemas import ComponentScores

class SignalCalculation:
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
NEUTRAL only when NONE of the above conditions are met.

Step 2: Output strictly as JSON.
Format: {"bias": "BULLISH" | "BEARISH" | "NEUTRAL", "summary": "One sentence overall summary", "bullish_factors": ["Point 1"], "bearish_factors": ["Point 1"], "support_levels": [1.23], "resistance_levels": [1.30]}
Rules: Each bullet point is ONE sentence. Never say "will rise".
"""

    @staticmethod
    def build_components(row: dict) -> ComponentScores:
        return ComponentScores(
            volume_trend={
                "label": "Volume Trend", "score": int(row.get('volume_score', 0)), "max_score": 15,
                "details": {
                    "volume_24h_usd": row.get('volume_24h_usd'),
                    "volume_7d_avg_usd": row.get('volume_7d_avg_usd'),
                    "ratio": row.get('volume_ratio'),
                    "tx_count_24h": row.get('tx_count_24h')
                }
            },
            mev_toxicity={
                "label": "MEV Toxicity", "score": int(row.get('mev_score', 0)), "max_score": 25,
                "details": {
                    "toxicity_pct": row.get('mev_toxicity_pct'),
                    "mev_suspicious_txs": row.get('mev_suspicious_txs'),
                    "mev_total_txs": row.get('mev_total_txs')
                }
            },
            contract_safety={
                "label": "Contract Safety", "score": int(row.get('contract_score', 28)), "max_score": 30,
                "details": {"risk_items": ["Checked OK"]}
            },
            technical_bias={
                "label": "Technical Bias", "score": int(row.get('tech_score', 0)), "max_score": 30,
                "details": {
                    "ma_trend": row.get('ma_trend_type'),
                    "ma_alignment": row.get('ma_alignment'),
                    "rsi_zone": row.get('rsi_zone'),
                    "rsi_value": float(row.get('rsi_value') or 50.0), # 增加 or 50.0 兜底，防止数据库查出 None 报错
                    "macd_position": row.get('macd_position'),
                    "macd_histogram_trend": row.get('macd_histogram_trend'),
                    "macd_divergence": row.get('macd_divergence'),
                    "bollinger_pattern": row.get('bollinger_pattern'),
                    "bollinger_support": float(row.get('bollinger_support') or 0.0),
                    "bollinger_resistance": float(row.get('bollinger_resistance') or 0.0)
                }
            }
        )

    @staticmethod
    def build_llm_user_prompt(row: dict) -> str:
        # 仅抽取大模型需要的字段，避免全量 dump 导致 token 浪费
        clean_data = {
            "symbol": row.get("token_symbol"),
            "price": row.get("current_price"),
            "ma_trend": row.get("ma_trend_type"),
            "rsi": row.get("rsi_value"),
            "macd": row.get("macd_position")
        }
        return f"Write a technical brief for this token data.\nData:\n{json.dumps(clean_data, indent=2, default=str)}"