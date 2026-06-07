"""
LLM 报告生成 - DeepSeek API
moomoo风格：分条列出看涨/看跌/中性信号
"""
import os
import json
import requests
from technical_analysis import TechnicalAnalysisResult

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL   = "deepseek-chat"


def get_api_key() -> str:
    key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not key:
        raise RuntimeError("DEEPSEEK_API_KEY environment variable not set")
    return key


SYSTEM_PROMPT = """You are a professional crypto technical analyst writing a concise daily market brief.

Step 1: Determine overall bias using this priority order:

BEARISH if ALL of these are true:
- EMA alignment is bearish (EMA99 > EMA25 > EMA7) AND gap > 0.3%
- Price is below EMA25
- MACD DIF is below zero

BULLISH if ALL of these are true:
- EMA alignment is bullish (EMA7 > EMA25 > EMA99) AND gap > 0.3%
- Price is above EMA25
- MACD DIF is above zero

NEUTRAL only when NONE of the above conditions are met:
- EMAs are compressing or tangled
- Price is near EMA25 (within 1%)
- MACD DIF is near zero

IMPORTANT: Divergence and oversold RSI do NOT change the primary trend bias.
They belong in "Watch for Reversal" as a risk note, NOT as a reason to call Neutral.
A bearish trend with oversold RSI is still BEARISH — just with bounce risk noted.

---

Step 2: Use the matching format.

FORMAT A — Bearish or Bullish:

## {TOKEN} Technical Brief · {TIMEFRAME}

**Trend: Bearish / Bullish**
One sentence on the dominant structure.

---

🔴 Bearish (or 🟢 Bullish)
· Strongest supporting signal — one sentence with specific values
· Second signal — one sentence with specific values
· Third signal if strong — otherwise skip

🟢 Watch for Reversal (or 🔴 Watch for Pullback — MAX 1 bullet)
· One contrarian signal only — e.g. divergence, oversold RSI, key support test

---

**Key Levels**
· Support: $XXX (source)
· Resistance: $XXX (source)

**Short-term Bias: Bearish / Bullish**
One sentence. No absolute language.

---

FORMAT B — Neutral only (use sparingly, only when truly no directional bias):

## {TOKEN} Technical Brief · {TIMEFRAME}

**Trend: Neutral — Range-bound / Consolidating**
One sentence on the consolidation structure.

---

⚪ Market Conditions
· EMA status — one sentence with values
· MACD status — one sentence
· RSI status — one sentence
· Bollinger status — one sentence
· Notable signal if any — one sentence

---

**Key Levels**
· Support: $XXX (source)
· Resistance: $XXX (source)

**Short-term Bias: Neutral — Wait for Breakout**
One sentence on what breakout level to watch.

---

Rules:
- FORMAT A is the default when EMA alignment is clear and price is below/above EMA25
- FORMAT B only when EMAs are genuinely mixed or compressing
- Each bullet is ONE sentence with specific values
- If sfi signal is "Insufficient data", skip smart money entirely
- Support/Resistance must use actual values from the data
- Never say "will rise" or "guaranteed" """


def build_prompt(result: TechnicalAnalysisResult) -> str:
    data = result.to_dict()

    if data["sfi"]["value"] is None:
        data.pop("sfi")

    return f"""Write a technical brief for this token data.
Follow the exact format from your instructions.

Data:
{json.dumps(data, indent=2)}"""


def generate_report(result: TechnicalAnalysisResult) -> str:
    prompt = build_prompt(result)

    headers = {
        "Authorization": f"Bearer {get_api_key()}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "max_tokens": 500,
        "temperature": 0.2,
    }

    resp = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


# ── 测试入口 ──
if __name__ == "__main__":
    from technical_analysis import run_technical_analysis

    for token in ["ETH", "BTC", "SOL"]:
        print(f"\n{'='*60}")
        result = run_technical_analysis(token)
        report = generate_report(result)
        print(report)
        print()