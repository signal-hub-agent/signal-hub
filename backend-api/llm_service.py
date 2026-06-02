"""
信号参谋 - LLM 一句话总结生成
调用 Anthropic API，根据各维度数据生成操作参考
"""
import anthropic
from models import TokenSignal


def generate_llm_summary(signal: TokenSignal) -> str:
    """
    根据跟单指数各维度数据，让LLM生成一句话操作参考
    明确区分真实数据和Mock数据，保持保守措辞
    """
    vt = signal.volume_trend
    mev = signal.mev_toxicity

    prompt = f"""你是一个链上数据分析助手，请根据以下代币分析数据，给出一句话操作参考（不超过80字）。

代币：{signal.token_symbol}
跟单指数：{signal.total_score}/100（{signal.color.value}）

【真实数据维度】
1. 成交量趋势（{signal.volume_trend.score}/15分）
   - 近24h成交量：${vt.volume_24h:,.0f}
   - 近7日日均：${vt.volume_7d_avg:,.0f}
   - 量比：{vt.ratio:.2f}x（{vt.get_trend_label()}）
   - 近24h交易笔数：{vt.data_points_24h}笔

2. MEV污染度（{signal.mev_toxicity.score}/25分）
   - 污染度：{mev.toxicity_pct:.1f}%（{mev.get_toxicity_label()}）
   - 总交易：{mev.total_txs}笔，疑似MEV：{mev.suspicious_txs}笔

【暂为模拟数据，请不要在结论中引用】
3. 合约安全：开发中
4. 技术面指标：开发中

要求：
- 结论基于真实数据（成交量 + MEV）
- 不说"一定""必涨""必跌"等绝对化词语
- 对开发中的维度不作任何引用
- 用中文，100字以内
- 格式：直接输出结论，不需要前缀

示例格式："近24h量能{vt.get_trend_label()}，MEV污染处于低/中/高水平，链上交易活跃度[描述]，建议[操作参考]。"
"""

    client = anthropic.Anthropic()
    message = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}]
    )
    
    return message.content[0].text.strip()