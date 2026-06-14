import json

class DashboardCalculation:

    @staticmethod
    def build_tokens_batch_prompt(tokens_data: list) -> tuple[str, str]:
        system_prompt = """You are a Web3 data analyst.
Based on the provided on-chain data for 10 tokens, write a short, professional one-sentence analysis in English (under 12 words) for each token.
Strictly return a JSON object where the Key is the token Symbol and the Value is the analysis string.
Example: {"BTC": "Surging volume with low MEV, suggesting institutional accumulation.", "ETH": "High-frequency smart money inflow, strong short-term momentum."}
Note: Do not output any markdown formatting, return pure JSON only."""

        # 浓缩喂给大模型的数据，节省 Token
        clean_data = {
            t["symbol"]: f"Vol:${t['volume_1h_usd']}, MEV:{t['mev_toxicity_pct']}%, Score:{t['ai_score']}"
            for t in tokens_data
        }

        user_prompt = f"Please generate the analysis. Data:\n{json.dumps(clean_data)}"
        return system_prompt, user_prompt

    @staticmethod
    def build_addresses_batch_prompt(addresses_data: list) -> tuple[str, str]:
        system_prompt = """You are a Smart Money tracking expert.
Based on the provided on-chain data for 10 addresses, write a short, sharp one-sentence analysis in English (under 12 words) for each address.
Strictly return a JSON object where the Key is the wallet address and the Value is the analysis string.
Example: {"0x123...": "Extremely high win rate, focusing on high-frequency swing trades.", "0x456...": "Incredible PnL ratio, recommend monitoring their accumulation targets."}
Note: Do not output any markdown formatting, return pure JSON only."""

        clean_data = {
            a["address"]: f"WinRate:{a['win_rate']}%, PnL:{a['pnl_ratio']}x, Tags:{a['tags']}"
            for a in addresses_data
        }

        user_prompt = f"Please generate the analysis. Data:\n{json.dumps(clean_data)}"
        return system_prompt, user_prompt