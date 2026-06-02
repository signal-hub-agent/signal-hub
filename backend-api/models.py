"""
信号参谋 - 数据模型定义
所有维度的输入输出结构
"""
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class SignalColor(str, Enum):
    GREEN = "green"   # >= 80分，可以考虑跟单
    YELLOW = "yellow" # 60-79分，谨慎观望
    RED = "red"       # < 60分，风险较高


@dataclass
class VolumeTrend:
    """成交量趋势 - 真实数据"""
    volume_24h: float          # 近24小时成交量 (USD)
    volume_7d_avg: float       # 近7日日均成交量 (USD)
    ratio: float               # 比值 = volume_24h / volume_7d_avg
    score: int                 # 得分 0-15
    data_points_24h: int       # 近24h交易笔数
    is_real_data: bool = True

    def get_trend_label(self) -> str:
        if self.ratio >= 1.5:
            return "量能显著放大"
        elif self.ratio >= 1.0:
            return "量能温和放大"
        elif self.ratio >= 0.5:
            return "量能正常"
        else:
            return "量能萎缩"


@dataclass
class MevToxicity:
    """MEV污染度 - 真实数据（初版）"""
    total_txs: int             # 总交易笔数
    suspicious_txs: int        # 疑似MEV交易笔数
    toxicity_pct: float        # 污染度百分比
    score: int                 # 得分 0-25
    is_real_data: bool = True
    note: str = "初版检测：同一区块内同地址买卖配对，精度待提升"

    def get_toxicity_label(self) -> str:
        if self.toxicity_pct < 10:
            return "MEV污染低"
        elif self.toxicity_pct < 30:
            return "MEV污染中等"
        else:
            return "MEV污染严重"


@dataclass
class ContractSafety:
    """合约安全 - 当前为Mock数据"""
    score: int = 28            # 0-30，默认28
    risk_items: List[str] = field(default_factory=lambda: [
        "合约安全扫描模块开发中，当前为默认评估值"
    ])
    is_real_data: bool = False
    note: str = "需要RPC获取字节码进行分析，完整版支持增发/黑名单/流动性锁定检测"


@dataclass
class TechnicalBias:
    """技术面多空 - 当前为Mock数据"""
    indicators: List[dict] = field(default_factory=lambda: [
        {"name": "均线(MA)", "status": "多头", "emoji": "📈"},
        {"name": "MACD", "status": "多头", "emoji": "📈"},
        {"name": "RSI", "status": "中性", "emoji": "➡️"},
        {"name": "布林带", "status": "多头", "emoji": "📈"},
        {"name": "筹码分布", "status": "多头", "emoji": "📈"},
    ])
    bull_count: int = 4        # 看多指标数
    bear_count: int = 0        # 看空指标数
    neutral_count: int = 1     # 中性指标数
    score: int = 24            # 0-30
    is_real_data: bool = False
    note: str = "需要接入K线数据源（Bybit API），完整版支持MA/MACD/RSI/布林带实时计算"


@dataclass
class TokenSignal:
    """完整的代币信号结果"""
    token_symbol: str
    
    # 四个维度
    volume_trend: VolumeTrend
    mev_toxicity: MevToxicity
    contract_safety: ContractSafety
    technical_bias: TechnicalBias
    
    # 汇总
    total_score: int = 0
    color: SignalColor = SignalColor.YELLOW
    llm_summary: str = ""
    
    # 数据时间范围
    data_window_hours: int = 24

    def compute_total(self):
        """计算总分和颜色信号"""
        self.total_score = (
            self.contract_safety.score +
            self.mev_toxicity.score +
            self.technical_bias.score +
            self.volume_trend.score
        )
        if self.total_score >= 80:
            self.color = SignalColor.GREEN
        elif self.total_score >= 60:
            self.color = SignalColor.YELLOW
        else:
            self.color = SignalColor.RED
        return self

    def to_dict(self) -> dict:
        """转为前端友好的字典格式"""
        return {
            "token_symbol": self.token_symbol,
            "total_score": self.total_score,
            "max_score": 100,
            "color": self.color.value,
            "llm_summary": self.llm_summary,
            "dimensions": {
                "volume_trend": {
                    "label": "成交量趋势",
                    "score": self.volume_trend.score,
                    "max_score": 15,
                    "is_real_data": True,
                    "details": {
                        "volume_24h_usd": round(self.volume_trend.volume_24h, 2),
                        "volume_7d_avg_usd": round(self.volume_trend.volume_7d_avg, 2),
                        "ratio": round(self.volume_trend.ratio, 2),
                        "tx_count_24h": self.volume_trend.data_points_24h,
                        "trend_label": self.volume_trend.get_trend_label(),
                    }
                },
                "mev_toxicity": {
                    "label": "MEV污染度",
                    "score": self.mev_toxicity.score,
                    "max_score": 25,
                    "is_real_data": True,
                    "details": {
                        "total_txs": self.mev_toxicity.total_txs,
                        "suspicious_txs": self.mev_toxicity.suspicious_txs,
                        "toxicity_pct": round(self.mev_toxicity.toxicity_pct, 2),
                        "toxicity_label": self.mev_toxicity.get_toxicity_label(),
                        "note": self.mev_toxicity.note,
                    }
                },
                "contract_safety": {
                    "label": "合约安全",
                    "score": self.contract_safety.score,
                    "max_score": 30,
                    "is_real_data": False,
                    "status": "开发中",
                    "details": {
                        "risk_items": self.contract_safety.risk_items,
                        "note": self.contract_safety.note,
                    }
                },
                "technical_bias": {
                    "label": "技术面多空",
                    "score": self.technical_bias.score,
                    "max_score": 30,
                    "is_real_data": False,
                    "status": "开发中",
                    "details": {
                        "indicators": self.technical_bias.indicators,
                        "bull_count": self.technical_bias.bull_count,
                        "bear_count": self.technical_bias.bear_count,
                        "neutral_count": self.technical_bias.neutral_count,
                        "note": self.technical_bias.note,
                    }
                },
            }
        }