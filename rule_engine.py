"""
规则引擎模块 (The Brain)
基于 if-then 逻辑的策略树，每小时遍历所有 Active 状态的 Campaign/AdGroup
输出四类信号：止损、扩量、素材优化、竞品洞察
"""

import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from google.cloud import bigquery
from dotenv import load_dotenv
from config.data_source import get_data_source_config


class SignalType(Enum):
    """信号类型"""
    STOP_LOSS = "stop_loss"          # 止损信号 - 最优先
    SCALE_UP = "scale_up"            # 扩量信号
    CREATIVE_REFRESH = "creative_refresh"  # 素材优化
    COMPETITOR_INSIGHT = "competitor_insight"  # 竞品洞察


class SignalPriority(Enum):
    """信号优先级"""
    CRITICAL = 1   # 紧急 - 立即处理
    HIGH = 2       # 高 - 当天处理
    MEDIUM = 3     # 中 - 关注
    LOW = 4        # 低 - 参考


@dataclass
class Signal:
    """策略信号"""
    signal_type: SignalType
    priority: SignalPriority
    campaign_id: str
    campaign_name: str
    optimizer: str
    message: str
    action: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signal_type": self.signal_type.value,
            "priority": self.priority.value,
            "campaign_id": self.campaign_id,
            "campaign_name": self.campaign_name,
            "optimizer": self.optimizer,
            "message": self.message,
            "action": self.action,
            "metrics": self.metrics,
            "created_at": self.created_at
        }


@dataclass
class RuleConfig:
    """规则配置参数"""
    # 策略A: 止损信号阈值
    stop_loss_min_spend: float = 30.0       # 最低消耗门槛 $30
    stop_loss_max_roas: float = 0.10        # ROAS < 10% 触发止损

    # 策略B: 扩量信号阈值
    scale_up_min_roas: float = 0.40         # ROAS > 40%
    scale_up_min_spend: float = 50.0        # 消耗 > $50
    scale_up_target_cpi: float = 2.0        # CPI < 目标值

    # 策略C: 素材优化阈值
    creative_refresh_ctr_drop: float = 0.20  # CTR 环比下降 20%
    creative_refresh_min_ctr: float = 0.01   # CTR < 1%

    # 通用配置
    lookback_days: int = 1                   # 回溯天数 (D0)


class RuleEngine:
    """规则引擎核心类"""

    def __init__(self, project_id: str, dataset_id: str = None, config: RuleConfig = None, table_id: str = None):
        """
        初始化规则引擎

        Args:
            project_id: GCP 项目 ID
            dataset_id: BigQuery 数据集 ID，默认从配置读取
            config: 规则配置
            table_id: 表 ID，默认从配置读取
        """
        # 从配置获取默认数据源
        data_source_config = get_data_source_config()

        self.project_id = project_id
        self.dataset_id = dataset_id or data_source_config["dataset_id"]
        self.table_id = table_id or data_source_config["table_id"]
        self.config = config or RuleConfig()
        self.client = bigquery.Client(project=project_id)
        self.signals: List[Signal] = []

    def run(self, date: str = None) -> List[Signal]:
        """
        运行规则引擎，生成所有信号

        Args:
            date: 分析日期，默认今天

        Returns:
            信号列表，按优先级排序
        """
        if date is None:
            # 数据日期按 KST (dbt kst_date 口径), 不能用本地北京时钟
            date = (datetime.utcnow() + timedelta(hours=9)).strftime('%Y-%m-%d')

        print(f"[RuleEngine] 开始分析 {date} 的数据...")
        self.signals = []

        # 获取活跃 Campaign 数据
        campaigns = self._get_active_campaigns(date)
        print(f"[RuleEngine] 找到 {len(campaigns)} 个活跃 Campaign")

        # 遍历每个 Campaign 执行策略检查
        for campaign in campaigns:
            # 策略A: 止损检查 (最优先)
            stop_loss_signal = self._check_stop_loss(campaign)
            if stop_loss_signal:
                self.signals.append(stop_loss_signal)
                continue  # 止损信号优先，跳过其他检查

            # 策略B: 扩量检查
            scale_up_signal = self._check_scale_up(campaign)
            if scale_up_signal:
                self.signals.append(scale_up_signal)

            # 策略C: 素材优化检查
            creative_signal = self._check_creative_refresh(campaign, date)
            if creative_signal:
                self.signals.append(creative_signal)

        # 按优先级排序
        self.signals.sort(key=lambda s: s.priority.value)

        print(f"[RuleEngine] 生成 {len(self.signals)} 个信号")
        return self.signals

    def _get_active_campaigns(self, date: str) -> List[Dict[str, Any]]:
        """获取活跃状态的 Campaign 数据"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        query = f"""
        SELECT
            campaign_id,
            campaign_name,
            optimizer,
            channel,
            country_code,
            SUM(spend) as spend,
            SUM(new_users_mkt) as new_users_mkt,
            SUM(d24h_revenue) as revenue,
            SUM(impression) as impression,
            SUM(clicks) as clicks,
            SAFE_DIVIDE(SUM(mmp_total_revenue), SUM(spend)) as mmp_roas,
            SAFE_DIVIDE(SUM(spend), SUM(new_users_mkt)) as cpi,
            SAFE_DIVIDE(SUM(clicks), SUM(impression)) as ctr,
            SAFE_DIVIDE(SUM(new_users_mkt), SUM(clicks)) as cvr
        FROM `{table_ref}`
        WHERE kst_date = '{date}'
          AND campaign_status = 'Active'
        GROUP BY campaign_id, campaign_name, optimizer, channel, country_code
        HAVING spend > 0
        """

        try:
            result = self.client.query(query).result()
            campaigns = []
            for row in result:
                campaigns.append({
                    "campaign_id": row.campaign_id,
                    "campaign_name": row.campaign_name,
                    "optimizer": row.optimizer or "未知",
                    "channel": row.channel,
                    "country_code": row.country_code,
                    "spend": float(row.spend or 0),
                    "new_users_mkt": int(row.new_users_mkt or 0),
                    "revenue": float(row.revenue or 0),
                    "impression": int(row.impression or 0),
                    "clicks": int(row.clicks or 0),
                    "mmp_roas": float(row.mmp_roas or 0),
                    "cpi": float(row.cpi or 0),
                    "ctr": float(row.ctr or 0),
                    "cvr": float(row.cvr or 0),
                })
            return campaigns
        except Exception as e:
            print(f"[RuleEngine] 查询失败: {e}")
            return []

    def _check_stop_loss(self, campaign: Dict[str, Any]) -> Optional[Signal]:
        """
        策略A: 止损信号检查
        逻辑: Spend > $30 且 MMP ROAS < 10% (或 Revenue = 0)
        """
        spend = campaign["spend"]
        mmp_roas = campaign["mmp_roas"]
        revenue = campaign["revenue"]

        # 检查止损条件
        if spend > self.config.stop_loss_min_spend:
            if revenue == 0 or mmp_roas < self.config.stop_loss_max_roas:
                return Signal(
                    signal_type=SignalType.STOP_LOSS,
                    priority=SignalPriority.CRITICAL,
                    campaign_id=campaign["campaign_id"],
                    campaign_name=campaign["campaign_name"],
                    optimizer=campaign["optimizer"],
                    message=f"消耗 ${spend:.2f}，MMP ROAS {mmp_roas:.1%}，收入 ${revenue:.2f}",
                    action="立即关停",
                    metrics={
                        "spend": spend,
                        "mmp_roas": mmp_roas,
                        "revenue": revenue,
                        "channel": campaign["channel"],
                        "country_code": campaign["country_code"]
                    }
                )
        return None

    def _check_scale_up(self, campaign: Dict[str, Any]) -> Optional[Signal]:
        """
        策略B: 扩量信号检查
        逻辑: MMP ROAS > 40% 且 Spend > $50 且 CPI < 目标值
        """
        spend = campaign["spend"]
        mmp_roas = campaign["mmp_roas"]
        cpi = campaign["cpi"]

        if (mmp_roas > self.config.scale_up_min_roas and
            spend > self.config.scale_up_min_spend and
            cpi < self.config.scale_up_target_cpi):
            return Signal(
                signal_type=SignalType.SCALE_UP,
                priority=SignalPriority.HIGH,
                campaign_id=campaign["campaign_id"],
                campaign_name=campaign["campaign_name"],
                optimizer=campaign["optimizer"],
                message=f"MMP ROAS {mmp_roas:.1%}，CPI ${cpi:.2f}，消耗 ${spend:.2f}",
                action="建议预算上调 20% 或复制计划到其他版位",
                metrics={
                    "spend": spend,
                    "mmp_roas": mmp_roas,
                    "cpi": cpi,
                    "new_users_mkt": campaign["new_users_mkt"],
                    "channel": campaign["channel"],
                    "country_code": campaign["country_code"]
                }
            )
        return None

    def _check_creative_refresh(self, campaign: Dict[str, Any], date: str) -> Optional[Signal]:
        """
        策略C: 素材优化检查
        逻辑: 计划ROAS达标，但 CTR 呈下降趋势 (环比昨日下降20%) 或 CTR < 1%
        """
        ctr = campaign["ctr"]
        mmp_roas = campaign["mmp_roas"]

        # 只检查 ROAS 达标的计划
        if mmp_roas < self.config.scale_up_min_roas:
            return None

        # 获取昨日 CTR 进行环比
        yesterday_ctr = self._get_yesterday_ctr(campaign["campaign_id"], date)

        ctr_drop = 0
        if yesterday_ctr and yesterday_ctr > 0:
            ctr_drop = (yesterday_ctr - ctr) / yesterday_ctr

        # 检查 CTR 下降或过低
        if ctr_drop > self.config.creative_refresh_ctr_drop or ctr < self.config.creative_refresh_min_ctr:
            # 获取 XMP 素材推荐
            top_materials = self._get_top_materials(campaign["channel"])
            recommendation = self._format_material_recommendation(top_materials)

            return Signal(
                signal_type=SignalType.CREATIVE_REFRESH,
                priority=SignalPriority.MEDIUM,
                campaign_id=campaign["campaign_id"],
                campaign_name=campaign["campaign_name"],
                optimizer=campaign["optimizer"],
                message=f"CTR {ctr:.2%} (环比 {-ctr_drop:.1%})，素材疲劳",
                action=f"建议更换素材库中 TOP3 视频{recommendation}",
                metrics={
                    "ctr": ctr,
                    "yesterday_ctr": yesterday_ctr,
                    "ctr_drop": ctr_drop,
                    "mmp_roas": mmp_roas,
                    "top_materials": top_materials
                }
            )
        return None

    def _get_yesterday_ctr(self, campaign_id: str, date: str) -> Optional[float]:
        """获取昨日 CTR"""
        yesterday = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        query = f"""
        SELECT SAFE_DIVIDE(SUM(clicks), SUM(impression)) as ctr
        FROM `{table_ref}`
        WHERE kst_date = '{yesterday}'
          AND campaign_id = '{campaign_id}'
        """

        try:
            result = self.client.query(query).result()
            for row in result:
                return float(row.ctr) if row.ctr else None
        except Exception:
            return None

    def _get_top_materials(self, channel: str, limit: int = 3) -> List[Dict[str, Any]]:
        """从 XMP 获取该渠道表现最好的素材"""
        table_ref = f"{self.project_id}.{self.dataset_id}.xmp_materials"

        query = f"""
        SELECT
            user_material_id,
            user_material_name,
            click_rate,
            conversion_rate,
            cost
        FROM `{table_ref}`
        WHERE channel = '{channel}'
          AND cost > 10
        ORDER BY conversion_rate DESC, click_rate DESC
        LIMIT {limit}
        """

        try:
            result = self.client.query(query).result()
            materials = []
            for row in result:
                materials.append({
                    "material_id": row.user_material_id,
                    "material_name": row.user_material_name,
                    "ctr": float(row.click_rate or 0),
                    "cvr": float(row.conversion_rate or 0)
                })
            return materials
        except Exception:
            return []

    def _format_material_recommendation(self, materials: List[Dict[str, Any]]) -> str:
        """格式化素材推荐"""
        if not materials:
            return ""
        names = [m["material_name"][:20] for m in materials[:3]]
        return f": {', '.join(names)}"

    def get_signals_by_type(self, signal_type: SignalType) -> List[Signal]:
        """按类型获取信号"""
        return [s for s in self.signals if s.signal_type == signal_type]

    def get_signals_by_optimizer(self, optimizer: str) -> List[Signal]:
        """按优化师获取信号"""
        return [s for s in self.signals if s.optimizer == optimizer]

    def get_summary(self) -> Dict[str, Any]:
        """获取信号汇总"""
        return {
            "total": len(self.signals),
            "stop_loss": len(self.get_signals_by_type(SignalType.STOP_LOSS)),
            "scale_up": len(self.get_signals_by_type(SignalType.SCALE_UP)),
            "creative_refresh": len(self.get_signals_by_type(SignalType.CREATIVE_REFRESH)),
            "by_priority": {
                "critical": len([s for s in self.signals if s.priority == SignalPriority.CRITICAL]),
                "high": len([s for s in self.signals if s.priority == SignalPriority.HIGH]),
                "medium": len([s for s in self.signals if s.priority == SignalPriority.MEDIUM]),
                "low": len([s for s in self.signals if s.priority == SignalPriority.LOW]),
            }
        }

    def format_for_lark(self) -> List[Dict[str, Any]]:
        """格式化信号为 Lark 消息格式"""
        messages = []

        # 按优化师分组
        optimizer_signals: Dict[str, List[Signal]] = {}
        for signal in self.signals:
            opt = signal.optimizer
            if opt not in optimizer_signals:
                optimizer_signals[opt] = []
            optimizer_signals[opt].append(signal)

        # 为每个优化师生成消息
        for optimizer, signals in optimizer_signals.items():
            # 止损信号 - 紧急
            stop_loss = [s for s in signals if s.signal_type == SignalType.STOP_LOSS]
            if stop_loss:
                messages.append({
                    "optimizer": optimizer,
                    "type": "stop_loss",
                    "level": "error",
                    "title": f"🚨 止损告警 ({len(stop_loss)}个)",
                    "signals": [s.to_dict() for s in stop_loss]
                })

            # 扩量信号
            scale_up = [s for s in signals if s.signal_type == SignalType.SCALE_UP]
            if scale_up:
                messages.append({
                    "optimizer": optimizer,
                    "type": "scale_up",
                    "level": "info",
                    "title": f"📈 扩量机会 ({len(scale_up)}个)",
                    "signals": [s.to_dict() for s in scale_up]
                })

            # 素材优化信号
            creative = [s for s in signals if s.signal_type == SignalType.CREATIVE_REFRESH]
            if creative:
                messages.append({
                    "optimizer": optimizer,
                    "type": "creative_refresh",
                    "level": "warning",
                    "title": f"🎨 素材优化 ({len(creative)}个)",
                    "signals": [s.to_dict() for s in creative]
                })

        return messages


# ============ 便捷函数 ============

def run_rule_engine(project_id: str, dataset_id: str, date: str = None,
                    config: RuleConfig = None) -> List[Signal]:
    """
    运行规则引擎的便捷函数

    Args:
        project_id: GCP 项目 ID
        dataset_id: BigQuery 数据集 ID
        date: 分析日期
        config: 规则配置

    Returns:
        信号列表
    """
    engine = RuleEngine(project_id, dataset_id, config)
    return engine.run(date)


# ============ 测试代码 ============

if __name__ == "__main__":
    load_dotenv()

    project_id = os.getenv('BQ_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET_ID')

    if not project_id or not dataset_id:
        print("错误: 请在 .env 文件中设置 BQ_PROJECT_ID 和 BQ_DATASET_ID")
        exit(1)

    # 自定义配置（可选）
    config = RuleConfig(
        stop_loss_min_spend=30.0,
        stop_loss_max_roas=0.10,
        scale_up_min_roas=0.40,
        scale_up_min_spend=50.0,
        scale_up_target_cpi=2.0,
    )

    # 运行规则引擎
    engine = RuleEngine(project_id, dataset_id, config)
    signals = engine.run()

    # 打印结果
    print("\n" + "="*60)
    print("规则引擎执行结果")
    print("="*60)

    summary = engine.get_summary()
    print(f"\n总信号数: {summary['total']}")
    print(f"  - 止损信号: {summary['stop_loss']}")
    print(f"  - 扩量信号: {summary['scale_up']}")
    print(f"  - 素材优化: {summary['creative_refresh']}")

    print("\n详细信号:")
    for signal in signals:
        priority_emoji = {
            SignalPriority.CRITICAL: "🚨",
            SignalPriority.HIGH: "⚠️",
            SignalPriority.MEDIUM: "📊",
            SignalPriority.LOW: "ℹ️"
        }
        print(f"\n{priority_emoji[signal.priority]} [{signal.signal_type.value}] {signal.campaign_name}")
        print(f"   优化师: {signal.optimizer}")
        print(f"   消息: {signal.message}")
        print(f"   建议: {signal.action}")
