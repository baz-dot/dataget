"""
Lark (飞书) 机器人播报模块
支持：群消息、@指定人、富文本卡片消息、策略信号推送
"""

import requests
import json
import hashlib
import base64
import hmac
import time
from typing import Optional, List, Dict, Any

# 尝试导入 Gemini Advisor
try:
    from .gemini_advisor import GeminiAdvisor, create_advisor
    GEMINI_AVAILABLE = True
except ImportError:
    try:
        from gemini_advisor import GeminiAdvisor, create_advisor
        GEMINI_AVAILABLE = True
    except ImportError:
        GEMINI_AVAILABLE = False
        GeminiAdvisor = None
        create_advisor = None


# 优化师 -> 飞书 open_id 映射表 (需要配置)
OPTIMIZER_USER_MAP: Dict[str, str] = {
    # "张三": "ou_xxxxxxxxxxxx",
    # "李四": "ou_yyyyyyyyyyyy",
}

# ============ 默认配置 ============
DEFAULT_CONFIG = {
    "roas_green_threshold": 0.40,    # ROAS >= 40%: 绿色 (🌟 S级)
    "roas_yellow_threshold": 0.35,   # 35% <= ROAS < 40%: 黄色 (⚠️ 效率下滑)
    # ROAS < 35%: 红色 (🚨 需关注)
}


# ============ 数据清洗工具函数 ============
def format_currency(value: float, currency: str = "$", default: str = "-") -> str:
    """
    格式化货币显示，保留2位小数

    Args:
        value: 金额数值
        currency: 货币符号
        default: 空值时的默认显示

    Returns:
        格式化后的字符串，如 "$12,345.67"
    """
    if value is None or (isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf'))):
        return default
    try:
        return f"{currency}{value:,.2f}"
    except (TypeError, ValueError):
        return default


def format_roas(value: float, default: str = "0.00%") -> str:
    """
    格式化 ROAS 显示为百分比

    Args:
        value: ROAS 数值 (如 0.437 表示 43.7%)
        default: 空值或无穷大时的默认显示

    Returns:
        格式化后的字符串，如 "43.7%"
    """
    if value is None:
        return default
    # 检查 NaN 和无穷大
    if isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
        return default
    try:
        return f"{value:.1%}"
    except (TypeError, ValueError):
        return default


def safe_get_number(data: dict, key: str, default: float = 0) -> float:
    """
    安全获取数值，处理空值和无穷大

    Args:
        data: 数据字典
        key: 键名
        default: 默认值

    Returns:
        数值，如果是空值或无穷大则返回默认值
    """
    value = data.get(key)
    if value is None:
        return default
    if isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class LarkBot:
    """飞书机器人播报类"""

    def __init__(self, webhook_url: str, secret: str = None, config: Dict[str, Any] = None,
                 gemini_api_key: str = None):
        """
        初始化飞书机器人

        Args:
            webhook_url: 机器人 Webhook 地址
            secret: 签名密钥（可选，用于安全验证）
            config: 配置参数（可选），包含:
                - roas_green_threshold: ROAS 绿色阈值 (默认 0.40)
                - roas_yellow_threshold: ROAS 黄色阈值 (默认 0.30)
            gemini_api_key: Gemini API Key（可选，用于 AI 策略建议）
        """
        self.webhook_url = webhook_url
        self.secret = secret
        # 合并默认配置和用户配置
        self.config = {**DEFAULT_CONFIG, **(config or {})}

        # 初始化 Gemini Advisor
        self.gemini_advisor = None
        if GEMINI_AVAILABLE:
            try:
                # 优先使用传入的 key，其次从 config，最后从环境变量
                import os
                api_key = gemini_api_key or (config and config.get("gemini_api_key")) or os.getenv("GEMINI_API_KEY")
                if api_key:
                    self.gemini_advisor = create_advisor(api_key)
            except Exception:
                pass  # Gemini 初始化失败，使用规则降级

    def _gen_sign(self, timestamp: str) -> str:
        """生成签名"""
        if not self.secret:
            return None
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        return base64.b64encode(hmac_code).decode("utf-8")

    def send_text(self, text: str, at_all: bool = False, at_user_ids: List[str] = None) -> dict:
        """
        发送文本消息

        Args:
            text: 消息内容
            at_all: 是否@所有人
            at_user_ids: 要@的用户 open_id 列表
        """
        # 构建@内容
        if at_all:
            text = f"<at user_id=\"all\">所有人</at>\n{text}"
        elif at_user_ids:
            at_text = "".join([f"<at user_id=\"{uid}\"></at> " for uid in at_user_ids])
            text = f"{at_text}\n{text}"

        payload = {
            "msg_type": "text",
            "content": {"text": text}
        }
        return self._send(payload)

    def send_card(self, title: str, content: List[Dict], color: str = "blue",
                  at_user_ids: List[str] = None) -> dict:
        """
        发送卡片消息（适合数据播报）

        Args:
            title: 卡片标题
            content: 卡片内容列表，每项为 {"label": "指标", "value": "数值"}
            color: 卡片颜色 (blue/green/red/orange/purple)
            at_user_ids: 要@的用户 open_id 列表
        """
        # 构建内容元素
        elements = []

        # 添加@用户
        if at_user_ids:
            at_elements = [{"tag": "at", "user_id": uid} for uid in at_user_ids]
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": " ".join([f"<at id={uid}></at>" for uid in at_user_ids])
                }
            })
            elements.append({"tag": "hr"})

        # 添加数据行
        for item in content:
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**{item['label']}**: {item['value']}"
                }
            })

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": title},
                    "template": color
                },
                "elements": elements
            }
        }
        return self._send(payload)

    def send_market_report(self, report_data: Dict[str, Any], at_user_ids: List[str] = None) -> dict:
        """
        发送市场监控报告

        Args:
            report_data: 报告数据
            at_user_ids: 要@的用户
        """
        content = [
            {"label": "📊 监控日期", "value": report_data.get("date", "-")},
            {"label": "📈 总曝光量", "value": f"{report_data.get('impressions', 0):,}"},
            {"label": "👆 总点击量", "value": f"{report_data.get('clicks', 0):,}"},
            {"label": "💰 总消耗", "value": f"¥{report_data.get('cost', 0):,.2f}"},
            {"label": "📉 CTR", "value": f"{report_data.get('ctr', 0):.2%}"},
            {"label": "💵 CPC", "value": f"¥{report_data.get('cpc', 0):.2f}"},
        ]

        # 根据消耗情况选择颜色
        color = "green" if report_data.get('cost', 0) < report_data.get('budget', float('inf')) else "red"

        return self.send_card(
            title="📢 市场监控日报",
            content=content,
            color=color,
            at_user_ids=at_user_ids
        )

    def send_ad_performance_report(self, report_data: Dict[str, Any], at_user_ids: List[str] = None) -> dict:
        """
        发送投放效果监控报告

        Args:
            report_data: 报告数据
            at_user_ids: 要@的用户
        """
        content = [
            {"label": "📅 统计周期", "value": report_data.get("period", "-")},
            {"label": "🎯 投放渠道", "value": report_data.get("channel", "-")},
            {"label": "📈 转化数", "value": f"{report_data.get('conversions', 0):,}"},
            {"label": "💰 转化成本", "value": f"¥{report_data.get('cpa', 0):.2f}"},
            {"label": "📊 ROI", "value": f"{report_data.get('roi', 0):.2f}"},
            {"label": "⚡ 环比变化", "value": report_data.get("change", "-")},
        ]

        # 根据 ROI 选择颜色
        roi = report_data.get('roi', 0)
        color = "green" if roi >= 1 else "orange" if roi >= 0.5 else "red"

        return self.send_card(
            title="📢 投放效果监控",
            content=content,
            color=color,
            at_user_ids=at_user_ids
        )

    def send_alert(self, alert_type: str, message: str, level: str = "warning",
                   at_user_ids: List[str] = None) -> dict:
        """
        发送告警消息

        Args:
            alert_type: 告警类型
            message: 告警内容
            level: 告警级别 (info/warning/error)
            at_user_ids: 要@的用户
        """
        level_config = {
            "info": {"emoji": "ℹ️", "color": "blue"},
            "warning": {"emoji": "⚠️", "color": "orange"},
            "error": {"emoji": "🚨", "color": "red"}
        }
        config = level_config.get(level, level_config["warning"])

        content = [
            {"label": "告警类型", "value": alert_type},
            {"label": "告警内容", "value": message},
            {"label": "告警时间", "value": time.strftime("%Y-%m-%d %H:%M:%S")},
        ]

        return self.send_card(
            title=f"{config['emoji']} 系统告警",
            content=content,
            color=config["color"],
            at_user_ids=at_user_ids
        )

    def send_strategy_signal(self, signal: Dict[str, Any], at_optimizer: bool = True) -> dict:
        """
        发送策略信号消息

        Args:
            signal: 策略信号数据
            at_optimizer: 是否@对应优化师
        """
        signal_type = signal.get("signal_type", "unknown")
        campaign_name = signal.get("campaign_name", "-")
        optimizer = signal.get("optimizer", "未知")
        message = signal.get("message", "-")
        action = signal.get("action", "-")
        metrics = signal.get("metrics", {})

        # 信号类型配置
        type_config = {
            "stop_loss": {"emoji": "🚨", "title": "止损告警", "color": "red"},
            "scale_up": {"emoji": "📈", "title": "扩量机会", "color": "green"},
            "creative_refresh": {"emoji": "🎨", "title": "素材优化", "color": "orange"},
            "competitor_insight": {"emoji": "🔍", "title": "竞品洞察", "color": "blue"},
        }
        config = type_config.get(signal_type, {"emoji": "📊", "title": "策略信号", "color": "blue"})

        content = [
            {"label": "📋 计划名称", "value": campaign_name},
            {"label": "👤 优化师", "value": optimizer},
            {"label": "📊 数据指标", "value": message},
            {"label": "💡 建议动作", "value": action},
        ]

        # 添加关键指标
        if metrics.get("spend"):
            content.append({"label": "💰 消耗", "value": f"${metrics['spend']:.2f}"})
        if metrics.get("d0_roas"):
            content.append({"label": "📈 D0 ROAS", "value": f"{metrics['d0_roas']:.1%}"})

        # 获取优化师的飞书 ID
        at_user_ids = None
        if at_optimizer and optimizer in OPTIMIZER_USER_MAP:
            at_user_ids = [OPTIMIZER_USER_MAP[optimizer]]

        return self.send_card(
            title=f"{config['emoji']} {config['title']}",
            content=content,
            color=config["color"],
            at_user_ids=at_user_ids
        )

    def send_strategy_batch(self, signals: List[Dict[str, Any]], group_by_optimizer: bool = True) -> List[dict]:
        """
        批量发送策略信号

        Args:
            signals: 信号列表
            group_by_optimizer: 是否按优化师分组发送

        Returns:
            发送结果列表
        """
        results = []

        if group_by_optimizer:
            # 按优化师分组
            optimizer_signals: Dict[str, List[Dict]] = {}
            for signal in signals:
                opt = signal.get("optimizer", "未知")
                if opt not in optimizer_signals:
                    optimizer_signals[opt] = []
                optimizer_signals[opt].append(signal)

            # 为每个优化师发送汇总消息
            for optimizer, opt_signals in optimizer_signals.items():
                result = self._send_optimizer_summary(optimizer, opt_signals)
                results.append(result)
        else:
            # 逐条发送
            for signal in signals:
                result = self.send_strategy_signal(signal)
                results.append(result)

        return results

    def _send_optimizer_summary(self, optimizer: str, signals: List[Dict[str, Any]]) -> dict:
        """发送优化师汇总消息"""
        # 统计各类信号
        stop_loss = [s for s in signals if s.get("signal_type") == "stop_loss"]
        scale_up = [s for s in signals if s.get("signal_type") == "scale_up"]
        creative = [s for s in signals if s.get("signal_type") == "creative_refresh"]

        # 构建汇总内容
        elements = []

        # 获取优化师的飞书 ID
        at_user_ids = None
        if optimizer in OPTIMIZER_USER_MAP:
            at_user_ids = [OPTIMIZER_USER_MAP[optimizer]]
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"<at id={OPTIMIZER_USER_MAP[optimizer]}></at> 您有新的策略信号"
                }
            })
            elements.append({"tag": "hr"})

        # 止损信号 (最重要)
        if stop_loss:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**🚨 止损告警 ({len(stop_loss)}个)**"}
            })
            for s in stop_loss[:5]:  # 最多显示5个
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"• {s['campaign_name']}: {s['message']}"}
                })
            elements.append({"tag": "hr"})

        # 扩量信号
        if scale_up:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**📈 扩量机会 ({len(scale_up)}个)**"}
            })
            for s in scale_up[:5]:
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"• {s['campaign_name']}: {s['message']}"}
                })
            elements.append({"tag": "hr"})

        # 素材优化信号
        if creative:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**🎨 素材优化 ({len(creative)}个)**"}
            })
            for s in creative[:5]:
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"• {s['campaign_name']}: {s['action']}"}
                })

        # 确定卡片颜色
        color = "red" if stop_loss else "orange" if creative else "green"

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"📢 策略信号汇总 - {optimizer}"},
                    "template": color
                },
                "elements": elements
            }
        }
        return self._send(payload)

    # ============ 模版 0: 日报播报 (Daily Report) ============
    def send_daily_report(self, data: Dict[str, Any], bi_link: str = None) -> dict:
        """
        发送日报播报 (Daily Report) - 为管理层提供昨天的全盘复盘

        Args:
            data: 报告数据，包含:
                - date: 日期
                - summary: {total_spend, total_revenue, global_roas}
                - summary_prev: {total_spend, total_revenue, global_roas} (T-2数据，用于环比)
                - optimizers: [{name, spend, roas, campaign_count, top_campaign}]
                - dramas_top5: [{name, spend, roas}]
                - countries_top5: [{name, spend, roas}]
                - scale_up_dramas: [{name, spend, roas}] 放量剧目
                - opportunity_markets: [{drama_name, country, spend, roas}] 机会市场
            bi_link: BI 报表链接
        """
        date = data.get("date", time.strftime("%Y-%m-%d"))
        summary = data.get("summary", {})
        summary_prev = data.get("summary_prev", {})
        optimizers = data.get("optimizers", [])
        dramas_top5 = data.get("dramas_top5", [])
        countries_top5 = data.get("countries_top5", [])
        scale_up_dramas = data.get("scale_up_dramas", [])
        opportunity_markets = data.get("opportunity_markets", [])

        # 计算环比变化
        total_spend = summary.get("total_spend", 0)
        prev_spend = summary_prev.get("total_spend", 0)
        global_roas = summary.get("global_roas", 0)
        prev_roas = summary_prev.get("global_roas", 0)

        spend_change = (total_spend - prev_spend) / prev_spend if prev_spend > 0 else 0
        roas_change = (global_roas - prev_roas) / prev_roas if prev_roas > 0 else 0

        # 环比 emoji
        spend_emoji = "📈" if spend_change >= 0 else "📉"
        roas_emoji = "📈" if roas_change >= 0 else "📉"

        # 生成核心评价
        evaluation = self._generate_daily_evaluation(total_spend, global_roas, spend_change, roas_change)

        # 格式化日期显示 (12.21)
        date_display = f"{date[5:7]}.{date[8:10]}"

        # 获取 ROAS 阈值配置
        roas_green = self.config.get("roas_green_threshold", 0.40)
        roas_yellow = self.config.get("roas_yellow_threshold", 0.30)

        elements = []

        # ========== 板块 1: 大盘核心总结 ==========
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**📅 [{date_display}] 昨日大盘综述**"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 总消耗：**${total_spend:,.2f}** ({spend_emoji} {spend_change:+.0%})"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 综合 ROAS：**{global_roas:.1%}** ({roas_emoji} {roas_change:+.0%})"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 核心评价：{evaluation}"}})
        elements.append({"tag": "hr"})

        # ========== 板块 2: 策略建议 (AI Insight) ==========
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**💡 策略建议**"}})

        # 使用 Gemini 生成智能策略建议
        strategy_insights = self._generate_strategy_insights(data)

        # 放量剧目
        scale_up_text = strategy_insights.get("scale_up_drama", "暂无符合条件的剧目")
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🚀 建议放量剧目：{scale_up_text}"}})

        # 机会市场
        opportunity_text = strategy_insights.get("opportunity_market", "暂无新兴市场机会")
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🌍 机会市场：{opportunity_text}"}})

        # 测剧建议
        test_drama_text = strategy_insights.get("test_drama_suggestion", "")
        if test_drama_text:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• ⚠️ 测剧建议：{test_drama_text}"}})

        elements.append({"tag": "hr"})

        # ========== 板块 3: 投手排行榜 ==========
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🏆 投手表现**"}})

        for i, opt in enumerate(optimizers):
            name = opt.get("name", "未知")
            spend = opt.get("spend", 0)
            roas = opt.get("roas", 0)

            # 评级逻辑（使用配置的阈值）
            if roas >= roas_green:
                rating = "🌟 S级"
            elif roas >= roas_yellow:
                rating = "⚠️ 效率下滑"
            else:
                rating = "🚨 需关注"

            # 消耗格式化
            if spend >= 10000:
                spend_str = f"${spend/10000:.1f}w"
            else:
                spend_str = f"${spend:,.0f}"

            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"{i+1}. **{name}**: 耗 {spend_str} | ROAS {roas:.1%} ({rating})"}})

        elements.append({"tag": "hr"})

        # ========== 板块 4: 数据明细 ==========
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**📊 数据明细**"}})

        # 表1: 分投手 (使用飞书 table 组件)
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**表1: 分投手**"}})
        optimizer_rows = []
        for opt in optimizers:
            top_camp = opt.get("top_campaign", "-")
            optimizer_rows.append({
                "optimizer": opt['name'],
                "spend": f"${opt['spend']:,.0f}",
                "roas": f"{opt['roas']:.1%}",
                "top_campaign": top_camp
            })
        elements.append({
            "tag": "table",
            "columns": [
                {"name": "optimizer", "display_name": "投手"},
                {"name": "spend", "display_name": "消耗"},
                {"name": "roas", "display_name": "ROAS"},
                {"name": "top_campaign", "display_name": "Top Campaign"}
            ],
            "rows": optimizer_rows
        })

        # 表2: 分剧集 Top 5 (使用飞书 table 组件)
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**表2: 分剧集 Top 5**"}})
        drama_rows = []
        for drama in dramas_top5:
            drama_rows.append({
                "drama": drama['name'],
                "spend": f"${drama['spend']:,.0f}",
                "roas": f"{drama['roas']:.1%}"
            })
        elements.append({
            "tag": "table",
            "columns": [
                {"name": "drama", "display_name": "剧集"},
                {"name": "spend", "display_name": "消耗"},
                {"name": "roas", "display_name": "ROAS"}
            ],
            "rows": drama_rows
        })

        # 表3: 分国家 Top 5 (使用飞书 table 组件)
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**表3: 分国家 Top 5**"}})
        country_rows = []
        for country in countries_top5:
            country_rows.append({
                "country": country['name'],
                "spend": f"${country['spend']:,.0f}",
                "roas": f"{country['roas']:.1%}"
            })
        elements.append({
            "tag": "table",
            "columns": [
                {"name": "country", "display_name": "国家"},
                {"name": "spend", "display_name": "消耗"},
                {"name": "roas", "display_name": "ROAS"}
            ],
            "rows": country_rows
        })

        # BI 链接按钮
        if bi_link:
            elements.append({"tag": "hr"})
            elements.append({"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "📊 查看详细 BI 报表"}, "type": "primary", "url": bi_link}
            ]})

        # 根据 ROAS 选择卡片颜色（使用配置的阈值）
        roas_green = self.config.get("roas_green_threshold", 0.40)
        roas_yellow = self.config.get("roas_yellow_threshold", 0.30)

        if global_roas >= roas_green:
            color = "green"
        elif global_roas >= roas_yellow:
            color = "yellow"
        else:
            color = "red"

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"📅 [{date_display}] 昨日大盘日报"},
                    "template": color
                },
                "elements": elements
            }
        }
        return self._send(payload)

    def _generate_daily_evaluation(self, spend: float, roas: float, spend_change: float, roas_change: float) -> str:
        """生成日报核心评价"""
        parts = []

        # 获取 ROAS 阈值配置
        roas_green = self.config.get("roas_green_threshold", 0.40)
        roas_yellow = self.config.get("roas_yellow_threshold", 0.30)

        # ROAS 评价
        if roas >= roas_green:
            parts.append("大盘健康，处于盈利状态")
        else:
            parts.append(f"ROAS未达标({roas:.0%})，需重点关注亏损计划")

        # 消耗趋势评价
        if spend_change > 0.10:
            parts.append("消耗处于扩张期")
        elif spend_change < -0.10:
            parts.append("消耗收缩明显")

        # 警戒线提醒（30%-40% 之间）
        if roas_yellow <= roas < roas_green:
            parts.append(f"逼近{roas_green:.0%}警戒线")

        return "，".join(parts) + "。"

    def _generate_strategy_insights(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        生成策略建议 (AI Insight)

        筛选逻辑：
        - 放量剧目: Spend > $1000 且 ROAS > 45%
        - 机会市场: Spend > $100 且 ROAS > 50% 且 Country 不在主投Top3国家

        Args:
            data: 日报数据

        Returns:
            {
                "scale_up_drama": "放量剧目建议文案",
                "opportunity_market": "机会市场建议文案",
                "test_drama_suggestion": "测剧建议文案"
            }
        """
        # 如果有 Gemini Advisor，使用 AI 生成
        if self.gemini_advisor:
            try:
                return self.gemini_advisor.generate_strategy_insights(data)
            except Exception:
                pass  # AI 生成失败，降级到规则生成

        # 规则降级：使用传入的数据或自动筛选
        return self._fallback_strategy_insights(data)

    def _fallback_strategy_insights(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        降级策略建议生成（基于规则）

        筛选逻辑：
        - 放量剧目: Spend > $1000 且 ROAS > 45%
        - 机会市场: Spend > $100 且 ROAS > 50% 且 Country 不在主投Top3国家
        """
        result = {
            "scale_up_drama": "暂无符合条件的剧目",
            "opportunity_market": "暂无新兴市场机会",
            "test_drama_suggestion": ""
        }

        # 获取数据
        dramas = data.get("dramas", []) or data.get("dramas_top5", [])
        drama_country = data.get("drama_country", []) or data.get("opportunity_markets", [])
        countries_top5 = data.get("countries_top5", [])

        # 主投 Top3 国家
        top3_countries = set(data.get("top3_countries", []))
        if not top3_countries and countries_top5:
            top3_countries = set([c.get("name", "") for c in countries_top5[:3]])

        # 1. 筛选放量剧目: Spend > $1000 且 ROAS > 45%
        scale_up_candidates = [
            d for d in dramas
            if d.get("spend", 0) > 1000 and d.get("roas", 0) > 0.45
        ]
        scale_up_candidates.sort(key=lambda x: x.get("roas", 0), reverse=True)

        if scale_up_candidates:
            d = scale_up_candidates[0]
            result["scale_up_drama"] = f"《{d['name']}》(ROAS {d['roas']:.0%}, 消耗${d['spend']/1000:.1f}k+)"

        # 2. 筛选机会市场: Spend > $100 且 ROAS > 50% 且不在主投Top3
        opportunity_candidates = [
            dc for dc in drama_country
            if dc.get("spend", 0) > 100
            and dc.get("roas", 0) > 0.50
            and dc.get("country", "") not in top3_countries
        ]
        opportunity_candidates.sort(key=lambda x: x.get("roas", 0), reverse=True)

        if opportunity_candidates:
            dc = opportunity_candidates[0]
            drama_name = dc.get("drama_name", dc.get("name", "未知"))
            result["opportunity_market"] = f"剧集《{drama_name}》在 [{dc['country']}] ROAS {dc['roas']:.0%}，建议增投"

        # 3. 测剧建议
        if len(dramas) < 3:
            result["test_drama_suggestion"] = "本周新剧测试数量不足，建议增加素材供给"
        elif len(dramas) < 5:
            result["test_drama_suggestion"] = "在投剧集较少，建议适当增加测试新剧"
        else:
            high_roas_count = len([d for d in dramas if d.get("roas", 0) > 0.45])
            if high_roas_count < 2:
                result["test_drama_suggestion"] = "高效剧集较少，建议加大测剧力度寻找爆款"

        return result

    def _generate_realtime_insights(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        生成实时播报的 AI 建议

        Args:
            data: 实时播报数据

        Returns:
            {
                "overall_assessment": "整体态势评估",
                "stop_loss_advice": "止损建议",
                "scale_up_advice": "扩量建议"
            }
        """
        # 如果有 Gemini Advisor，使用 AI 生成
        if self.gemini_advisor:
            try:
                return self.gemini_advisor.generate_realtime_insights(data)
            except Exception:
                pass  # AI 生成失败，降级到规则生成

        # 规则降级
        return self._fallback_realtime_insights(data)

    def _fallback_realtime_insights(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        实时播报降级策略建议（基于规则）
        """
        summary = data.get("summary", {})
        d0_roas = summary.get("d0_roas", 0)
        stop_loss = data.get("stop_loss_campaigns", [])
        scale_up = data.get("scale_up_campaigns", [])

        result = {
            "overall_assessment": "",
            "stop_loss_advice": "当前无需止损",
            "scale_up_advice": "当前无明显扩量机会"
        }

        # 整体态势
        if d0_roas >= 0.40:
            result["overall_assessment"] = "大盘健康，继续保持当前节奏"
        elif d0_roas >= 0.30:
            result["overall_assessment"] = "效率略低，需关注低效计划"
        else:
            result["overall_assessment"] = "效率偏低，建议收缩消耗、优先止损"

        # 止损建议
        if stop_loss:
            top = stop_loss[0]
            result["stop_loss_advice"] = f"建议关停 {top.get('optimizer', '未知')} 的《{top.get('drama_name', '未知')}》(ROAS {top.get('roas', 0):.0%})"

        # 扩量建议
        if scale_up:
            top = scale_up[0]
            result["scale_up_advice"] = f"建议加投 {top.get('optimizer', '未知')} 的《{top.get('drama_name', '未知')}》(ROAS {top.get('roas', 0):.0%})"

        return result

    # ============ 模版 1: 每日投放战报 ============
    def send_daily_battle_report(self, data: Dict[str, Any], bi_link: str = None) -> dict:
        """
        发送每日投放战报 (Daily Battle Report)

        Args:
            data: 报告数据，包含:
                - date: 日期
                - total_spend: 总消耗
                - spend_change: 消耗环比变化 (如 -0.05 表示 -5%)
                - d0_roas: D0 ROAS
                - roas_target: ROAS 目标
                - optimizers: 投手数据列表 [{name, spend, roas, new_campaigns, comment}]
                - warnings: 警示区数据 [{name, spend, roas, suggestion}]
            bi_link: BI 报表链接
        """
        date = data.get("date", time.strftime("%Y-%m-%d"))
        total_spend = data.get("total_spend", 0)
        spend_change = data.get("spend_change", 0)
        d0_roas = data.get("d0_roas", 0)
        roas_target = data.get("roas_target", 0.4)
        optimizers = data.get("optimizers", [])
        warnings = data.get("warnings", [])

        # 环比变化显示
        change_emoji = "🔴" if spend_change < 0 else "🟢"
        change_text = f"{change_emoji} 环比 {spend_change:+.0%}"

        # ROAS 状态
        roas_status = "🟢 达标" if d0_roas >= roas_target else "🔴 未达标"

        elements = [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**🌍 大盘总览:**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• 总消耗: **${total_spend:,.0f}** ({change_text})"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• D0 ROAS: **{d0_roas:.0%}** ({roas_status})"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**🏆 投手数据 (按消耗排序):**"}},
        ]

        # 投手排名
        medals = ["🥇", "🥈", "🥉"]
        for i, opt in enumerate(optimizers):
            medal = medals[i] if i < 3 else f"{i+1}."
            name = opt.get("name", "未知")
            spend = opt.get("spend", 0)
            roas = opt.get("roas", 0)
            new_campaigns = opt.get("new_campaigns", 0)
            comment = opt.get("comment", "")

            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**{medal} {name}:**"}})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 消耗: ${spend:,.0f} | ROAS: {roas:.0%}"}})
            if new_campaigns:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 新建计划: {new_campaigns}个"}})
            if comment:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 点评: {comment}"}})

        # 警示区
        if warnings:
            elements.append({"tag": "hr"})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**💀 警示区 (ROAS < 30%):**"}})
            for w in warnings[:3]:
                name = w.get("name", "未知")
                spend = w.get("spend", 0)
                roas = w.get("roas", 0)
                suggestion = w.get("suggestion", "")
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• {name}: 消耗 ${spend:,.0f} | ROAS {roas:.0%} 🔴"}})
                if suggestion:
                    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"  系统建议: {suggestion}"}})

        # BI 链接
        if bi_link:
            elements.append({"tag": "hr"})
            elements.append({"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "👉 查看详细BI报表"}, "type": "primary", "url": bi_link}
            ]})

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"🔥 每日投放战报 {date}"},
                    "template": "red" if warnings else "blue"
                },
                "elements": elements
            }
        }
        return self._send(payload)

    # ============ 模版 2: 每日素材产出榜 ============
    def send_daily_creative_report(self, data: Dict[str, Any], xmp_link: str = None) -> dict:
        """
        发送每日素材产出榜 (Daily Creative Report)

        Args:
            data: 报告数据，包含:
                - date: 日期
                - total_creatives: 今日总产素材数
                - hot_creatives: 起量素材数
                - editors: 剪辑师数据列表 [{name, output, ai_output, hot_material, hot_spend, hot_roas}]
                - insight: 爆款特征总结
            xmp_link: XMP 页面链接
        """
        date = data.get("date", time.strftime("%Y-%m-%d"))
        total_creatives = data.get("total_creatives", 0)
        hot_creatives = data.get("hot_creatives", 0)
        editors = data.get("editors", [])
        insight = data.get("insight", "")

        elements = [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**📊 产能概览:**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• 今日总计新产素材: **{total_creatives}条**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• 今日起量素材: **{hot_creatives}条** (消耗>$100, ROAS>40%)"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**🌟 剪辑师表现:**"}},
        ]

        # 剪辑师排名
        for i, editor in enumerate(editors[:5]):
            name = editor.get("name", "未知")
            output = editor.get("output", 0)
            ai_output = editor.get("ai_output", 0)
            hot_material = editor.get("hot_material", "")
            hot_spend = editor.get("hot_spend", 0)
            hot_roas = editor.get("hot_roas", 0)

            ai_text = f" (含{ai_output}条AI混剪)" if ai_output else ""
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**{i+1}. {name}:**"}})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 产出: {output}条{ai_text}"}})
            if hot_material:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 爆款: {hot_material}"}})
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 数据: 消耗 ${hot_spend:,.0f} / ROAS {hot_roas:.0%}"}})
            else:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 爆款: 无"}})

        # 爆款特征总结
        if insight:
            elements.append({"tag": "hr"})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**💡 爆款特征总结:** {insight}"}})

        # XMP 链接
        if xmp_link:
            elements.append({"tag": "hr"})
            elements.append({"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "👉 预览今日爆款视频"}, "type": "primary", "url": xmp_link}
            ]})

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"🎬 素材产出日报 {date}"},
                    "template": "purple"
                },
                "elements": elements
            }
        }
        return self._send(payload)

    # ============ 模版 3: 每周经营复盘 ============
    def send_weekly_review(self, data: Dict[str, Any]) -> dict:
        """
        发送每周经营复盘 (Weekly Review)

        Args:
            data: 报告数据，包含:
                - week: 周次 (如 "W51")
                - period: 周期 (如 "12.16 - 12.22")
                - total_spend: 周总消耗
                - spend_target: 消耗目标
                - avg_roas: 周综合 ROAS
                - roas_target: ROAS 目标
                - groups: 分组表现 [{name, avg_spend, roas, conclusion}]
                - suggestions: 下周策略建议列表
        """
        week = data.get("week", "W??")
        period = data.get("period", "")
        total_spend = data.get("total_spend", 0)
        spend_target = data.get("spend_target", 0)
        avg_roas = data.get("avg_roas", 0)
        roas_target = data.get("roas_target", 0.4)
        groups = data.get("groups", [])
        suggestions = data.get("suggestions", [])

        # 计算完成率
        spend_rate = total_spend / spend_target if spend_target > 0 else 0
        roas_status = "⚠️ 略低" if avg_roas < roas_target else "✅ 达标"

        elements = [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**🎯 OKR 进度:**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• 周总消耗: **${total_spend:,.0f}** (目标完成率: {spend_rate:.0%})"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• 周综合 ROAS: **{avg_roas:.0%}** (目标: {roas_target:.0%}, {roas_status})"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**⚖️ 分组表现:**"}},
        ]

        # 分组表现
        for group in groups:
            name = group.get("name", "未知")
            avg_spend = group.get("avg_spend", 0)
            roas = group.get("roas", 0)
            conclusion = group.get("conclusion", "")
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• {name}: 人均消耗 ${avg_spend:,.0f}/天，ROAS {roas:.0%}"}})

        if groups and groups[0].get("conclusion"):
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 结论: {groups[0].get('conclusion', '')}"}})

        # 下周策略建议
        if suggestions:
            elements.append({"tag": "hr"})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**🚩 下周策略建议:**"}})
            for i, sug in enumerate(suggestions[:5]):
                category = sug.get("category", "")
                content = sug.get("content", "")
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"{i+1}. **{category}**: {content}"}})

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"📈 周度经营复盘 {week} ({period})"},
                    "template": "blue"
                },
                "elements": elements
            }
        }
        return self._send(payload)

    # ============ 模版 4: 紧急止损预警 ============
    def send_stop_loss_alert(self, data: Dict[str, Any], media_link: str = None, bi_link: str = None) -> dict:
        """
        发送紧急止损预警

        Args:
            data: 预警数据，包含:
                - drama_name: 剧集名称
                - campaign_id: Campaign ID
                - spend: 今日消耗
                - d0_roas: D0 ROAS
                - cpi: CPI
                - cpi_baseline: CPI 基线
                - judgment: 模型判断
                - action: 建议操作
            media_link: 媒体后台链接
            bi_link: BI 详情链接
        """
        drama_name = data.get("drama_name", "未知剧集")
        campaign_id = data.get("campaign_id", "")
        spend = data.get("spend", 0)
        d0_roas = data.get("d0_roas", 0)
        cpi = data.get("cpi", 0)
        cpi_baseline = data.get("cpi_baseline", 2)
        judgment = data.get("judgment", "消耗已过测试线，且无明显回收，属于赔钱计划。")
        action = data.get("action", "立即关停")
        optimizer = data.get("optimizer", "")

        elements = [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**检测时间:** {time.strftime('%H:%M')}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**对象:** 剧集《{drama_name}》 / Campaign ID: {campaign_id}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**当前数据:**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• 今日消耗: **${spend:.2f}**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• D0 ROAS: **{d0_roas:.2%}** (极低)"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• CPI: **${cpi:.2f}** (高于基线 ${cpi_baseline})"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**模型判断:** {judgment}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**建议操作:** 🔴 {action}"}},
        ]

        # 操作按钮
        actions = []
        if media_link:
            actions.append({"tag": "button", "text": {"tag": "plain_text", "content": "跳转媒体后台"}, "type": "danger", "url": media_link})
        if bi_link:
            actions.append({"tag": "button", "text": {"tag": "plain_text", "content": "查看BI详情"}, "type": "default", "url": bi_link})
        if actions:
            elements.append({"tag": "action", "actions": actions})

        # @优化师
        at_user_ids = None
        if optimizer and optimizer in OPTIMIZER_USER_MAP:
            at_user_ids = [OPTIMIZER_USER_MAP[optimizer]]

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": "⚠️ 止损建议 - 高耗低效预警"},
                    "template": "red"
                },
                "elements": elements
            }
        }
        return self._send(payload)

    # ============ 模版 5: 扩量与机会建议 ============
    def send_scale_up_suggestion(self, data: Dict[str, Any], media_link: str = None) -> dict:
        """
        发送扩量与机会建议

        Args:
            data: 建议数据，包含:
                - drama_name: 剧集名称
                - campaign_id: Campaign ID
                - spend: 今日消耗
                - d0_roas: D0 ROAS
                - ctr: CTR
                - competitor_insight: 竞品情报
                - suggestions: 建议操作列表
            media_link: 媒体后台链接
        """
        drama_name = data.get("drama_name", "未知剧集")
        campaign_id = data.get("campaign_id", "")
        spend = data.get("spend", 0)
        d0_roas = data.get("d0_roas", 0)
        ctr = data.get("ctr", 0)
        competitor_insight = data.get("competitor_insight", "")
        suggestions = data.get("suggestions", [])
        optimizer = data.get("optimizer", "")

        elements = [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**检测时间:** {time.strftime('%H:%M')}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**对象:** 剧集《{drama_name}》 / Campaign ID: {campaign_id}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**当前数据:**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• 今日消耗: **${spend:.2f}**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• D0 ROAS: **{d0_roas:.0%}** (优异)"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• CTR: **{ctr:.1%}**"}},
        ]

        # 竞品情报
        if competitor_insight:
            elements.append({"tag": "hr"})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**竞品情报(ADX):** {competitor_insight}"}})

        # 建议操作
        if suggestions:
            elements.append({"tag": "hr"})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**建议操作:**"}})
            for i, sug in enumerate(suggestions[:3]):
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"{i+1}. {sug}"}})

        # 操作按钮
        if media_link:
            elements.append({"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "跳转媒体后台"}, "type": "primary", "url": media_link}
            ]})

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": "🚀 扩量建议 - 发现潜力计划"},
                    "template": "green"
                },
                "elements": elements
            }
        }
        return self._send(payload)

    def _send(self, payload: dict) -> dict:
        """发送消息到飞书"""
        # 添加签名
        if self.secret:
            timestamp = str(int(time.time()))
            payload["timestamp"] = timestamp
            payload["sign"] = self._gen_sign(timestamp)

        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(
                self.webhook_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=10
            )
            return response.json()
        except Exception as e:
            return {"code": -1, "msg": str(e)}

    # ============ 模版 6: 实时播报 (Real-time Report) ============
    def send_realtime_report(self, data: Dict[str, Any], prev_data: Dict[str, Any] = None) -> dict:
        """
        发送实时播报 (Real-time Report) - 为执行层提供每小时监控

        触发时间：每日 9:00 - 24:00，每整点触发
        播报群：vigloo投放剪辑群 + 个人推送

        Args:
            data: 当前实时数据，包含:
                - date: 日期
                - current_hour: 当前时间
                - data_delayed: 是否数据延迟
                - api_update_time: API 最后更新时间
                - summary: {total_spend, total_revenue, d0_roas}
                - optimizer_spend: [{optimizer, spend, roas, top_campaigns}]
                - stop_loss_campaigns: [{campaign_name, optimizer, spend, roas}]
                - scale_up_campaigns: [{campaign_name, optimizer, spend, roas}]
                - country_marginal_roas: [{country, spend, roas}]
            prev_data: 上一小时快照数据，用于计算环比
        """
        current_hour = data.get("current_hour", time.strftime("%H:%M"))
        summary = data.get("summary", {})
        data_delayed = data.get("data_delayed", False)
        optimizer_spend = data.get("optimizer_spend", [])
        stop_loss_campaigns = data.get("stop_loss_campaigns", [])
        scale_up_campaigns = data.get("scale_up_campaigns", [])
        country_marginal_roas = data.get("country_marginal_roas", [])

        # 当前值
        total_spend = summary.get("total_spend", 0)
        d0_roas = summary.get("d0_roas", 0)

        # 计算小时环比
        hourly_spend_delta = 0
        roas_trend = 0
        prev_total_spend = 0
        prev_roas = 0

        if prev_data:
            prev_total_spend = prev_data.get("total_spend", 0)
            prev_roas = prev_data.get("d0_roas", 0)
            hourly_spend_delta = total_spend - prev_total_spend
            roas_trend = d0_roas - prev_roas

        # 环比百分比
        hourly_spend_change_pct = (hourly_spend_delta / prev_total_spend * 100) if prev_total_spend > 0 else 0

        elements = []

        # ========== 数据延迟警告 ==========
        if data_delayed:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "**⚠️ 数据延迟** - API 更新时间超过2小时，请关注数据时效性"}
            })
            elements.append({"tag": "hr"})

        # ========== 板块 1: 小时级异动监控 (Hourly Pulse) ==========
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**⏰ 实时战报**"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 截止当前总耗：**${total_spend:,.2f}**"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 当前 D0 ROAS：**{d0_roas:.1%}**"}})

        # 过去1小时新增消耗 (仅当有上小时数据时显示)
        if prev_data and prev_data.get("total_spend", 0) > 0:
            spend_emoji = "🔥" if hourly_spend_change_pct > 10 else "📊"
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"• 过去1小时新增消耗：**${hourly_spend_delta:,.2f}** ({spend_emoji} 较上小时 {hourly_spend_change_pct:+.0f}%)"}
            })

            # ROAS 趋势
            roas_emoji = "↗️ 上升" if roas_trend > 0 else "↘️ 下滑" if roas_trend < 0 else "➡️ 持平"
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"• 过去1小时 ROAS 趋势：{roas_emoji} {abs(roas_trend):.1%}"}
            })
        else:
            # 没有上小时数据时，显示今日累计信息
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "• 📌 今日首次播报，环比数据将在下一小时显示"}})

        elements.append({"tag": "hr"})

        # ========== 板块 2: 核心变化归因 (Change Attribution) ==========
        if prev_data and prev_data.get("total_spend", 0) > 0:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🔍 谁在花钱？(过去1小时变化)**"}})
        else:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🔍 谁在花钱？(今日累计)**"}})

        # 计算每个投手的小时消耗增量
        prev_optimizer_map = {}
        if prev_data and prev_data.get("optimizer_data"):
            for opt in prev_data.get("optimizer_data", []):
                prev_optimizer_map[opt.get("optimizer")] = opt.get("spend", 0)

        optimizer_deltas = []
        for opt in optimizer_spend[:5]:
            optimizer_name = opt.get("optimizer", "未知")
            current_spend = opt.get("spend", 0)
            prev_spend = prev_optimizer_map.get(optimizer_name, 0)
            delta = current_spend - prev_spend

            # 获取主力计划 (包含 drama 和 country)
            top_campaigns = opt.get("top_campaigns", [])
            top_camp_info = []
            for c in top_campaigns[:2]:
                camp_name = c.get("name", "")
                drama = c.get("drama_name", "")
                country = c.get("country", "")
                if drama and country:
                    top_camp_info.append(f"{drama}({country})")
                elif drama:
                    top_camp_info.append(drama)
                elif camp_name:
                    top_camp_info.append(camp_name[:20])

            optimizer_deltas.append({
                "name": optimizer_name,
                "delta": delta,
                "total": current_spend,
                "top_campaigns": top_camp_info
            })

        # 按增量排序
        optimizer_deltas.sort(key=lambda x: x["delta"], reverse=True)

        # 使用表格组件展示投手消耗
        optimizer_rows = []
        for opt in optimizer_deltas[:5]:
            delta = opt["delta"]
            status = "🔥" if delta > 100 else "⚠️ 停滞" if delta < 50 else ""
            camp_str = ", ".join(opt['top_campaigns']) if opt['top_campaigns'] else "-"
            optimizer_rows.append({
                "optimizer": opt['name'],
                "delta": f"${delta:,.0f}",
                "total": f"${opt['total']:,.0f}",
                "top_campaigns": camp_str,
                "status": status
            })

        if optimizer_rows:
            elements.append({
                "tag": "table",
                "columns": [
                    {"name": "optimizer", "display_name": "投手"},
                    {"name": "delta", "display_name": "新增消耗"},
                    {"name": "total", "display_name": "累计消耗"},
                    {"name": "top_campaigns", "display_name": "主力计划"},
                    {"name": "status", "display_name": "状态"}
                ],
                "rows": optimizer_rows
            })

        elements.append({"tag": "hr"})

        # ========== 板块 3: 实时策略建议 (Actionable Insights) ==========
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**⚡️ 操作建议**"}})

        # AI 生成整体态势和具体建议
        ai_insights = self._generate_realtime_insights(data)
        if ai_insights:
            # 整体态势评估
            overall = ai_insights.get("overall_assessment", "")
            if overall:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 📊 整体态势：{overall}"}})

            # AI 止损建议
            stop_advice = ai_insights.get("stop_loss_advice", "")
            if stop_advice and stop_advice != "当前无需止损":
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🤖 AI止损建议：{stop_advice}"}})

            # AI 扩量建议
            scale_advice = ai_insights.get("scale_up_advice", "")
            if scale_advice and scale_advice != "当前无明显扩量机会":
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🤖 AI扩量建议：{scale_advice}"}})

            elements.append({"tag": "hr"})

        # 止损预警 (使用表格组件)
        if stop_loss_campaigns:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🔴 止损预警：**"}})
            stop_loss_rows = []
            for camp in stop_loss_campaigns[:5]:
                stop_loss_rows.append({
                    "optimizer": camp.get("optimizer", "未知"),
                    "campaign": camp.get("campaign_name", ""),
                    "drama": camp.get("drama_name", "-"),
                    "country": camp.get("country", "-"),
                    "spend": f"${camp.get('spend', 0):,.0f}",
                    "roas": f"{camp.get('roas', 0):.0%}",
                    "action": "立即关停"
                })
            elements.append({
                "tag": "table",
                "columns": [
                    {"name": "optimizer", "display_name": "投手"},
                    {"name": "campaign", "display_name": "计划"},
                    {"name": "drama", "display_name": "剧集"},
                    {"name": "country", "display_name": "国家"},
                    {"name": "spend", "display_name": "消耗"},
                    {"name": "roas", "display_name": "ROAS"},
                    {"name": "action", "display_name": "建议"}
                ],
                "rows": stop_loss_rows
            })
        else:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "🔴 止损预警：暂无"}})

        # 扩量机会 (使用表格组件)
        if scale_up_campaigns:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🟢 扩量机会：**"}})
            scale_up_rows = []
            for camp in scale_up_campaigns[:5]:
                scale_up_rows.append({
                    "optimizer": camp.get("optimizer", "未知"),
                    "campaign": camp.get("campaign_name", ""),
                    "drama": camp.get("drama_name", "-"),
                    "country": camp.get("country", "-"),
                    "spend": f"${camp.get('spend', 0):,.0f}",
                    "roas": f"{camp.get('roas', 0):.0%}",
                    "action": "大幅提预算"
                })
            elements.append({
                "tag": "table",
                "columns": [
                    {"name": "optimizer", "display_name": "投手"},
                    {"name": "campaign", "display_name": "计划"},
                    {"name": "drama", "display_name": "剧集"},
                    {"name": "country", "display_name": "国家"},
                    {"name": "spend", "display_name": "消耗"},
                    {"name": "roas", "display_name": "ROAS"},
                    {"name": "action", "display_name": "建议"}
                ],
                "rows": scale_up_rows
            })
        else:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "🟢 扩量机会：暂无"}})

        # 地区观察 (边际 ROAS > 50% 的国家，使用表格组件)
        high_roas_countries = [c for c in country_marginal_roas if c.get("roas", 0) > 0.50]
        if high_roas_countries:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🌍 地区观察：**"}})
            country_rows = []
            for country in high_roas_countries[:5]:
                country_rows.append({
                    "country": country.get("country", ""),
                    "spend": f"${country.get('spend', 0):,.0f}",
                    "roas": f"{country.get('roas', 0):.0%}",
                    "action": "关注是否加投"
                })
            elements.append({
                "tag": "table",
                "columns": [
                    {"name": "country", "display_name": "国家"},
                    {"name": "spend", "display_name": "消耗"},
                    {"name": "roas", "display_name": "ROAS"},
                    {"name": "action", "display_name": "建议"}
                ],
                "rows": country_rows
            })

        # 根据整体 ROAS 选择卡片颜色
        roas_green = self.config.get("roas_green_threshold", 0.40)
        roas_yellow = self.config.get("roas_yellow_threshold", 0.30)

        if d0_roas >= roas_green:
            color = "green"
        elif d0_roas >= roas_yellow:
            color = "yellow"
        else:
            color = "red"

        # 如果有止损预警，强制红色
        if stop_loss_campaigns:
            color = "red"

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"⏰ 实时战报 [{current_hour}]"},
                    "template": color
                },
                "elements": elements
            }
        }
        return self._send(payload)

    # ============ 异常处理报警 ============
    def send_data_missing_alert(self, date: str = None) -> dict:
        """
        发送数据源缺失报警

        若 9:00 未读取到昨日数据，机器人报警至管理群

        Args:
            date: 缺失数据的日期
        """
        if date is None:
            from datetime import datetime, timedelta
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        return self.send_alert(
            alert_type="数据源缺失",
            message=f"数据源未更新，日报生成失败，请检查 BI 同步。缺失日期: {date}",
            level="warning"
        )

    def send_zero_spend_alert(self) -> dict:
        """
        发送消耗为0报警

        若 Total Spend = 0，报警提示检查 API Token
        """
        return self.send_alert(
            alert_type="数据异常",
            message="读取到消耗为0，请检查 API Token 是否过期。",
            level="error"
        )

    def validate_daily_data(self, data: Dict[str, Any]) -> tuple:
        """
        验证日报数据，检查异常情况

        Args:
            data: 日报数据

        Returns:
            (is_valid, error_type, error_message)
            - is_valid: 数据是否有效
            - error_type: 错误类型 ('missing', 'zero_spend', None)
            - error_message: 错误信息
        """
        if not data:
            return False, 'missing', '数据为空'

        summary = data.get("summary", {})
        total_spend = safe_get_number(summary, "total_spend", 0)

        # 检查消耗是否为0
        if total_spend == 0:
            return False, 'zero_spend', '总消耗为0'

        return True, None, None


# ============ 定时任务函数 ============
def Daily_Job(webhook_url: str, secret: str = None, data: Dict[str, Any] = None,
              bi_link: str = None, config: Dict[str, Any] = None) -> dict:
    """
    日报定时任务 - 每日 9:00 执行

    为管理层提供昨天的全盘复盘，包含：
    - 大盘核心总结
    - 策略建议
    - 投手排行榜
    - 数据明细

    Args:
        webhook_url: 飞书机器人 Webhook 地址
        secret: 签名密钥（可选）
        data: 日报数据，包含:
            - date: 日期
            - summary: {total_spend, total_revenue, global_roas}
            - summary_prev: {total_spend, total_revenue, global_roas}
            - optimizers: [{name, spend, roas, campaign_count, top_campaign}]
            - dramas_top5: [{name, spend, roas}]
            - countries_top5: [{name, spend, roas}]
            - scale_up_dramas: [{name, spend, roas}]
            - opportunity_markets: [{drama_name, country, spend, roas}]
        bi_link: BI 报表链接
        config: 配置参数（可选）

    Returns:
        发送结果
    """
    bot = LarkBot(webhook_url=webhook_url, secret=secret, config=config)

    # 验证数据
    is_valid, error_type, error_message = bot.validate_daily_data(data)

    if not is_valid:
        if error_type == 'missing':
            # 数据源缺失报警
            return bot.send_data_missing_alert()
        elif error_type == 'zero_spend':
            # 消耗为0报警
            return bot.send_zero_spend_alert()

    # 发送日报
    return bot.send_daily_report(data, bi_link=bi_link)


def Hourly_Job(webhook_url: str, secret: str = None, data: Dict[str, Any] = None,
               config: Dict[str, Any] = None) -> List[dict]:
    """
    小时级定时任务 - 每小时执行

    实时监控投放效果，发送止损预警和扩量建议

    Args:
        webhook_url: 飞书机器人 Webhook 地址
        secret: 签名密钥（可选）
        data: 监控数据，包含:
            - stop_loss_alerts: 止损预警列表
            - scale_up_suggestions: 扩量建议列表
        config: 配置参数（可选）

    Returns:
        发送结果列表
    """
    bot = LarkBot(webhook_url=webhook_url, secret=secret, config=config)
    results = []

    if not data:
        return results

    # 处理止损预警
    stop_loss_alerts = data.get("stop_loss_alerts", [])
    for alert in stop_loss_alerts:
        result = bot.send_stop_loss_alert(alert)
        results.append(result)

    # 处理扩量建议
    scale_up_suggestions = data.get("scale_up_suggestions", [])
    for suggestion in scale_up_suggestions:
        result = bot.send_scale_up_suggestion(suggestion)
        results.append(result)

    return results


# ============ 使用示例 ============

if __name__ == "__main__":
    # 1. 创建机器人实例（需要替换为实际的 webhook 地址）
    bot = LarkBot(
        webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxx",
        secret="your_secret_key"  # 可选
    )

    # 2. 发送简单文本消息
    # bot.send_text("这是一条测试消息")

    # 3. 发送文本消息并@所有人
    # bot.send_text("重要通知：系统将于今晚维护", at_all=True)

    # 4. 发送文本消息并@指定人
    # bot.send_text("请查看数据报告", at_user_ids=["ou_xxx", "ou_yyy"])

    # 5. 发送市场监控报告
    market_data = {
        "date": "2025-01-15",
        "impressions": 1500000,
        "clicks": 45000,
        "cost": 12500.50,
        "ctr": 0.03,
        "cpc": 0.28,
        "budget": 15000
    }
    # bot.send_market_report(market_data, at_user_ids=["ou_xxx"])

    # 6. 发送投放效果报告
    ad_data = {
        "period": "2025-01-08 ~ 2025-01-15",
        "channel": "抖音/快手/腾讯广告",
        "conversions": 3200,
        "cpa": 15.5,
        "roi": 1.85,
        "change": "↑ 12.5%"
    }
    # bot.send_ad_performance_report(ad_data)

    # 7. 发送告警
    # bot.send_alert(
    #     alert_type="预算告警",
    #     message="腾讯广告渠道消耗已达预算 90%",
    #     level="warning",
    #     at_user_ids=["ou_xxx"]
    # )

    print("Lark Bot 模块已就绪，请配置 webhook 地址后使用")
