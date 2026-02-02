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
import os
import sys
from typing import Optional, List, Dict, Any

# 导入日志模块
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
try:
    from utils.logger import get_logger
except ImportError:
    # 降级：使用简单的 print 包装
    class SimpleLogger:
        def info(self, msg): print(f"[INFO] {msg}")
        def warning(self, msg): print(f"[WARNING] {msg}")
        def error(self, msg): print(f"[ERROR] {msg}")
        def debug(self, msg): pass
    def get_logger(name): return SimpleLogger()

# 初始化 logger
logger = get_logger("dataget.lark")

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

# 尝试导入 ChatGPT Advisor
try:
    from .chatgpt_advisor import ChatGPTAdvisor, create_chatgpt_advisor
    CHATGPT_AVAILABLE = True
except ImportError:
    try:
        from chatgpt_advisor import ChatGPTAdvisor, create_chatgpt_advisor
        CHATGPT_AVAILABLE = True
    except ImportError:
        CHATGPT_AVAILABLE = False
        ChatGPTAdvisor = None
        create_chatgpt_advisor = None


# 优化师 -> 飞书 open_id 映射表 (需要配置)
OPTIMIZER_USER_MAP: Dict[str, str] = {
    # "张三": "ou_xxxxxxxxxxxx",
    # "李四": "ou_yyyyyyyyyyyy",
}

# 团队分组配置
CN_TEAM = ["hannibal", "kino", "zane", "silas", "kimi", "echo", "felix"]
KR_TEAM = ["lyla", "juria", "jade"]

def get_optimizer_team(optimizer_name: str) -> str:
    """获取投手所属团队"""
    name_lower = optimizer_name.lower() if optimizer_name else ""
    if name_lower in [n.lower() for n in CN_TEAM]:
        return "CN"
    elif name_lower in [n.lower() for n in KR_TEAM]:
        return "KR"
    return "Other"

# ============ 默认配置 ============
DEFAULT_CONFIG = {
    "roas_green_threshold": 0.40,    # ROAS >= 40%: 绿色 (🌟 S级)
    "roas_yellow_threshold": 0.30,   # 30% <= ROAS < 40%: 黄色 (⚠️ 效率下滑)
    # ROAS < 30%: 红色 (🚨 需关注)
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
                 gemini_api_key: str = None, chatgpt_api_key: str = None):
        """
        初始化飞书机器人

        Args:
            webhook_url: 机器人 Webhook 地址
            secret: 签名密钥（可选，用于安全验证）
            config: 配置参数（可选），包含:
                - roas_green_threshold: ROAS 绿色阈值 (默认 0.40)
                - roas_yellow_threshold: ROAS 黄色阈值 (默认 0.30)
            gemini_api_key: Gemini API Key（可选，用于 AI 策略建议）
            chatgpt_api_key: ChatGPT API Key（可选，用于 AI 智能分析）
        """
        self.webhook_url = webhook_url
        self.secret = secret
        # 合并默认配置和用户配置
        self.config = {**DEFAULT_CONFIG, **(config or {})}

        import os

        # 初始化 Gemini Advisor
        self.gemini_advisor = None
        if GEMINI_AVAILABLE:
            try:
                api_key = gemini_api_key or (config and config.get("gemini_api_key")) or os.getenv("GEMINI_API_KEY")
                if api_key:
                    self.gemini_advisor = create_advisor(api_key)
            except Exception:
                pass

        # 初始化 ChatGPT Advisor
        self.chatgpt_advisor = None
        if CHATGPT_AVAILABLE:
            try:
                api_key = chatgpt_api_key or (config and config.get("chatgpt_api_key")) or os.getenv("OPENAI_API_KEY") or os.getenv("GEMINI_API_KEY")
                if api_key:
                    self.chatgpt_advisor = create_chatgpt_advisor(api_key)
            except Exception:
                pass

    def _format_at_optimizer(self, optimizer_name: str) -> str:
        """
        格式化 @投手 文本

        如果投手在 OPTIMIZER_USER_MAP 中有配置，返回飞书 @格式
        否则返回普通文本
        """
        if optimizer_name in OPTIMIZER_USER_MAP:
            user_id = OPTIMIZER_USER_MAP[optimizer_name]
            return f"<at id={user_id}></at>"
        return f"**{optimizer_name}**"

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
        if metrics.get("media_roas"):
            content.append({"label": "📈 Media ROAS", "value": f"{metrics['media_roas']:.1%}"})

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
        channel_benchmark = data.get("channel_benchmark", {})

        # 计算环比变化
        total_spend = summary.get("total_spend", 0)
        prev_spend = summary_prev.get("total_spend", 0)
        global_roas = summary.get("global_roas", 0)
        prev_roas = summary_prev.get("global_roas", 0)

        spend_change = (total_spend - prev_spend) / prev_spend if prev_spend > 0 else 0
        roas_change = global_roas - prev_roas  # 绝对变化（百分点）

        # 平台总营收环比
        platform_revenue = summary.get("platform_total_revenue", 0)
        prev_platform_revenue = summary_prev.get("platform_total_revenue", 0)
        platform_revenue_change = (platform_revenue - prev_platform_revenue) / prev_platform_revenue if prev_platform_revenue > 0 else 0

        # 收支比环比
        revenue_spend_ratio = summary.get("revenue_spend_ratio", 0)
        prev_ratio = summary_prev.get("revenue_spend_ratio", 0)
        ratio_change = revenue_spend_ratio - prev_ratio  # 绝对变化

        # 环比 emoji
        spend_emoji = "📈" if spend_change >= 0 else "📉"
        roas_emoji = "📈" if roas_change >= 0 else "📉"
        revenue_emoji = "📈" if platform_revenue_change >= 0 else "📉"
        ratio_emoji = "📈" if ratio_change >= 0 else "📉"

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

        # 平台总收入 + 日环比
        if platform_revenue > 0:
            revenue_change_str = f" ({revenue_emoji} {platform_revenue_change:+.0%})" if prev_platform_revenue > 0 else ""
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 平台总收入：**${platform_revenue:,.2f}**{revenue_change_str}"}})

        # 收支比 + 日环比
        if revenue_spend_ratio > 0:
            ratio_change_str = f" ({ratio_emoji} {ratio_change:+.1%})" if prev_ratio > 0 else ""
            ratio_status = "✅" if revenue_spend_ratio >= 1 else "⚠️"
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 收支比：**{revenue_spend_ratio:.1%}** {ratio_status}{ratio_change_str}"}})

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 综合 ROAS：**{global_roas:.1%}** ({roas_emoji} {roas_change:+.1%})"}})

        # 分渠道数据 (TikTok / Meta)
        tiktok_data = channel_benchmark.get("tiktok", {}) or channel_benchmark.get("TikTok", {})
        meta_data = channel_benchmark.get("facebook", {}) or channel_benchmark.get("meta", {}) or channel_benchmark.get("Meta", {})
        if tiktok_data or meta_data:
            channel_parts = []
            if tiktok_data.get("spend", 0) > 0:
                tk_spend = tiktok_data.get("spend", 0)
                tk_revenue = tiktok_data.get("revenue", 0)
                tk_roas = tiktok_data.get("roas", 0)
                channel_parts.append(f"TikTok: ${tk_spend:,.0f}/${tk_revenue:,.0f}({tk_roas:.0%})")
            if meta_data.get("spend", 0) > 0:
                meta_spend = meta_data.get("spend", 0)
                meta_revenue = meta_data.get("revenue", 0)
                meta_roas = meta_data.get("roas", 0)
                channel_parts.append(f"Meta: ${meta_spend:,.0f}/${meta_revenue:,.0f}({meta_roas:.0%})")
            if channel_parts:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 分渠道：{' | '.join(channel_parts)}"}})

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 核心评价：{evaluation}"}})
        elements.append({"tag": "hr"})

        # ========== 板块 2: 策略建议 (AI Insight) ==========
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**💡 策略建议**"}})

        # 使用 Gemini 生成智能策略建议
        strategy_insights = self._generate_strategy_insights(data)

        # 放量剧目 [Gemini]
        scale_up_text = strategy_insights.get("scale_up_drama", "暂无符合条件的剧目")
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🚀 建议放量剧目：{scale_up_text} [Gemini]"}})

        # 机会市场 [Gemini]
        opportunity_text = strategy_insights.get("opportunity_market", "暂无新兴市场机会")
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🌍 机会市场：{opportunity_text} [Gemini]"}})

        # 测剧建议 [Gemini]
        test_drama_text = strategy_insights.get("test_drama_suggestion", "")
        if test_drama_text:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• ⚠️ 测剧建议：{test_drama_text} [Gemini]"}})

        # ========== 板块 2.5: ChatGPT 智能分析 ==========
        if self.chatgpt_advisor:
            try:
                ai_analysis = self.chatgpt_advisor.analyze_daily_data(data)

                # 核心洞察 [GPT]
                key_insights = ai_analysis.get("key_insights", "")
                if key_insights:
                    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🤖 AI洞察：{key_insights} [GPT]"}})

                # 异常点 [GPT]
                anomalies = ai_analysis.get("anomalies", [])
                for anomaly in anomalies[:2]:
                    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• ⚠️ {anomaly} [GPT]"}})

                # 机会 [GPT]
                opportunities = ai_analysis.get("opportunities", [])
                for opp in opportunities[:2]:
                    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 💎 {opp} [GPT]"}})
            except Exception as e:
                pass  # ChatGPT 分析失败，静默跳过

        elements.append({"tag": "hr"})

        # ========== 板块 2.8: 头部剧集综合榜 ==========
        top_dramas_detail = data.get("top_dramas_detail", [])
        if top_dramas_detail:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🎬 昨日头部剧集表现**"}})

            for i, drama in enumerate(top_dramas_detail):
                name = drama.get("name", "未知")
                spend = drama.get("spend", 0)
                roas = drama.get("roas", 0)

                # ROAS 状态标记
                roas_mark = "📉" if roas < roas_yellow else ""

                # 格式化消耗
                if spend >= 10000:
                    spend_str = f"${spend/1000:.1f}k"
                else:
                    spend_str = f"${spend:,.0f}"

                # 获取语区和国家
                top_langs = drama.get("top_languages", [])
                top_countries = drama.get("top_countries", [])

                lang_str = ", ".join([f"{l['language']}" for l in top_langs[:2]]) if top_langs else "-"
                country_str = ", ".join([c['country'] for c in top_countries[:3]]) if top_countries else "-"

                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**{i+1}. 《{name}》**"}})
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"   消耗: {spend_str} | ROAS: {roas:.1%} {roas_mark}"}})
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"   语区: {lang_str} | 国家: {country_str}"}})

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
            "page_size": 20,
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
        media_roas = summary.get("media_roas", 0)
        stop_loss = data.get("stop_loss_campaigns", [])
        scale_up = data.get("scale_up_campaigns", [])

        result = {
            "overall_assessment": "",
            "stop_loss_advice": "当前无需止损",
            "scale_up_advice": "当前无明显扩量机会"
        }

        # 整体态势
        if media_roas >= 0.40:
            result["overall_assessment"] = "大盘健康，继续保持当前节奏"
        elif media_roas >= 0.30:
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
                - media_roas: Media ROAS
                - roas_target: ROAS 目标
                - optimizers: 投手数据列表 [{name, spend, roas, new_campaigns, comment}]
                - warnings: 警示区数据 [{name, spend, roas, suggestion}]
            bi_link: BI 报表链接
        """
        date = data.get("date", time.strftime("%Y-%m-%d"))
        total_spend = data.get("total_spend", 0)
        spend_change = data.get("spend_change", 0)
        media_roas = data.get("media_roas", 0)
        roas_target = data.get("roas_target", 0.4)
        optimizers = data.get("optimizers", [])
        warnings = data.get("warnings", [])

        # 环比变化显示
        change_emoji = "🔴" if spend_change < 0 else "🟢"
        change_text = f"{change_emoji} 环比 {spend_change:+.0%}"

        # ROAS 状态
        roas_status = "🟢 达标" if media_roas >= roas_target else "🔴 未达标"

        elements = [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**🌍 大盘总览:**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• 总消耗: **${total_spend:,.0f}** ({change_text})"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• Media ROAS: **{media_roas:.0%}** ({roas_status})"}},
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
                - media_roas: Media ROAS
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
        media_roas = data.get("media_roas", 0)
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
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• Media ROAS: **{media_roas:.2%}** (极低)"}},
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
                - media_roas: Media ROAS
                - ctr: CTR
                - competitor_insight: 竞品情报
                - suggestions: 建议操作列表
            media_link: 媒体后台链接
        """
        drama_name = data.get("drama_name", "未知剧集")
        campaign_id = data.get("campaign_id", "")
        spend = data.get("spend", 0)
        media_roas = data.get("media_roas", 0)
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
            {"tag": "div", "text": {"tag": "lark_md", "content": f"• Media ROAS: **{media_roas:.0%}** (优异)"}},
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
        """发送消息到飞书（带重试）"""
        # 添加签名
        if self.secret:
            timestamp = str(int(time.time()))
            payload["timestamp"] = timestamp
            payload["sign"] = self._gen_sign(timestamp)

        headers = {"Content-Type": "application/json"}

        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    self.webhook_url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=10
                )
                result = response.json()

                # 检查飞书返回的错误码
                if result.get("code") == 0 or result.get("StatusCode") == 0:
                    logger.debug(f"飞书消息发送成功")
                    return result

                # 如果是限流错误，等待后重试
                if result.get("code") in [9499, 99991400]:
                    if attempt < max_retries - 1:
                        delay = 2 ** attempt
                        logger.warning(f"飞书限流 (尝试 {attempt + 1}/{max_retries}), {delay}s 后重试...")
                        time.sleep(delay)
                        continue

                logger.warning(f"飞书返回错误: {result}")
                return result

            except requests.exceptions.Timeout as e:
                last_error = e
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning(f"飞书请求超时 (尝试 {attempt + 1}/{max_retries}): {e}, {delay}s 后重试...")
                    time.sleep(delay)
            except requests.exceptions.ConnectionError as e:
                last_error = e
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning(f"飞书连接失败 (尝试 {attempt + 1}/{max_retries}): {e}, {delay}s 后重试...")
                    time.sleep(delay)
            except Exception as e:
                logger.error(f"飞书发送异常: {e}")
                return {"code": -1, "msg": str(e)}

        logger.error(f"飞书消息发送失败，已重试 {max_retries} 次: {last_error}")
        return {"code": -1, "msg": str(last_error)}

    # ============ 模版 7: 周报播报 (Weekly Report) ============
    def send_weekly_report(self, data: Dict[str, Any]) -> dict:
        """
        发送周报播报 (Weekly Report) - 为管理层提供周度趋势分析

        触发时间：每周一 09:30
        数据范围：上周一至周日 (W-1) 完整 7 天数据

        Args:
            data: 周报数据，包含:
                - week_start: 周开始日期
                - week_end: 周结束日期
                - summary: 本周大盘汇总
                - prev_week_summary: 上周大盘汇总 (环比)
                - daily_stats: 日趋势数据
                - optimizer_weekly: 投手周度数据
                - top_dramas: 头部剧集
                - potential_dramas: 潜力剧集
                - declining_dramas: 衰退剧集
                - top_countries: 主力市场
                - emerging_markets: 新兴机会
        """
        week_start = data.get("week_start", "")
        week_end = data.get("week_end", "")
        summary = data.get("summary", {})
        prev_summary = data.get("prev_week_summary", {})
        daily_stats = data.get("daily_stats", [])
        optimizer_weekly = data.get("optimizer_weekly", [])
        top_dramas = data.get("top_dramas", [])
        potential_dramas = data.get("potential_dramas", [])
        declining_dramas = data.get("declining_dramas", [])
        losing_dramas = data.get("losing_dramas", [])  # 新增：尾部亏损剧集
        top_countries = data.get("top_countries", [])
        emerging_markets = data.get("emerging_markets", [])
        editor_stats = data.get("editor_stats", [])  # 剪辑师统计数据

        # 计算环比
        week_spend = summary.get("week_total_spend", 0)
        prev_spend = prev_summary.get("week_total_spend", 0)
        week_revenue = summary.get("week_total_revenue", 0)
        prev_revenue = prev_summary.get("week_total_revenue", 0)
        week_roas = summary.get("week_avg_roas", 0)
        prev_roas = prev_summary.get("week_avg_roas", 0)
        week_cpm = summary.get("week_avg_cpm", 0)
        prev_cpm = prev_summary.get("week_avg_cpm", 0)

        spend_change = (week_spend - prev_spend) / prev_spend if prev_spend > 0 else 0
        revenue_change = (week_revenue - prev_revenue) / prev_revenue if prev_revenue > 0 else 0
        roas_change = week_roas - prev_roas
        cpm_change = (week_cpm - prev_cpm) / prev_cpm if prev_cpm > 0 else 0

        # 收支比计算
        week_ratio = summary.get("revenue_spend_ratio", 0)
        if week_ratio == 0 and week_spend > 0:
            week_ratio = week_revenue / week_spend
        prev_ratio = prev_summary.get("revenue_spend_ratio", 0)
        if prev_ratio == 0 and prev_spend > 0:
            prev_ratio = prev_revenue / prev_spend
        ratio_change = week_ratio - prev_ratio

        # 环比 emoji
        spend_emoji = "📈" if spend_change >= 0 else "📉"
        revenue_emoji = "📈" if revenue_change >= 0 else "📉"
        roas_emoji = "📈" if roas_change >= 0 else "📉"
        ratio_emoji = "📈" if ratio_change >= 0 else "📉"

        # 格式化日期显示
        start_display = f"{week_start[5:7]}.{week_start[8:10]}" if week_start else ""
        end_display = f"{week_end[5:7]}.{week_end[8:10]}" if week_end else ""

        # 生成核心评价
        evaluation = self._generate_weekly_evaluation(week_spend, week_roas, spend_change, roas_change)

        elements = []

        # ========== 板块 1: 周度大盘综述 ==========
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**📅 [{start_display}-{end_display}] 周度大盘综述**"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 周总消耗：**${week_spend:,.0f}** ({spend_emoji} {spend_change:+.0%})"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 周总营收：**${week_revenue:,.0f}** ({revenue_emoji} {revenue_change:+.0%})"}})
        # 收支比
        if week_ratio > 0:
            ratio_change_str = f" ({ratio_emoji} {ratio_change:+.1%})" if prev_ratio > 0 else ""
            ratio_status = "✅" if week_ratio >= 1 else "⚠️"
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 收支比：**{week_ratio:.1%}** {ratio_status}{ratio_change_str}"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 周均 ROAS：**{week_roas:.1%}** ({roas_emoji} {roas_change:+.1%})"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 日均消耗：**${summary.get('daily_avg_spend', 0):,.0f}**"}})
        # CPM 变化
        if week_cpm > 0:
            cpm_emoji = "📈" if cpm_change >= 0 else "📉"
            cpm_warning = " ⚠️" if cpm_change > 0.05 else ""  # CPM 上涨超过 5% 警告
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 平均 CPM：**${week_cpm:.2f}** ({cpm_emoji} {cpm_change:+.0%}){cpm_warning}"}})
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 核心评价：{evaluation}"}})
        elements.append({"tag": "hr"})

        # ========== 板块 1.5: 团队分组汇总 ==========
        self._add_weekly_team_summary(elements, optimizer_weekly)

        # ========== 板块 2: 日趋势分析 ==========
        if daily_stats:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**📈 日趋势**"}})
            daily_rows = []
            prev_roas_val = 0
            for day in daily_stats:
                date_str = day.get("date", "")
                if len(date_str) >= 10:
                    date_display = f"{date_str[5:7]}/{date_str[8:10]}"
                else:
                    date_display = date_str
                spend = day.get("spend", 0)
                roas = day.get("roas", 0)
                trend = "📈" if roas > prev_roas_val else "📉" if roas < prev_roas_val else "➡️"
                prev_roas_val = roas
                daily_rows.append({
                    "date": date_display,
                    "spend": f"${spend:,.0f}",
                    "roas": f"{roas:.1%}",
                    "trend": trend
                })
            elements.append({
                "tag": "table",
                "page_size": 7,
                "columns": [
                    {"name": "date", "display_name": "日期", "width": "auto"},
                    {"name": "spend", "display_name": "消耗", "width": "auto"},
                    {"name": "roas", "display_name": "ROAS", "width": "auto"},
                    {"name": "trend", "display_name": "趋势", "width": "auto"}
                ],
                "rows": daily_rows
            })
            elements.append({"tag": "hr"})

        # ========== 板块 3: 投手周度排行 ==========
        self._add_weekly_optimizer_section(elements, optimizer_weekly)

        # ========== 板块 4: 剪辑师产出与质量 ==========
        self._add_weekly_editor_section(elements, editor_stats)

        # ========== 板块 5: 剧集周度表现 ==========
        self._add_weekly_drama_section(elements, top_dramas, potential_dramas, declining_dramas, losing_dramas)

        # ========== 板块 6: 市场分析 ==========
        self._add_weekly_market_section(elements, top_countries, emerging_markets)

        # ========== 板块 7: AI 周度洞察 ==========
        self._add_weekly_ai_insights(elements, data)

        # ========== 板块 8: 总结与规划 ==========
        self._add_weekly_summary_plan(elements, losing_dramas, potential_dramas, emerging_markets)

        # 构建卡片
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": f"📊 周报 [{start_display}-{end_display}]"},
                "template": "blue"
            },
            "elements": elements
        }

        payload = {
            "msg_type": "interactive",
            "card": card
        }
        return self._send(payload)

    def _generate_weekly_evaluation(self, spend: float, roas: float, spend_change: float, roas_change: float) -> str:
        """生成周报核心评价"""
        parts = []

        # 消耗评价
        if spend_change > 0.1:
            parts.append("消耗大幅增长")
        elif spend_change > 0:
            parts.append("消耗稳步增长")
        elif spend_change > -0.1:
            parts.append("消耗小幅回落")
        else:
            parts.append("消耗明显下降")

        # ROAS 评价
        if roas >= 0.45:
            parts.append("效率优秀")
        elif roas >= 0.40:
            parts.append("效率良好")
        elif roas >= 0.30:
            parts.append("效率一般")
        else:
            parts.append("效率需关注")

        # 趋势评价
        if roas_change > 0.05:
            parts.append("ROAS 显著提升")
        elif roas_change > 0:
            parts.append("ROAS 小幅提升")
        elif roas_change > -0.05:
            parts.append("ROAS 小幅回落")
        else:
            parts.append("ROAS 明显下滑")

        return "，".join(parts)

    def _add_weekly_team_summary(self, elements: list, optimizer_weekly: list):
        """添加团队分组汇总板块"""
        if not optimizer_weekly:
            return

        # 按团队分组统计
        cn_spend, cn_revenue, kr_spend, kr_revenue = 0, 0, 0, 0
        cn_count, kr_count = 0, 0
        cn_campaigns, kr_campaigns = 0, 0

        for opt in optimizer_weekly:
            name = opt.get("name", "")
            spend = opt.get("spend", 0)
            revenue = opt.get("revenue", 0)
            campaigns = opt.get("campaign_count", 0)
            team = get_optimizer_team(name)

            if team == "CN":
                cn_spend += spend
                cn_revenue += revenue
                cn_count += 1
                cn_campaigns += campaigns
            elif team == "KR":
                kr_spend += spend
                kr_revenue += revenue
                kr_count += 1
                kr_campaigns += campaigns

        cn_roas = cn_revenue / cn_spend if cn_spend > 0 else 0
        kr_roas = kr_revenue / kr_spend if kr_spend > 0 else 0
        total_spend = cn_spend + kr_spend

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**⚔️ 团队作战表现**"}})

        # CN 团队
        if cn_count > 0:
            cn_ratio = cn_spend / total_spend if total_spend > 0 else 0
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**🇨🇳 CN 投放团队**"}})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 结果：消耗 **${cn_spend:,.0f}** (占比 {cn_ratio:.0%}) | ROAS **{cn_roas:.1%}**"}})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 计划数：{cn_campaigns} 个"}})

        # KR 团队
        if kr_count > 0:
            kr_ratio = kr_spend / total_spend if total_spend > 0 else 0
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**🇰🇷 KR 投放团队**"}})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 结果：消耗 **${kr_spend:,.0f}** (占比 {kr_ratio:.0%}) | ROAS **{kr_roas:.1%}**"}})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 计划数：{kr_campaigns} 个"}})

        elements.append({"tag": "hr"})

    def _add_weekly_optimizer_section(self, elements: list, optimizer_weekly: list):
        """添加投手周度排行板块 - 按团队分组"""
        if not optimizer_weekly:
            return

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🏆 投手周度排行**"}})

        roas_green = self.config.get("roas_green_threshold", 0.40)
        roas_yellow = self.config.get("roas_yellow_threshold", 0.30)

        # 按团队分组
        cn_optimizers = []
        kr_optimizers = []
        for opt in optimizer_weekly:
            team = get_optimizer_team(opt.get("name", ""))
            if team == "CN":
                cn_optimizers.append(opt)
            elif team == "KR":
                kr_optimizers.append(opt)

        # CN 团队
        if cn_optimizers:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "🇨🇳 **CN团队**"}})
            for i, opt in enumerate(cn_optimizers[:7]):
                self._add_optimizer_row(elements, i, opt, roas_green, roas_yellow)

        # KR 团队
        if kr_optimizers:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "🇰🇷 **KR团队**"}})
            for i, opt in enumerate(kr_optimizers[:5]):
                self._add_optimizer_row(elements, i, opt, roas_green, roas_yellow)

        elements.append({"tag": "hr"})

    def _add_optimizer_row(self, elements: list, index: int, opt: dict, roas_green: float, roas_yellow: float):
        """添加单个投手行"""
        name = opt.get("name", "未知")
        spend = opt.get("spend", 0)
        roas = opt.get("roas", 0)
        roas_change = opt.get("roas_change", 0)
        campaign_count = opt.get("campaign_count", 0)

        # 评级
        rating = ""
        if index == 0 and roas >= roas_green:
            rating = "🌟 MVP"
        elif roas_change > 0.05:
            rating = f"📈 +{roas_change:.1%}"
        elif roas < roas_yellow:
            rating = "🚨 需关注"
        elif roas_change != 0:
            rating = f"{roas_change:+.1%}"

        # 消耗格式化
        if spend >= 10000:
            spend_str = f"${spend/10000:.1f}w"
        else:
            spend_str = f"${spend:,.0f}"

        rating_str = f" ({rating})" if rating else ""
        campaign_str = f" | {campaign_count}计划" if campaign_count > 0 else ""
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"  {index+1}. **{name}**: 耗 {spend_str} | ROAS {roas:.1%}{campaign_str}{rating_str}"}})

    def _add_weekly_editor_section(self, elements: list, editor_stats: list):
        """添加剪辑师产出与质量板块"""
        if not editor_stats:
            return

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🎬 剪辑师产出与质量**"}})

        # 构建表格数据
        rows = []
        for editor in editor_stats[:10]:  # 最多显示 10 人
            name = editor.get("name", "未知")
            material_count = editor.get("material_count", 0)
            total_cost = editor.get("total_cost", 0)
            hot_count = editor.get("hot_count", 0)

            # 格式化消耗
            if total_cost >= 1000:
                cost_str = f"${total_cost/1000:.1f}k"
            else:
                cost_str = f"${total_cost:.0f}"

            rows.append({
                "editor": name,
                "count": str(material_count),
                "cost": cost_str,
                "hot": str(hot_count)
            })

        if rows:
            elements.append({
                "tag": "table",
                "page_size": 10,
                "columns": [
                    {"name": "editor", "display_name": "剪辑师", "width": "auto"},
                    {"name": "count", "display_name": "产出数", "width": "auto"},
                    {"name": "cost", "display_name": "消耗", "width": "auto"},
                    {"name": "hot", "display_name": "爆款数", "width": "auto"}
                ],
                "rows": rows
            })

        elements.append({"tag": "hr"})

    def _add_weekly_drama_section(self, elements: list, top_dramas: list, potential_dramas: list, declining_dramas: list, losing_dramas: list = None):
        """添加剧集周度表现板块"""
        if losing_dramas is None:
            losing_dramas = []
        if not top_dramas and not potential_dramas and not declining_dramas and not losing_dramas:
            return

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🎬 剧集周度表现**"}})

        # 头部剧集
        if top_dramas:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "【头部剧集】消耗 > $10k 且 ROAS > 40%"}})
            for i, drama in enumerate(top_dramas[:5]):
                name = drama.get("name", "未知")
                spend = drama.get("spend", 0)
                roas = drama.get("roas", 0)
                country_details = drama.get("country_details", [])

                if spend >= 10000:
                    spend_str = f"${spend/10000:.1f}w"
                else:
                    spend_str = f"${spend:,.0f}"

                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**{i+1}. 《{name}》**: 耗 {spend_str} | ROAS {roas:.1%}"}})

                # 展示各地区详细数据 (单行格式)
                if country_details:
                    region_parts = []
                    for cd in country_details[:5]:
                        c_country = cd.get("country", "-")
                        c_spend = cd.get("spend", 0)
                        c_roas = cd.get("roas", 0)
                        if c_spend >= 10000:
                            c_spend_str = f"${c_spend/10000:.1f}w"
                        elif c_spend >= 1000:
                            c_spend_str = f"${c_spend/1000:.1f}k"
                        else:
                            c_spend_str = f"${c_spend:.0f}"
                        region_parts.append(f"{c_country}({c_spend_str}/{c_roas:.0%})")
                    if region_parts:
                        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"   └ {' | '.join(region_parts)}"}})

        # 潜力剧集
        if potential_dramas:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "【潜力剧集】消耗 $1k-$10k 且 ROAS > 50%"}})
            for i, drama in enumerate(potential_dramas[:3]):
                name = drama.get("name", "未知")
                spend = drama.get("spend", 0)
                roas = drama.get("roas", 0)
                spend_str = f"${spend:,.0f}"
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"{i+1}. 《{name}》: 耗 {spend_str} | ROAS {roas:.1%} ⭐ 建议放量"}})

        # 衰退预警
        if declining_dramas:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "【衰退预警】ROAS 周环比下降 > 10%"}})
            for i, drama in enumerate(declining_dramas[:3]):
                name = drama.get("name", "未知")
                roas = drama.get("roas", 0)
                roas_change = drama.get("roas_change", 0)
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"{i+1}. 《{name}》: ROAS {roas:.1%} (📉 {roas_change:+.1%})"}})

        # 尾部亏损剧集
        if losing_dramas:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "【💀 尾部亏损】消耗 > $1k 且 ROAS < 25%"}})
            for i, drama in enumerate(losing_dramas[:5]):
                name = drama.get("name", "未知")
                spend = drama.get("spend", 0)
                roas = drama.get("roas", 0)
                spend_str = f"${spend/1000:.1f}k" if spend >= 1000 else f"${spend:.0f}"
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"{i+1}. 《{name}》: 耗 {spend_str} | ROAS {roas:.1%} 🚨 建议止损"}})

        elements.append({"tag": "hr"})

    def _add_weekly_market_section(self, elements: list, top_countries: list, emerging_markets: list):
        """添加市场分析板块"""
        if not top_countries and not emerging_markets:
            return

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🌍 地区机会**"}})

        # 主力市场 - 单行格式避免表格限制
        if top_countries:
            for country in top_countries[:5]:
                name = country.get("name", "")
                spend = country.get("spend", 0)
                roas = country.get("roas", 0)
                roas_change = country.get("roas_change", 0)

                if spend >= 10000:
                    spend_str = f"${spend/10000:.1f}w"
                else:
                    spend_str = f"${spend:,.0f}"

                # 机会点标记
                status = ""
                if roas >= 0.50:
                    status = "🔥 机会点"
                elif roas >= 0.40:
                    status = "稳健"
                elif roas < 0.30:
                    status = "⚠️ 需关注"

                change_str = f" ({roas_change:+.1%})" if roas_change != 0 else ""
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🇺🇸 **{name}**: 耗 {spend_str} | ROAS {roas:.1%}{change_str} ({status})"}})

        # 新兴机会
        if emerging_markets:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "【🔥 新兴机会】非主投国家，ROAS > 50%"}})
            for market in emerging_markets[:3]:
                name = market.get("name", "")
                roas = market.get("roas", 0)
                spend = market.get("spend", 0)
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• {name}: ROAS {roas:.1%} (消耗 ${spend:,.0f}) 建议测试放量"}})

        elements.append({"tag": "hr"})

    def _add_weekly_ai_insights(self, elements: list, data: Dict[str, Any]):
        """添加 AI 周度洞察板块"""
        if not self.chatgpt_advisor:
            return

        try:
            ai_analysis = self.chatgpt_advisor.analyze_weekly_data(data)

            key_findings = ai_analysis.get("key_findings", "")
            risk_alerts = ai_analysis.get("risk_alerts", "")
            suggestions = ai_analysis.get("next_week_suggestions", "")

            if not key_findings and not risk_alerts and not suggestions:
                return

            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🤖 AI 周度洞察 [GPT]**"}})

            if key_findings:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 核心发现：{key_findings}"}})

            if risk_alerts:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 风险提示：{risk_alerts}"}})

            if suggestions:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 下周建议：{suggestions}"}})

            elements.append({"tag": "hr"})

        except Exception as e:
            logger.warning(f"AI 周度洞察生成失败: {e}")

    def _add_weekly_summary_plan(self, elements: list, losing_dramas: list, potential_dramas: list, emerging_markets: list):
        """添加总结与规划板块"""
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**📝 本周总结与下周规划**"}})

        # 下周重点 - 基于数据自动生成
        next_week_actions = []

        # 1. 扩量建议 - 潜力剧集
        if potential_dramas:
            top_potential = potential_dramas[0]
            next_week_actions.append(f"**扩量**：针对《{top_potential.get('name', '')}》进行放量测试 (当前 ROAS {top_potential.get('roas', 0):.0%})")

        # 2. 止损建议 - 亏损剧集
        if losing_dramas:
            top_losing = losing_dramas[0]
            next_week_actions.append(f"**止损**：关停《{top_losing.get('name', '')}》相关计划 (ROAS {top_losing.get('roas', 0):.0%})")

        # 3. 市场机会
        if emerging_markets:
            top_market = emerging_markets[0]
            next_week_actions.append(f"**拓展**：测试 {top_market.get('name', '')} 市场 (ROAS {top_market.get('roas', 0):.0%})")

        if next_week_actions:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "【下周重点】"}})
            for i, action in enumerate(next_week_actions[:3]):
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"{i+1}. {action}"}})

        elements.append({"tag": "hr"})

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
                - summary: {total_spend, total_revenue, media_roas}
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
        channel_benchmark = data.get("channel_benchmark", {})

        # 当前值
        total_spend = summary.get("total_spend", 0)
        total_revenue = summary.get("total_media_revenue", 0)  # 改用媒体归因收入
        media_roas = summary.get("media_roas", 0)
        platform_total_revenue = summary.get("platform_total_revenue", 0)  # 平台总营收
        revenue_spend_ratio = summary.get("revenue_spend_ratio", 0)  # 收支比

        # 前一日同时刻数据 (日环比)
        yesterday_summary = data.get("yesterday_summary", {})
        yesterday_spend = yesterday_summary.get("total_spend", 0)
        yesterday_revenue = yesterday_summary.get("total_media_revenue", 0)  # 改用媒体归因收入
        yesterday_media_roas = yesterday_summary.get("media_roas", 0)

        # 计算日环比
        daily_spend_change_pct = ((total_spend - yesterday_spend) / yesterday_spend * 100) if yesterday_spend > 0 else 0
        daily_revenue_change_pct = ((total_revenue - yesterday_revenue) / yesterday_revenue * 100) if yesterday_revenue > 0 else 0
        daily_media_roas_change = media_roas - yesterday_media_roas

        # 计算小时环比 - 使用 data 中的 prev_hour_summary
        prev_hour_summary = data.get("prev_hour_summary", {})
        hourly_spend_delta = 0
        roas_trend = 0
        prev_total_spend = 0
        prev_roas = 0

        if prev_hour_summary:
            prev_total_spend = prev_hour_summary.get("total_spend", 0)
            prev_roas = prev_hour_summary.get("media_roas", 0)
            hourly_spend_delta = total_spend - prev_total_spend
            roas_trend = media_roas - prev_roas

        # 如果 prev_data 参数也传了，优先使用（兼容旧调用）
        if prev_data and prev_data.get("total_spend", 0) > 0:
            prev_total_spend = prev_data.get("total_spend", 0)
            prev_roas = prev_data.get("media_roas", 0)
            hourly_spend_delta = total_spend - prev_total_spend
            roas_trend = media_roas - prev_roas

        # 环比百分比
        hourly_spend_change_pct = (hourly_spend_delta / prev_total_spend * 100) if prev_total_spend > 0 else 0

        elements = []

        # ========== 数据延迟警告 ==========
        if data_delayed:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "**⚠️ 数据延迟** - API 更新时间超过1小时，请关注数据时效性"}
            })
            elements.append({"tag": "hr"})

        # ========== 板块 1: 小时级异动监控 (Hourly Pulse) ==========
        # 大盘预警状态
        roas_baseline = self.config.get("roas_green_threshold", 0.40)
        if media_roas < roas_baseline:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**🔴 大盘预警：当前 ROAS {media_roas:.1%} (低于基线 {roas_baseline:.0%})**"}})
        else:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**🟢 大盘健康：当前 ROAS {media_roas:.1%}**"}})
        elements.append({"tag": "hr"})

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**⏰ 实时战报**"}})

        # 消耗 + 日环比
        spend_daily_str = ""
        if yesterday_spend > 0:
            spend_emoji = "📈" if daily_spend_change_pct > 0 else "📉"
            spend_daily_str = f" ({spend_emoji} 日环比 {daily_spend_change_pct:+.1f}%)"
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 截止当前总耗：**${total_spend:,.2f}**{spend_daily_str}"}})

        # 收入 + 日环比
        revenue_daily_str = ""
        if yesterday_revenue > 0:
            revenue_emoji = "📈" if daily_revenue_change_pct > 0 else "📉"
            revenue_daily_str = f" ({revenue_emoji} 日环比 {daily_revenue_change_pct:+.1f}%)"
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 截止当前收入：**${total_revenue:,.2f}**{revenue_daily_str}"}})

        # Media ROAS + 日环比 (使用 media_roas 作为主要 ROAS 指标)
        media_roas_daily_str = ""
        if yesterday_media_roas > 0:
            media_roas_emoji = "📈" if daily_media_roas_change > 0 else "📉"
            media_roas_daily_str = f" ({media_roas_emoji} 日环比 {daily_media_roas_change:+.1%})"
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 当前 Media ROAS：**{media_roas:.1%}**{media_roas_daily_str}"}})

        # 分渠道数据 (TikTok / Meta)
        tiktok_data = channel_benchmark.get("tiktok", {}) or channel_benchmark.get("TikTok", {})
        meta_data = channel_benchmark.get("facebook", {}) or channel_benchmark.get("meta", {}) or channel_benchmark.get("Meta", {})
        if tiktok_data or meta_data:
            channel_parts = []
            if tiktok_data.get("spend", 0) > 0:
                tk_spend = tiktok_data.get("spend", 0)
                tk_revenue = tiktok_data.get("revenue", 0)
                tk_roas = tiktok_data.get("roas", 0)
                channel_parts.append(f"TikTok: ${tk_spend:,.0f}/${tk_revenue:,.0f}({tk_roas:.0%})")
            if meta_data.get("spend", 0) > 0:
                meta_spend = meta_data.get("spend", 0)
                meta_revenue = meta_data.get("revenue", 0)
                meta_roas = meta_data.get("roas", 0)
                channel_parts.append(f"Meta: ${meta_spend:,.0f}/${meta_revenue:,.0f}({meta_roas:.0%})")
            if channel_parts:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 分渠道：{' | '.join(channel_parts)}"}})

        # 平台总营收 (含广告变现) [已删除 - 不再显示此字段]
        # if platform_total_revenue > 0:
        #     elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 平台总营收：**${platform_total_revenue:,.2f}** (含Ad变现)"}})

        # 收支比 [NEW] (暂时屏蔽)
        # if revenue_spend_ratio > 0:
        #     ratio_emoji = "✅" if revenue_spend_ratio >= 1 else "⚠️"
        #     elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 收支比：**{revenue_spend_ratio:.1%}** {ratio_emoji}"}})

        # 过去1小时新增消耗 (仅当有上小时数据且数据未延迟时显示)
        if prev_total_spend > 0 and not data_delayed:
            current_batch_time = data.get("batch_time", "")
            prev_batch_time = data.get("prev_batch_time", "")
            time_label = f"({current_batch_time} vs {prev_batch_time})" if current_batch_time and prev_batch_time else ""
            spend_emoji = "🔥" if hourly_spend_change_pct > 10 else "📊"
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"• 新增消耗 {time_label}：**${hourly_spend_delta:,.2f}** ({spend_emoji} {hourly_spend_change_pct:+.0f}%)"}
            })

            # ROAS 趋势（只有当上小时 ROAS 有效时才显示）
            if prev_roas > 0:
                roas_emoji = "↗️ 上升" if roas_trend > 0 else "↘️ 下滑" if roas_trend < 0 else "➡️ 持平"
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"• 过去1小时 ROAS 趋势：{roas_emoji} {abs(roas_trend):.1%}"}
                })
        elif not data_delayed:
            # 没有上小时数据（但数据未延迟）
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "• ⚠️ 上小时数据缺失，无法计算环比数据"}})

        elements.append({"tag": "hr"})

        # ========== 板块 1.5: 地区机会雷达 [NEW] ==========
        region_radar = data.get("region_opportunity_radar", [])
        if region_radar:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🌍 地区机会雷达**"}})
            for region in region_radar:
                country = region.get("country", "")
                roas = region.get("roas", 0)
                core_drama = region.get("core_drama", "")
                drama_ratio = region.get("drama_spend_ratio", 0)

                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"• **{country}**: ROAS 飙升至 **{roas:.0%}** 🔥"}
                })
                if core_drama and drama_ratio > 0.5:
                    elements.append({
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": f"  核心驱动: 《{core_drama}》(该剧在{country}消耗占比{drama_ratio:.0%})"}
                    })
                    elements.append({
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": f"  建议: 其他投手可尝试在{country}跟投《{core_drama}》"}
                    })
            elements.append({"tag": "hr"})

        # ========== 板块 2: 核心变化归因 (Change Attribution) ==========
        if prev_total_spend > 0:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🔍 谁在花钱？(过去1小时变化)**"}})
        else:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🔍 谁在花钱？(无上小时数据，显示累计)**"}})

        # 计算每个投手的小时消耗增量
        prev_optimizer_map = {}
        # 优先使用 prev_hour_summary 中的 optimizer_data
        prev_opt_data = prev_hour_summary.get("optimizer_data", []) if prev_hour_summary else []
        # 兼容旧的 prev_data 参数
        if not prev_opt_data and prev_data and prev_data.get("optimizer_data"):
            prev_opt_data = prev_data.get("optimizer_data", [])
        for opt in prev_opt_data:
            prev_optimizer_map[opt.get("optimizer")] = opt.get("spend", 0)

        optimizer_deltas = []
        for opt in optimizer_spend:
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
                "roas": opt.get("roas", 0),
                "top_campaigns": top_camp_info,
                "channel_spend": opt.get("channel_spend", {})
            })

        # 按增量排序
        optimizer_deltas.sort(key=lambda x: x["delta"], reverse=True)

        # 使用表格组件展示投手消耗
        optimizer_rows = []
        for opt in optimizer_deltas:
            delta = opt["delta"]
            status = "🔥" if delta > 100 else "🐢 缓慢" if delta < 50 else ""
            camp_str = ", ".join(opt['top_campaigns']) if opt['top_campaigns'] else "-"
            roas_val = opt.get("roas", 0)
            roas_str = f"{roas_val:.1%}" if roas_val else "-"

            # 分渠道消耗
            channel_spend = opt.get("channel_spend", {})
            tiktok_data = channel_spend.get("TikTok", {}) or channel_spend.get("tiktok", {})
            meta_data = channel_spend.get("Meta", {}) or channel_spend.get("meta", {}) or channel_spend.get("facebook", {})
            tiktok_spend = tiktok_data.get("spend", 0)
            tiktok_roas = tiktok_data.get("roas", 0)
            meta_spend = meta_data.get("spend", 0)
            meta_roas = meta_data.get("roas", 0)

            optimizer_rows.append({
                "optimizer": opt['name'],
                "delta": f"${delta:,.0f}",
                "total": f"${opt['total']:,.0f}",
                "tiktok": f"${tiktok_spend:,.0f}({tiktok_roas:.0%})" if tiktok_spend > 0 else "-",
                "meta": f"${meta_spend:,.0f}({meta_roas:.0%})" if meta_spend > 0 else "-",
                "media_roas": roas_str,
                "status": status
            })

        if optimizer_rows:
            elements.append({
                "tag": "table",
                "page_size": 20,
                "columns": [
                    {"name": "optimizer", "display_name": "投手"},
                    {"name": "delta", "display_name": "新增"},
                    {"name": "total", "display_name": "累计"},
                    {"name": "tiktok", "display_name": "TikTok"},
                    {"name": "meta", "display_name": "Meta"},
                    {"name": "media_roas", "display_name": "ROAS"},
                    {"name": "status", "display_name": "状态"}
                ],
                "rows": optimizer_rows
            })

        # ========== 过去1小时异动分析 ==========
        # 找出消耗激增但 ROAS 低的投手
        roas_warning_threshold = self.config.get("roas_yellow_threshold", 0.30)
        anomaly_optimizers = [
            opt for opt in optimizer_deltas
            if opt["delta"] > 200 and opt["roas"] < roas_warning_threshold
        ]
        if anomaly_optimizers and prev_total_spend > 0:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**⚠️ 过去1小时变化：**"}})
            for opt in anomaly_optimizers[:3]:
                at_text = self._format_at_optimizer(opt["name"])
                camp_str = ", ".join(opt['top_campaigns'][:1]) if opt['top_campaigns'] else ""
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"🔥 {at_text} 消耗激增 ${opt['delta']:,.0f}，但 ROAS 仅 {opt['roas']:.0%}"}
                })
                if camp_str:
                    elements.append({
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": f"   请重点检查计划：{camp_str}"}
                    })

        elements.append({"tag": "hr"})

        # ========== 板块 3: 实时策略建议 (Actionable Insights) ==========
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**⚡️ 操作建议**"}})

        # ChatGPT 智能分析（GPT-5.2）
        if self.chatgpt_advisor:
            try:
                chatgpt_analysis = self.chatgpt_advisor.analyze_realtime_data(data, prev_data)

                # 小时趋势
                hourly_trend = chatgpt_analysis.get("hourly_trend", "")
                if hourly_trend:
                    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🤖 GPT分析：{hourly_trend}"}})

                # 消耗节奏评估
                pace = chatgpt_analysis.get("pace_assessment", "")
                if pace:
                    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 📈 节奏评估：{pace} [GPT]"}})

                # 紧急操作
                urgent_actions = chatgpt_analysis.get("urgent_actions", [])
                for action in urgent_actions[:2]:
                    if action:
                        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🚨 {action} [GPT]"}})

                # 观察项
                watch_list = chatgpt_analysis.get("watch_list", [])
                for item in watch_list[:2]:
                    if item:
                        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 👀 {item} [GPT]"}})
            except Exception:
                pass  # ChatGPT 分析失败，静默跳过

        # Gemini AI 生成整体态势和具体建议
        ai_insights = self._generate_realtime_insights(data)
        if ai_insights:
            # 整体态势评估 [Gemini]
            overall = ai_insights.get("overall_assessment", "")
            if overall:
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 📊 整体态势：{overall} [Gemini]"}})

            # Gemini 止损建议
            stop_advice = ai_insights.get("stop_loss_advice", "")
            if stop_advice and stop_advice != "当前无需止损":
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🤖 AI止损建议：{stop_advice} [Gemini]"}})

            # Gemini 扩量建议
            scale_advice = ai_insights.get("scale_up_advice", "")
            if scale_advice and scale_advice != "当前无明显扩量机会":
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"• 🤖 AI扩量建议：{scale_advice} [Gemini]"}})

            elements.append({"tag": "hr"})

        # 止损预警 (使用表格组件)
        if stop_loss_campaigns:
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**🔴 止损预警：**"}})
            stop_loss_rows = []
            for camp in stop_loss_campaigns[:5]:
                stop_loss_rows.append({
                    "optimizer": camp.get("optimizer", "未知"),
                    "drama": camp.get("drama_name", "-"),
                    "channel": camp.get("channel", "-"),
                    "spend": f"${camp.get('spend', 0):,.0f}",
                    "roas": f"{camp.get('roas', 0):.0%}",
                    "action": "立即关停"
                })
            elements.append({
                "tag": "table",
                "columns": [
                    {"name": "optimizer", "display_name": "投手"},
                    {"name": "drama", "display_name": "剧集"},
                    {"name": "channel", "display_name": "渠道"},
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
                    "drama": camp.get("drama_name", "-"),
                    "channel": camp.get("channel", "-"),
                    "spend": f"${camp.get('spend', 0):,.0f}",
                    "roas": f"{camp.get('roas', 0):.0%}",
                    "action": "大幅提预算"
                })
            elements.append({
                "tag": "table",
                "columns": [
                    {"name": "optimizer", "display_name": "投手"},
                    {"name": "drama", "display_name": "剧集"},
                    {"name": "channel", "display_name": "渠道"},
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
                # 分渠道消耗
                channel_spend = country.get("channel_spend", {})
                tiktok_spend = channel_spend.get("TikTok", {}).get("spend", 0) or channel_spend.get("tiktok", {}).get("spend", 0)
                meta_spend = channel_spend.get("Meta", {}).get("spend", 0) or channel_spend.get("meta", {}).get("spend", 0) or channel_spend.get("facebook", {}).get("spend", 0)

                country_rows.append({
                    "country": country.get("country", ""),
                    "spend": f"${country.get('spend', 0):,.0f}",
                    "tiktok": f"${tiktok_spend:,.0f}" if tiktok_spend > 0 else "-",
                    "meta": f"${meta_spend:,.0f}" if meta_spend > 0 else "-",
                    "roas": f"{country.get('roas', 0):.0%}"
                })
            elements.append({
                "tag": "table",
                "columns": [
                    {"name": "country", "display_name": "国家"},
                    {"name": "spend", "display_name": "总消耗"},
                    {"name": "tiktok", "display_name": "TikTok"},
                    {"name": "meta", "display_name": "Meta"},
                    {"name": "roas", "display_name": "ROAS"}
                ],
                "rows": country_rows
            })

        # 根据整体 ROAS 选择卡片颜色
        roas_green = self.config.get("roas_green_threshold", 0.40)
        roas_yellow = self.config.get("roas_yellow_threshold", 0.30)

        if media_roas >= roas_green:
            color = "green"
        elif media_roas >= roas_yellow:
            color = "yellow"
        else:
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

    # ============ 模块三：个人专属助理 (Personal Assistant) ============

    def send_optimizer_hourly_pacing(self, data: Dict[str, Any]) -> dict:
        """
        发送投手个人实况窗 - 小时级流水账 (Hourly Pacing)

        展示最近 3 小时的核心指标，辅助判断跑量趋势和平台故障

        Args:
            data: 流水账数据，包含:
                - optimizer: 投手名称
                - hourly_data: [{hour, spend, revenue, roas, cpm}]
                - market_hourly_data: [{hour, roas, cpm}]
        """
        optimizer = data.get("optimizer", "未知")
        hourly_data = data.get("hourly_data", [])
        market_data = data.get("market_hourly_data", [])

        elements = []

        # 构建表格数据
        if hourly_data:
            # 合并个人数据和大盘数据
            table_rows = []
            for i, hour_item in enumerate(hourly_data):
                market_item = market_data[i] if i < len(market_data) else {}

                spend = hour_item.get("spend", 0)
                revenue = hour_item.get("revenue", 0)
                roas = hour_item.get("roas", 0)
                cpm = hour_item.get("cpm", 0)
                market_roas = market_item.get("roas", 0)
                market_cpm = market_item.get("cpm", 0)

                # ROAS 对比标记
                roas_mark = ""
                if roas > 0 and market_roas > 0:
                    if roas > market_roas * 1.1:
                        roas_mark = " 🟢"  # 跑赢大盘 10%+
                    elif roas < market_roas * 0.9:
                        roas_mark = " 🔴"  # 跑输大盘 10%+

                table_rows.append({
                    "hour": hour_item.get("hour", "-"),
                    "spend": f"${spend:,.0f}",
                    "revenue": f"${revenue:,.0f}",
                    "roas": f"{roas:.0%}{roas_mark}",
                    "market_roas": f"{market_roas:.0%}",
                    "cpm": f"${cpm:.1f}" if cpm else "-",
                    "market_cpm": f"${market_cpm:.1f}" if market_cpm else "-"
                })

            elements.append({
                "tag": "table",
                "page_size": 10,
                "columns": [
                    {"name": "hour", "display_name": "时间"},
                    {"name": "spend", "display_name": "花费"},
                    {"name": "revenue", "display_name": "收入"},
                    {"name": "roas", "display_name": "个人ROAS"},
                    {"name": "market_roas", "display_name": "大盘ROAS"},
                    {"name": "cpm", "display_name": "CPM"},
                    {"name": "market_cpm", "display_name": "大盘CPM"}
                ],
                "rows": table_rows
            })

            # 备注
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "_大盘ROAS为全公司该小时均值，仅供参考。🟢跑赢大盘 🔴跑输大盘_"}
            })
        else:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "暂无数据"}
            })

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"📊 [{optimizer}] 个人实况窗"},
                    "template": "blue"
                },
                "elements": elements
            }
        }
        return self._send(payload)

    def send_optimizer_smart_alerts(self, data: Dict[str, Any]) -> dict:
        """
        发送投手智能预警 - 带大盘对比 + AI分析 (Smart Alerts)

        Args:
            data: 预警数据，包含:
                - optimizer: 投手名称
                - stop_loss_alerts: [{campaign_name, drama_name, country, spend, roas, benchmark_roas, conclusion}]
                - scale_up_alerts: [{campaign_name, drama_name, country, spend, roas, benchmark_roas, conclusion}]
        """
        optimizer = data.get("optimizer", "未知")
        stop_loss = data.get("stop_loss_alerts", [])
        scale_up = data.get("scale_up_alerts", [])

        # 获取 AI 分析
        ai_analysis = {}
        if self.chatgpt_advisor and (stop_loss or scale_up):
            try:
                ai_analysis = self.chatgpt_advisor.analyze_smart_alerts(data)
            except Exception:
                pass

        # 构建 AI 分析列表 (按索引匹配)
        stop_loss_ai = ai_analysis.get("stop_loss_analysis", [])
        scale_up_ai = ai_analysis.get("scale_up_analysis", [])

        elements = []

        # 标题
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**⚡️ [{optimizer}] 操作建议**"}
        })
        elements.append({"tag": "hr"})

        # 止损预警
        if stop_loss:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "**🔴 止损预警**"}
            })
            for alert in stop_loss:
                campaign_id = alert.get("campaign_id", "")
                campaign_name = alert.get("campaign_name", "")
                drama = alert.get("drama_name", "")
                channel = alert.get("channel", "")
                spend = alert.get("spend", 0)
                roas = alert.get("roas", 0)
                channel_roas = alert.get("channel_roas", 0)
                spend_ratio_in_channel = alert.get("spend_ratio_in_channel", 0)
                top_country = alert.get("top_country")

                # 渠道对比 + 消耗占比
                vs_benchmark = ""
                if channel_roas > 0:
                    ratio_warning = " ⚠️占比高" if spend_ratio_in_channel > 0.5 else ""
                    vs_benchmark = f"\n  **对比{channel}渠道**: 渠道ROAS {channel_roas:.1%} | 你占渠道消耗{spend_ratio_in_channel:.1%}{ratio_warning}"

                # 计划名和ID
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"• **{campaign_name}**"}
                })
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"  ID: {campaign_id} | 剧:《{drama}》| {channel}"}
                })

                # 系列整体数据
                revenue = alert.get("revenue", 0)
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"  **整体**: 耗${spend:,.0f} | 收${revenue:,.0f} | ROAS {roas:.0%}{vs_benchmark}"}
                })

                # Top 国家数据
                if top_country:
                    tc = top_country
                    elements.append({
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": f"  **Top国家**: {tc['country']}({tc['spend_ratio']:.0%}) | ${tc['spend']:,.0f} | ROAS {tc['roas']:.0%}"}
                    })
                # AI 分析 (按索引匹配)
                idx = stop_loss.index(alert)
                ai_info = stop_loss_ai[idx] if idx < len(stop_loss_ai) else {}
                if ai_info:
                    if ai_info.get("reason"):
                        elements.append({
                            "tag": "div",
                            "text": {"tag": "lark_md", "content": f"  🤖 **原因分析**: {ai_info['reason']} [GPT]"}
                        })
                    if ai_info.get("action"):
                        elements.append({
                            "tag": "div",
                            "text": {"tag": "lark_md", "content": f"  🤖 **操作建议**: {ai_info['action']} [GPT]"}
                        })
                    if ai_info.get("trend"):
                        elements.append({
                            "tag": "div",
                            "text": {"tag": "lark_md", "content": f"  🤖 **趋势预测**: {ai_info['trend']} [GPT]"}
                        })
            elements.append({"tag": "hr"})

        # 扩量机会
        if scale_up:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "**🟢 扩量机会**"}
            })
            for alert in scale_up:
                campaign_id = alert.get("campaign_id", "")
                campaign_name = alert.get("campaign_name", "")
                drama = alert.get("drama_name", "")
                channel = alert.get("channel", "")
                spend = alert.get("spend", 0)
                roas = alert.get("roas", 0)
                channel_roas = alert.get("channel_roas", 0)
                spend_ratio_in_channel = alert.get("spend_ratio_in_channel", 0)
                top_country = alert.get("top_country")

                # 渠道对比 + 消耗占比
                vs_benchmark = ""
                if channel_roas > 0:
                    ratio_warning = " ⚠️占比高" if spend_ratio_in_channel > 0.5 else ""
                    vs_benchmark = f" (大盘{channel_roas:.0%}, 占比{spend_ratio_in_channel:.0%}{ratio_warning})"

                # 计划名和ID
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"• **{campaign_name}**"}
                })
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"  ID: {campaign_id} | 剧:《{drama}》| {channel}"}
                })

                # 系列整体数据
                revenue = alert.get("revenue", 0)
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"  **整体**: 耗${spend:,.0f} | 收${revenue:,.0f} | ROAS {roas:.0%}{vs_benchmark}"}
                })

                # Top 国家数据
                if top_country:
                    tc = top_country
                    elements.append({
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": f"  **Top国家**: {tc['country']}({tc['spend_ratio']:.0%}) | ${tc['spend']:,.0f} | ROAS {tc['roas']:.0%}"}
                    })
                # AI 分析 (按索引匹配)
                idx = scale_up.index(alert)
                ai_info = scale_up_ai[idx] if idx < len(scale_up_ai) else {}
                if ai_info:
                    if ai_info.get("reason"):
                        elements.append({
                            "tag": "div",
                            "text": {"tag": "lark_md", "content": f"  🤖 **成功原因**: {ai_info['reason']} [GPT]"}
                        })
                    if ai_info.get("action"):
                        elements.append({
                            "tag": "div",
                            "text": {"tag": "lark_md", "content": f"  🤖 **扩量建议**: {ai_info['action']} [GPT]"}
                        })
                    if ai_info.get("trend"):
                        elements.append({
                            "tag": "div",
                            "text": {"tag": "lark_md", "content": f"  🤖 **趋势预测**: {ai_info['trend']} [GPT]"}
                        })

        # AI 整体建议
        overall_advice = ai_analysis.get("overall_advice", "")
        if overall_advice and overall_advice != "AI 分析暂不可用":
            elements.append({"tag": "hr"})
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**💡 AI 整体建议**: {overall_advice} [GPT]"}
            })

        # 无预警
        if not stop_loss and not scale_up:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "当前无需操作，继续保持 👍"}
            })

        # 卡片颜色
        color = "red" if stop_loss else "green" if scale_up else "blue"

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"⚡️ [{optimizer}] 操作建议"},
                    "template": color
                },
                "elements": elements
            }
        }
        return self._send(payload)

    def send_optimizer_zombie_alerts(self, data: Dict[str, Any]) -> dict:
        """
        发送重启提醒 (Zombie Alert)

        Args:
            data: 重启提醒数据
        """
        optimizer = data.get("optimizer", "未知")
        zombies = data.get("zombie_alerts", [])

        elements = []

        if zombies:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "**🧟 重启机会 (Zombie Alert)**"}
            })
            elements.append({"tag": "hr"})

            for z in zombies:
                campaign = z.get("campaign_name", "")[:30]
                drama = z.get("drama_name", "")
                country = z.get("country", "")
                spend = z.get("spend", 0)
                revenue = z.get("revenue", 0)
                roas = z.get("roas", 0)

                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"• **计划**: {campaign} (已关停)"}
                })
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"  剧:《{drama}》| 国家: {country}"}
                })
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"  **当前累计**: 总耗 ${spend:,.0f} | 收入 ${revenue:,.0f} | ROAS {roas:.0%} ✅"}
                })
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"  **建议**: 该计划后劲较强，建议重启"}
                })
                elements.append({"tag": "hr"})
        else:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": "当前无重启机会"}
            })

        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"🧟 [{optimizer}] 重启机会"},
                    "template": "purple"
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
    import os
    # 从环境变量获取 API Key
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    chatgpt_api_key = os.getenv("OPENAI_API_KEY") or gemini_api_key

    bot = LarkBot(
        webhook_url=webhook_url,
        secret=secret,
        config=config,
        gemini_api_key=gemini_api_key,
        chatgpt_api_key=chatgpt_api_key
    )

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
