"""
ChatGPT AI 智能分析模块
用于数据趋势分析、异常检测、归因分析
通过 OminiLink 代理调用 GPT-4
"""

import os
import json
import time
from typing import Dict, Any, List, Optional


# 尝试导入 OpenAI SDK
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    OpenAI = None


class ChatGPTAdvisor:
    """ChatGPT AI 智能分析顾问（通过 OminiLink 代理）"""

    def __init__(self, api_key: str = None, model: str = None):
        """
        初始化 ChatGPT Advisor

        Args:
            api_key: OminiLink API Key，如果不传则从环境变量获取
            model: 模型名称，默认 gpt-4o
        """
        if not OPENAI_AVAILABLE:
            raise ImportError("请先安装 openai: pip install openai")

        self.api_key = api_key or os.getenv("OPENAI_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("请设置 OPENAI_API_KEY 或 GEMINI_API_KEY 环境变量")

        # 使用 OminiLink 代理
        self.client = OpenAI(
            base_url="https://api.ominilink.ai/v1",
            api_key=self.api_key
        )
        self.model_name = model or "gpt-5.2"

    def _call_api(self, prompt: str, system_prompt: str = None) -> str:
        """调用 API 获取响应"""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=messages,
            temperature=0.3  # 降低随机性，让分析更稳定
        )
        return response.choices[0].message.content

    def _call_api_with_retry(self, prompt: str, system_prompt: str = None) -> str:
        """带重试的 API 调用"""
        max_retries = 3
        retry_delays = [5, 15, 30]

        for attempt in range(max_retries):
            try:
                return self._call_api(prompt, system_prompt)
            except Exception as e:
                error_str = str(e)
                is_rate_limit = "429" in error_str or "rate" in error_str.lower()

                if is_rate_limit and attempt < max_retries - 1:
                    delay = retry_delays[attempt]
                    print(f"[ChatGPT] 速率限制，{delay}秒后重试 ({attempt + 1}/{max_retries})...")
                    time.sleep(delay)
                    continue

                print(f"[ChatGPT Error] {e}")
                raise e

    # ============ 核心分析方法 ============

    def analyze_daily_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        日报智能分析 - 发现趋势、异常、机会

        Args:
            data: 日报数据，包含:
                - date: 日期
                - summary: {total_spend, total_revenue, global_roas}
                - summary_prev: 前一天数据（用于对比）
                - optimizers: 投手数据列表
                - dramas: 剧集数据列表
                - countries: 国家数据列表
                - drama_country: 剧集x国家维度数据

        Returns:
            {
                "trend_analysis": "趋势分析",
                "anomalies": ["异常点1", "异常点2"],
                "opportunities": ["机会1", "机会2"],
                "risks": ["风险1", "风险2"],
                "key_insights": "核心洞察（1-2句话总结）"
            }
        """
        system_prompt = self._get_analyst_system_prompt()
        prompt = self._build_daily_analysis_prompt(data)

        try:
            response_text = self._call_api_with_retry(prompt, system_prompt)
            return self._parse_analysis_response(response_text)
        except Exception as e:
            print(f"[ChatGPT] 日报分析失败: {e}")
            return self._fallback_daily_analysis(data)

    def analyze_realtime_data(self, data: Dict[str, Any], prev_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        实时数据智能分析 - 小时级监控

        Args:
            data: 当前实时数据
            prev_data: 上一小时数据（用于对比）

        Returns:
            {
                "hourly_trend": "小时趋势判断",
                "urgent_actions": ["紧急操作1"],
                "watch_list": ["观察项1"],
                "pace_assessment": "消耗节奏评估"
            }
        """
        system_prompt = self._get_analyst_system_prompt()
        prompt = self._build_realtime_analysis_prompt(data, prev_data)

        try:
            response_text = self._call_api_with_retry(prompt, system_prompt)
            return self._parse_realtime_analysis(response_text)
        except Exception as e:
            print(f"[ChatGPT] 实时分析失败: {e}")
            return self._fallback_realtime_analysis(data)

    # ============ Prompt 构建 ============

    def _get_analyst_system_prompt(self) -> str:
        """获取分析师角色的 System Prompt"""
        return """你是一位资深的短剧广告投放数据分析师，擅长从数据中发现趋势、异常和机会。

你的分析风格：
1. 基于数据说话，不做无根据的猜测
2. 关注变化和趋势，而不只是静态数值
3. 区分"需要立即行动"和"值得观察"的情况
4. 给出判断时要说明理由
5. 如果数据不足以得出结论，诚实说明

你熟悉的业务指标：
- ROAS (Return on Ad Spend): 广告回报率，revenue/spend
- CPI (Cost Per Install): 单次安装成本
- Media ROAS: 媒体口径 ROAS（media_user_revenue/spend）
- 止损线：通常 ROAS < 30% 需要关注
- 健康线：通常 ROAS > 40% 表现良好

你的输出要求：
- 简洁有力，每条建议不超过50字
- 重点突出，先说结论再说原因
- 使用业务术语，面向投放团队"""

    def _build_daily_analysis_prompt(self, data: Dict[str, Any]) -> str:
        """构建日报分析 Prompt"""
        summary = data.get("summary", {})
        summary_prev = data.get("summary_prev", {})
        optimizers = data.get("optimizers", [])
        dramas = data.get("dramas", []) or data.get("dramas_top5", [])
        countries = data.get("countries", []) or data.get("countries_top5", [])

        # 计算环比
        spend = summary.get("total_spend", 0)
        prev_spend = summary_prev.get("total_spend", 0)
        roas = summary.get("global_roas", 0)
        prev_roas = summary_prev.get("global_roas", 0)

        spend_change = ((spend - prev_spend) / prev_spend * 100) if prev_spend > 0 else 0
        roas_change = ((roas - prev_roas) / prev_roas * 100) if prev_roas > 0 else 0

        prompt = f"""## 昨日投放数据分析

### 大盘概览
- 日期: {data.get("date", "未知")}
- 总消耗: ${spend:,.2f} (环比 {spend_change:+.1f}%)
- 综合 ROAS: {roas:.1%} (环比 {roas_change:+.1f}%)

### 投手数据
"""
        for opt in optimizers[:10]:
            prompt += f"- {opt.get('name')}: 消耗${opt.get('spend', 0):,.0f}, ROAS {opt.get('roas', 0):.1%}\n"

        prompt += "\n### 剧集数据\n"
        for d in dramas[:10]:
            prompt += f"- 《{d.get('name')}》: 消耗${d.get('spend', 0):,.0f}, ROAS {d.get('roas', 0):.1%}\n"

        prompt += "\n### 国家数据\n"
        for c in countries[:10]:
            prompt += f"- {c.get('name')}: 消耗${c.get('spend', 0):,.0f}, ROAS {c.get('roas', 0):.1%}\n"

        prompt += """
---

请分析以上数据，输出以下内容（JSON格式）：

```json
{
  "trend_analysis": "整体趋势判断（上升/下降/平稳，以及原因）",
  "anomalies": ["异常点1: 具体描述", "异常点2: 具体描述"],
  "opportunities": ["机会1: 具体描述", "机会2: 具体描述"],
  "risks": ["风险1: 具体描述"],
  "key_insights": "一句话核心洞察"
}
```

分析要点：
1. 对比环比数据，判断趋势方向
2. 找出表现异常的投手/剧集/国家（好的或差的）
3. 发现潜在机会（如某剧在某国表现突出）
4. 识别风险（如某投手效率持续下滑）
5. 给出最重要的1条洞察"""

        return prompt

    def _build_realtime_analysis_prompt(self, data: Dict[str, Any], prev_data: Dict[str, Any] = None) -> str:
        """构建实时分析 Prompt"""
        summary = data.get("summary", {})
        current_hour = data.get("current_hour", "未知")
        optimizer_spend = data.get("optimizer_spend", [])

        total_spend = summary.get("total_spend", 0)
        media_roas = summary.get("media_roas", 0)

        # 计算小时环比 - 优先从 data 中的 prev_hour_summary 获取
        hourly_delta = 0
        prev_spend = 0
        prev_hour_summary = data.get("prev_hour_summary", {})
        if prev_hour_summary:
            prev_spend = prev_hour_summary.get("total_spend", 0)
            hourly_delta = total_spend - prev_spend
        elif prev_data:
            prev_spend = prev_data.get("total_spend", 0)
            hourly_delta = total_spend - prev_spend

        prompt = f"""## 实时投放数据分析 [{current_hour}]

### 当前状态
- 截止当前总消耗: ${total_spend:,.2f}
- 当前 Media ROAS: {media_roas:.1%}
- 过去1小时新增消耗: ${hourly_delta:,.2f}

### 投手消耗情况
"""
        for opt in optimizer_spend[:8]:
            prompt += f"- {opt.get('optimizer')}: 累计${opt.get('spend', 0):,.0f}\n"

        prompt += """
---

请快速分析，输出以下内容（JSON格式）：

```json
{
  "hourly_trend": "小时趋势（加速/平稳/放缓）",
  "urgent_actions": ["需立即处理的事项"],
  "watch_list": ["值得关注但不紧急的事项"],
  "pace_assessment": "消耗节奏评估（是否正常）"
}
```"""

        return prompt

    # ============ 响应解析 ============

    def _parse_analysis_response(self, response_text: str) -> Dict[str, Any]:
        """解析日报分析响应"""
        try:
            json_str = self._extract_json(response_text)
            if json_str:
                return json.loads(json_str)
        except json.JSONDecodeError:
            pass

        return {
            "trend_analysis": response_text[:200],
            "anomalies": [],
            "opportunities": [],
            "risks": [],
            "key_insights": "分析结果解析失败"
        }

    def _parse_realtime_analysis(self, response_text: str) -> Dict[str, Any]:
        """解析实时分析响应"""
        try:
            json_str = self._extract_json(response_text)
            if json_str:
                return json.loads(json_str)
        except json.JSONDecodeError:
            pass

        return {
            "hourly_trend": "解析失败",
            "urgent_actions": [],
            "watch_list": [],
            "pace_assessment": response_text[:100]
        }

    def _extract_json(self, text: str) -> Optional[str]:
        """从文本中提取 JSON 字符串"""
        import re
        # 匹配 ```json ... ``` 或 { ... }
        patterns = [
            r'```json\s*([\s\S]*?)\s*```',
            r'```\s*([\s\S]*?)\s*```',
            r'(\{[\s\S]*\})'
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        return None

    # ============ 降级方法 ============

    def _fallback_daily_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """日报分析降级"""
        summary = data.get("summary", {})
        roas = summary.get("global_roas", 0)

        trend = "平稳"
        if roas >= 0.40:
            trend = "大盘健康，ROAS 达标"
        elif roas >= 0.30:
            trend = "效率略低，需关注"
        else:
            trend = "效率偏低，建议优先止损"

        return {
            "trend_analysis": trend,
            "anomalies": [],
            "opportunities": [],
            "risks": [],
            "key_insights": "AI 分析暂不可用，请查看数据明细"
        }

    def _fallback_realtime_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """实时分析降级"""
        summary = data.get("summary", {})
        media_roas = summary.get("media_roas", 0)

        pace = "正常"
        if media_roas < 0.30:
            pace = "效率偏低，关注止损"

        return {
            "hourly_trend": "数据不足",
            "urgent_actions": [],
            "watch_list": [],
            "pace_assessment": pace
        }

    # ============ 智能预警分析 ============

    def analyze_smart_alerts(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        智能预警 AI 分析 - 分析原因、给出操作建议、预测趋势

        Args:
            data: 预警数据，包含:
                - optimizer: 投手名称
                - stop_loss_alerts: 止损预警列表
                - scale_up_alerts: 扩量机会列表

        Returns:
            {
                "stop_loss_analysis": [{"campaign": "", "reason": "", "action": "", "trend": ""}],
                "scale_up_analysis": [{"campaign": "", "reason": "", "action": "", "trend": ""}],
                "overall_advice": "整体建议"
            }
        """
        try:
            prompt = self._build_smart_alerts_prompt(data)
            system_prompt = """你是一位资深的广告投放优化师，擅长分析广告数据并给出精准的操作建议。
请基于数据给出简洁、可执行的建议，每条建议不超过30字。"""

            response = self._call_api_with_retry(prompt, system_prompt)
            return self._parse_smart_alerts_response(response)
        except Exception as e:
            print(f"[ChatGPT] 智能预警分析失败: {e}")
            return self._fallback_smart_alerts(data)

    def _build_smart_alerts_prompt(self, data: Dict[str, Any]) -> str:
        """构建智能预警分析 Prompt"""
        optimizer = data.get("optimizer", "未知")
        stop_loss = data.get("stop_loss_alerts", [])
        scale_up = data.get("scale_up_alerts", [])

        prompt = f"""## 投手 [{optimizer}] 智能预警分析

### 止损预警计划 ({len(stop_loss)} 个)
"""
        for alert in stop_loss[:5]:
            prompt += f"""- 计划: {alert.get('campaign_name', '')[:30]}
  剧集: {alert.get('drama_name', '')} | 国家: {alert.get('country', '')}
  消耗: ${alert.get('spend', 0):,.0f} | ROAS: {alert.get('roas', 0):.0%}
  大盘平均: {alert.get('benchmark_roas', 0):.0%}
  系统结论: {alert.get('conclusion', '建议关停')}
"""

        prompt += f"""
### 扩量机会计划 ({len(scale_up)} 个)
"""
        for alert in scale_up[:5]:
            prompt += f"""- 计划: {alert.get('campaign_name', '')[:30]}
  剧集: {alert.get('drama_name', '')} | 国家: {alert.get('country', '')}
  消耗: ${alert.get('spend', 0):,.0f} | ROAS: {alert.get('roas', 0):.0%}
  大盘平均: {alert.get('benchmark_roas', 0):.0%}
  系统结论: {alert.get('conclusion', '建议加预算')}
"""

        prompt += """
---
请分析每个计划，输出 JSON 格式：

```json
{
  "stop_loss_analysis": [
    {"campaign": "计划名", "reason": "ROAS低的可能原因", "action": "具体操作建议", "trend": "预测趋势"}
  ],
  "scale_up_analysis": [
    {"campaign": "计划名", "reason": "ROAS高的原因", "action": "扩量建议", "trend": "预测趋势"}
  ],
  "overall_advice": "给该投手的整体建议(一句话)"
}
```

分析要点：
1. 止损原因：素材疲劳/受众不匹配/出价过高/竞争加剧
2. 扩量原因：素材优质/受众精准/市场红利
3. 止损操作：可建议"关停"或"降预算+换素材"，灵活判断
4. 扩量操作：加预算xx%、复制扩量等
5. 趋势预测：继续恶化/可能回升/持续向好"""

        return prompt

    def _parse_smart_alerts_response(self, response_text: str) -> Dict[str, Any]:
        """解析智能预警分析响应"""
        try:
            json_str = self._extract_json(response_text)
            if json_str:
                return json.loads(json_str)
        except json.JSONDecodeError:
            pass

        return {
            "stop_loss_analysis": [],
            "scale_up_analysis": [],
            "overall_advice": response_text[:100] if response_text else "分析结果解析失败"
        }

    def _fallback_smart_alerts(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """智能预警分析降级"""
        return {
            "stop_loss_analysis": [],
            "scale_up_analysis": [],
            "overall_advice": "AI 分析暂不可用"
        }

    # ============ 周报分析 ============

    def analyze_weekly_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        周报智能分析 - 周度趋势、核心发现、风险提示、下周建议

        Args:
            data: 周报数据，包含:
                - week_start, week_end: 周期
                - summary: 本周大盘汇总
                - prev_week_summary: 上周数据
                - daily_stats: 日趋势
                - optimizer_weekly: 投手周度数据
                - top_dramas, potential_dramas, declining_dramas: 剧集分类
                - top_countries, emerging_markets: 市场数据

        Returns:
            {
                "key_findings": "核心发现",
                "risk_alerts": "风险提示",
                "next_week_suggestions": "下周建议"
            }
        """
        try:
            prompt = self._build_weekly_analysis_prompt(data)
            system_prompt = """你是一位资深的广告投放策略分析师，擅长从周度数据中发现趋势和机会。
请基于数据给出简洁、有洞察力的分析，每条建议不超过50字。"""

            response = self._call_api_with_retry(prompt, system_prompt)
            return self._parse_weekly_analysis(response)
        except Exception as e:
            print(f"[ChatGPT] 周报分析失败: {e}")
            return self._fallback_weekly_analysis(data)

    def _build_weekly_analysis_prompt(self, data: Dict[str, Any]) -> str:
        """构建周报分析 Prompt"""
        week_start = data.get("week_start", "")
        week_end = data.get("week_end", "")
        summary = data.get("summary", {})
        prev_summary = data.get("prev_week_summary", {})
        daily_stats = data.get("daily_stats", [])
        optimizer_weekly = data.get("optimizer_weekly", [])
        top_dramas = data.get("top_dramas", [])
        potential_dramas = data.get("potential_dramas", [])
        declining_dramas = data.get("declining_dramas", [])
        top_countries = data.get("top_countries", [])
        emerging_markets = data.get("emerging_markets", [])

        # 计算环比
        week_spend = summary.get("week_total_spend", 0)
        prev_spend = prev_summary.get("week_total_spend", 0)
        week_roas = summary.get("week_avg_roas", 0)
        prev_roas = prev_summary.get("week_avg_roas", 0)
        spend_change = (week_spend - prev_spend) / prev_spend if prev_spend > 0 else 0
        roas_change = week_roas - prev_roas

        prompt = f"""## 周报数据分析 [{week_start} ~ {week_end}]

### 大盘概况
- 周总消耗: ${week_spend:,.0f} (环比 {spend_change:+.1%})
- 周均 ROAS: {week_roas:.1%} (环比 {roas_change:+.1%})
- 日均消耗: ${summary.get('daily_avg_spend', 0):,.0f}

### 日趋势
"""
        for day in daily_stats:
            prompt += f"- {day['date']}: ${day['spend']:,.0f}, ROAS {day['roas']:.1%}\n"

        prompt += "\n### 投手表现 (按消耗排序)\n"
        for opt in optimizer_weekly[:5]:
            change = opt.get('roas_change', 0)
            prompt += f"- {opt['name']}: ${opt['spend']:,.0f}, ROAS {opt['roas']:.1%} ({change:+.1%})\n"

        prompt += "\n### 头部剧集 (消耗>$10k, ROAS>40%)\n"
        for d in top_dramas[:3]:
            prompt += f"- {d['name']}: ${d['spend']:,.0f}, ROAS {d['roas']:.1%}\n"

        prompt += "\n### 潜力剧集 (消耗$1k-$10k, ROAS>50%)\n"
        for d in potential_dramas[:3]:
            prompt += f"- {d['name']}: ${d['spend']:,.0f}, ROAS {d['roas']:.1%}\n"

        prompt += "\n### 衰退预警 (ROAS环比下降>10%)\n"
        for d in declining_dramas[:3]:
            prompt += f"- {d['name']}: ROAS {d['roas']:.1%} ({d.get('roas_change', 0):+.1%})\n"

        prompt += "\n### 主力市场\n"
        for c in top_countries[:5]:
            prompt += f"- {c['name']}: ${c['spend']:,.0f}, ROAS {c['roas']:.1%}\n"

        prompt += "\n### 新兴机会 (非主投国家, ROAS>50%)\n"
        for m in emerging_markets[:3]:
            prompt += f"- {m['name']}: ROAS {m['roas']:.1%}, ${m['spend']:,.0f}\n"

        prompt += """
---
请分析以上周度数据，输出 JSON 格式：

```json
{
  "key_findings": "本周最重要的1-2个发现（50字内）",
  "risk_alerts": "需要关注的风险点（50字内）",
  "next_week_suggestions": "下周操作建议（50字内）"
}
```

分析要点：
1. 从消耗和ROAS趋势判断整体健康度
2. 识别表现突出或下滑的投手/剧集/市场
3. 发现可放量的潜力剧集和新兴市场
4. 给出具体可执行的下周建议"""

        return prompt

    def _parse_weekly_analysis(self, response_text: str) -> Dict[str, Any]:
        """解析周报分析响应"""
        try:
            json_str = self._extract_json(response_text)
            if json_str:
                return json.loads(json_str)
        except json.JSONDecodeError:
            pass

        return {
            "key_findings": response_text[:100] if response_text else "分析结果解析失败",
            "risk_alerts": "",
            "next_week_suggestions": ""
        }

    def _fallback_weekly_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """周报分析降级"""
        summary = data.get("summary", {})
        roas = summary.get("week_avg_roas", 0)

        if roas >= 0.40:
            finding = "本周整体效率良好，ROAS达标"
        elif roas >= 0.30:
            finding = "本周效率一般，需关注优化"
        else:
            finding = "本周效率偏低，建议重点止损"

        return {
            "key_findings": finding,
            "risk_alerts": "AI 分析暂不可用",
            "next_week_suggestions": "请查看数据明细制定策略"
        }


# ============ 工厂函数 ============

def create_chatgpt_advisor(api_key: str = None, model: str = None) -> Optional[ChatGPTAdvisor]:
    """
    创建 ChatGPTAdvisor 实例

    Args:
        api_key: API Key，不传则从环境变量获取
        model: 模型名称，默认 gpt-4o

    Returns:
        ChatGPTAdvisor 实例，如果依赖不可用则返回 None
    """
    if not OPENAI_AVAILABLE:
        return None

    try:
        return ChatGPTAdvisor(api_key, model)
    except (ImportError, ValueError) as e:
        print(f"[ChatGPT] 初始化失败: {e}")
        return None
