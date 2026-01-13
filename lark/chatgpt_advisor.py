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
- D0 ROAS: 当天的 ROAS（用户当天付费/当天广告消耗）
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
        d0_roas = summary.get("d0_roas", 0)

        # 计算小时环比
        hourly_delta = 0
        prev_spend = 0
        if prev_data:
            prev_spend = prev_data.get("total_spend", 0)
            hourly_delta = total_spend - prev_spend

        prompt = f"""## 实时投放数据分析 [{current_hour}]

### 当前状态
- 截止当前总消耗: ${total_spend:,.2f}
- 当前 D0 ROAS: {d0_roas:.1%}
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
        d0_roas = summary.get("d0_roas", 0)

        pace = "正常"
        if d0_roas < 0.30:
            pace = "效率偏低，关注止损"

        return {
            "hourly_trend": "数据不足",
            "urgent_actions": [],
            "watch_list": [],
            "pace_assessment": pace
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
