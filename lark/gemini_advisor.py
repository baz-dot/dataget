"""
Gemini AI Advisor 模块
用于生成智能策略建议
通过 OminiLink 代理调用 Gemini 3 Pro
"""

import os
import time
from typing import Dict, Any, List, Optional

# 尝试导入 OpenAI SDK
try:
    from openai import OpenAI
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    OpenAI = None


class GeminiAdvisor:
    """Gemini AI 策略顾问（通过 OminiLink 代理）"""

    def __init__(self, api_key: str = None):
        """
        初始化 Gemini Advisor

        Args:
            api_key: OminiLink API Key，如果不传则从环境变量 GEMINI_API_KEY 获取
        """
        if not GEMINI_AVAILABLE:
            raise ImportError("请先安装 openai: pip install openai")

        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("请设置 GEMINI_API_KEY 环境变量或传入 api_key 参数")

        # 使用 OminiLink 代理
        self.client = OpenAI(
            base_url="https://api.ominilink.ai/v1",
            api_key=self.api_key
        )
        self.model_name = "gemini-3-pro-preview"  # Gemini 3 Pro

    def _call_api(self, prompt: str) -> str:
        """调用 API 获取响应"""
        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content

    def generate_strategy_insights(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        生成策略建议 (AI Insight)

        根据日报数据生成三类建议：
        1. 放量剧目建议
        2. 机会市场建议
        3. 测剧建议

        Args:
            data: 日报数据，包含:
                - summary: {total_spend, global_roas}
                - dramas: [{name, spend, roas}] 所有剧集数据
                - countries: [{name, spend, roas}] 所有国家数据
                - drama_country: [{drama_name, country, spend, roas}] 剧集x国家维度数据
                - top3_countries: [str] 主投Top3国家列表

        Returns:
            {
                "scale_up_drama": "建议放量剧目文案",
                "opportunity_market": "机会市场文案",
                "test_drama_suggestion": "测剧建议文案"
            }
        """
        # 1. 筛选放量剧目: Spend > $1000 且 ROAS > 45%
        # 兼容两种字段名: dramas 或 dramas_top5
        dramas = data.get("dramas", []) or data.get("dramas_top5", [])
        scale_up_candidates = [
            d for d in dramas
            if d.get("spend", 0) > 1000 and d.get("roas", 0) > 0.45
        ]
        # 按 ROAS 降序排序
        scale_up_candidates.sort(key=lambda x: x.get("roas", 0), reverse=True)

        # 2. 筛选机会市场: Spend > $100 且 ROAS > 50% 且不在主投Top3国家
        # 兼容两种字段名: top3_countries 或从 countries_top5 提取
        top3_countries = set(data.get("top3_countries", []) or [])
        if not top3_countries:
            countries_top5 = data.get("countries_top5", [])
            if countries_top5:
                top3_countries = set([c.get("name", "") for c in countries_top5[:3]])

        # 兼容两种字段名: drama_country 或 opportunity_markets
        drama_country = data.get("drama_country", []) or data.get("opportunity_markets", [])
        opportunity_candidates = [
            dc for dc in drama_country
            if dc.get("spend", 0) > 100
            and dc.get("roas", 0) > 0.50
            and dc.get("country", "") not in top3_countries
        ]
        # 按 ROAS 降序排序
        opportunity_candidates.sort(key=lambda x: x.get("roas", 0), reverse=True)

        # 3. 构建 Prompt 让 Gemini 生成建议
        prompt = self._build_strategy_prompt(
            summary=data.get("summary", {}),
            scale_up_candidates=scale_up_candidates,
            opportunity_candidates=opportunity_candidates,
            dramas=dramas,
            top3_countries=list(top3_countries)
        )

        # 重试配置：最多重试3次，每次等待时间递增
        max_retries = 3
        retry_delays = [5, 15, 30]  # 秒

        for attempt in range(max_retries):
            try:
                response_text = self._call_api(prompt)
                return self._parse_strategy_response(
                    response_text,
                    scale_up_candidates,
                    opportunity_candidates,
                    dramas
                )
            except Exception as e:
                error_str = str(e)
                is_rate_limit = "429" in error_str or "rate" in error_str.lower()

                if is_rate_limit and attempt < max_retries - 1:
                    delay = retry_delays[attempt]
                    print(f"[Gemini] 速率限制，{delay}秒后重试 ({attempt + 1}/{max_retries})...")
                    time.sleep(delay)
                    continue

                # 最后一次重试失败或非速率限制错误，降级到规则生成
                print(f"[Gemini Error] {e}")
                return self._fallback_strategy(
                    scale_up_candidates,
                    opportunity_candidates,
                    dramas
                )

    def _build_strategy_prompt(
        self,
        summary: Dict,
        scale_up_candidates: List[Dict],
        opportunity_candidates: List[Dict],
        dramas: List[Dict],
        top3_countries: List[str]
    ) -> str:
        """构建策略建议 Prompt"""

        # 格式化放量候选
        scale_up_text = ""
        if scale_up_candidates:
            scale_up_text = "\n".join([
                f"- 《{d['name']}》: 消耗${d['spend']:.0f}, ROAS {d['roas']:.0%}"
                for d in scale_up_candidates[:5]
            ])
        else:
            scale_up_text = "暂无符合条件的剧目（条件：消耗>$1000 且 ROAS>45%）"

        # 格式化机会市场候选
        opportunity_text = ""
        if opportunity_candidates:
            opportunity_text = "\n".join([
                f"- 《{dc['drama_name']}》在 {dc['country']}: 消耗${dc['spend']:.0f}, ROAS {dc['roas']:.0%}"
                for dc in opportunity_candidates[:5]
            ])
        else:
            opportunity_text = "暂无符合条件的机会市场（条件：消耗>$100 且 ROAS>50% 且不在主投Top3国家）"

        # 格式化所有剧集概况
        drama_summary = f"当前在投剧集数量: {len(dramas)}"
        if dramas:
            avg_roas = sum(d.get("roas", 0) for d in dramas) / len(dramas)
            drama_summary += f", 平均ROAS: {avg_roas:.0%}"

        prompt = f"""你是一位资深的短剧广告投放专家。请严格根据以下数据和规则，生成策略建议。

## 大盘数据
- 总消耗: ${summary.get('total_spend', 0):,.0f}
- 综合ROAS: {summary.get('global_roas', 0):.0%}
- 主投国家Top3: {', '.join(top3_countries) if top3_countries else '未知'}

## 放量剧目候选（已筛选：Spend>$1000 且 ROAS>45%）
{scale_up_text}

## 机会市场候选（已筛选：Spend>$100 且 ROAS>50% 且不在主投Top3）
{opportunity_text}

## 剧集概况
{drama_summary}

---

## 严格规则（必须遵守）

### 规则1: 放量剧目建议
- 只能从上面的"放量剧目候选"列表中选择，不能推荐列表外的剧目
- 如果候选列表为空或显示"暂无符合条件"，必须回复："暂无符合条件的剧目"
- 如果有候选，选择ROAS最高的1个，格式：《剧名》(ROAS XX%, 消耗$X.Xk+)，可加简短理由

### 规则2: 机会市场建议
- 只能从上面的"机会市场候选"列表中选择，不能推荐列表外的组合
- 如果候选列表为空或显示"暂无符合条件"，必须回复："暂无新兴市场机会"
- 如果有候选，选择ROAS最高的1个，格式：剧集《剧名》在 [国家] ROAS XX%，建议增投，可加简短理由

### 规则3: 测剧建议
- 剧集数量 < 3：回复"本周新剧测试数量不足，建议增加素材供给"
- 剧集数量 3-5：回复"在投剧集较少，建议适当增加测试新剧"
- 剧集数量 > 5 但高效剧集(ROAS>45%)少于2个：回复"高效剧集较少，建议加大测剧力度寻找爆款"
- 剧集数量 > 5 且高效剧集>=2个：回复"当前剧集储备充足，可聚焦优化现有高效剧目"
- 可以在规则文案基础上稍作润色，但核心意思不能变

---

请严格按以下格式输出（每条不超过50字）：

1. 放量剧目建议：[严格按规则1输出]
2. 机会市场建议：[严格按规则2输出]
3. 测剧建议：[严格按规则3输出]

重要：不要编造数据，不要推荐候选列表之外的内容，不要使用markdown格式。
"""
        return prompt

    def _parse_strategy_response(
        self,
        response_text: str,
        scale_up_candidates: List[Dict],
        opportunity_candidates: List[Dict],
        dramas: List[Dict]
    ) -> Dict[str, str]:
        """解析 Gemini 响应"""

        result = {
            "scale_up_drama": "",
            "opportunity_market": "",
            "test_drama_suggestion": ""
        }

        lines = response_text.strip().split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 移除序号前缀
            if line.startswith(("1.", "2.", "3.")):
                line = line[2:].strip()

            if "放量剧目" in line:
                # 提取冒号后的内容
                if "：" in line:
                    result["scale_up_drama"] = line.split("：", 1)[-1].strip()
                elif ":" in line:
                    result["scale_up_drama"] = line.split(":", 1)[-1].strip()
            elif "机会市场" in line:
                if "：" in line:
                    result["opportunity_market"] = line.split("：", 1)[-1].strip()
                elif ":" in line:
                    result["opportunity_market"] = line.split(":", 1)[-1].strip()
            elif "测剧建议" in line:
                if "：" in line:
                    result["test_drama_suggestion"] = line.split("：", 1)[-1].strip()
                elif ":" in line:
                    result["test_drama_suggestion"] = line.split(":", 1)[-1].strip()

        # 如果解析失败，使用 fallback
        if not result["scale_up_drama"] and not result["opportunity_market"]:
            return self._fallback_strategy(scale_up_candidates, opportunity_candidates, dramas)

        return result

    def _fallback_strategy(
        self,
        scale_up_candidates: List[Dict],
        opportunity_candidates: List[Dict],
        dramas: List[Dict]
    ) -> Dict[str, str]:
        """降级策略：使用规则生成建议"""

        result = {
            "scale_up_drama": "暂无符合条件的剧目",
            "opportunity_market": "暂无新兴市场机会",
            "test_drama_suggestion": ""
        }

        # 放量剧目
        if scale_up_candidates:
            d = scale_up_candidates[0]
            result["scale_up_drama"] = f"《{d['name']}》(ROAS {d['roas']:.0%}, 消耗${d['spend']/1000:.1f}k+)"

        # 机会市场
        if opportunity_candidates:
            dc = opportunity_candidates[0]
            result["opportunity_market"] = f"剧集《{dc['drama_name']}》在 [{dc['country']}] ROAS {dc['roas']:.0%}，建议增投"

        # 测剧建议
        if len(dramas) < 3:
            result["test_drama_suggestion"] = "本周新剧测试数量不足，建议增加素材供给"
        elif len(dramas) < 5:
            result["test_drama_suggestion"] = "在投剧集较少，建议适当增加测试新剧"
        else:
            # 检查是否有高ROAS剧集
            high_roas_count = len([d for d in dramas if d.get("roas", 0) > 0.45])
            if high_roas_count < 2:
                result["test_drama_suggestion"] = "高效剧集较少，建议加大测剧力度寻找爆款"
            else:
                result["test_drama_suggestion"] = "当前剧集储备充足，可聚焦优化现有高效剧目"

        return result

    # ============ 实时播报 AI 建议 ============
    def generate_realtime_insights(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        生成实时播报的 AI 建议

        根据实时数据生成：
        1. 整体态势评估
        2. 止损计划的具体操作建议
        3. 扩量计划的具体操作建议

        Args:
            data: 实时播报数据，包含:
                - summary: {total_spend, d0_roas}
                - stop_loss_campaigns: [{campaign_name, optimizer, spend, roas, drama_name, country}]
                - scale_up_campaigns: [{campaign_name, optimizer, spend, roas, drama_name, country}]
                - country_marginal_roas: [{country, spend, roas}]

        Returns:
            {
                "overall_assessment": "整体态势评估",
                "stop_loss_advice": "止损建议",
                "scale_up_advice": "扩量建议"
            }
        """
        summary = data.get("summary", {})
        stop_loss = data.get("stop_loss_campaigns", [])
        scale_up = data.get("scale_up_campaigns", [])
        country_roas = data.get("country_marginal_roas", [])

        # 构建 Prompt
        prompt = self._build_realtime_prompt(summary, stop_loss, scale_up, country_roas)

        # 重试配置
        max_retries = 3
        retry_delays = [5, 15, 30]

        for attempt in range(max_retries):
            try:
                response_text = self._call_api(prompt)
                return self._parse_realtime_response(response_text, summary, stop_loss, scale_up)
            except Exception as e:
                error_str = str(e)
                is_rate_limit = "429" in error_str or "rate" in error_str.lower()

                if is_rate_limit and attempt < max_retries - 1:
                    delay = retry_delays[attempt]
                    print(f"[Gemini] 速率限制，{delay}秒后重试 ({attempt + 1}/{max_retries})...")
                    time.sleep(delay)
                    continue

                print(f"[Gemini Error] {e}")
                return self._fallback_realtime(summary, stop_loss, scale_up)

    def _build_realtime_prompt(
        self,
        summary: Dict,
        stop_loss: List[Dict],
        scale_up: List[Dict],
        country_roas: List[Dict]
    ) -> str:
        """构建实时播报 AI 建议 Prompt"""

        total_spend = summary.get("total_spend", 0)
        d0_roas = summary.get("d0_roas", 0)

        # 格式化止损计划
        stop_loss_text = ""
        if stop_loss:
            stop_loss_text = "\n".join([
                f"- {c.get('optimizer', '未知')}: 《{c.get('drama_name', '未知')}》({c.get('country', '未知')}) 消耗${c.get('spend', 0):.0f}, ROAS {c.get('roas', 0):.0%}"
                for c in stop_loss[:5]
            ])
        else:
            stop_loss_text = "暂无需要止损的计划"

        # 格式化扩量计划
        scale_up_text = ""
        if scale_up:
            scale_up_text = "\n".join([
                f"- {c.get('optimizer', '未知')}: 《{c.get('drama_name', '未知')}》({c.get('country', '未知')}) 消耗${c.get('spend', 0):.0f}, ROAS {c.get('roas', 0):.0%}"
                for c in scale_up[:5]
            ])
        else:
            scale_up_text = "暂无符合扩量条件的计划"

        # 格式化高 ROAS 国家
        high_roas_countries = [c for c in country_roas if c.get("roas", 0) > 0.50][:5]
        country_text = ""
        if high_roas_countries:
            country_text = ", ".join([f"{c['country']}({c['roas']:.0%})" for c in high_roas_countries])
        else:
            country_text = "暂无"

        prompt = f"""你是一位资深的短剧广告投放专家。请严格根据以下实时数据和规则，生成操作建议。

## 当前大盘数据
- 截止当前总消耗: ${total_spend:,.0f}
- 当前 D0 ROAS: {d0_roas:.1%}

## 止损预警计划（已筛选：Spend>$300 且 ROAS<30%）
{stop_loss_text}

## 扩量机会计划（已筛选：Spend>$300 且 ROAS>50%）
{scale_up_text}

## 高 ROAS 国家（边际ROAS>50%）
{country_text}

---

## 严格规则（必须遵守）

### 规则1: 整体态势评估
- ROAS >= 40%：回复"大盘健康，继续保持当前节奏"
- ROAS 30%-40%：回复"效率略低，需关注低效计划"
- ROAS < 30%：回复"效率偏低，建议收缩消耗、优先止损"
- 可在规则基础上稍作润色，但核心意思不能变

### 规则2: 止损建议
- 如果有止损计划：针对消耗最高的1-2个计划，给出具体建议（如"立即关停"或"降低预算50%"）
- 如果没有止损计划：回复"当前无需止损"
- 必须提及具体的投手名和剧名

### 规则3: 扩量建议
- 如果有扩量计划：针对ROAS最高的1-2个计划，给出具体建议（如"提升预算30%"或"复制计划到新国家"）
- 如果没有扩量计划：回复"当前无明显扩量机会"
- 必须提及具体的投手名和剧名

---

请严格按以下格式输出（每条不超过50字）：

1. 整体态势：[严格按规则1输出]
2. 止损建议：[严格按规则2输出]
3. 扩量建议：[严格按规则3输出]

重要：不要编造数据，只能基于上面提供的计划列表给建议，不要使用markdown格式。
"""
        return prompt

    def _parse_realtime_response(
        self,
        response_text: str,
        summary: Dict,
        stop_loss: List[Dict],
        scale_up: List[Dict]
    ) -> Dict[str, str]:
        """解析实时播报 AI 响应"""

        result = {
            "overall_assessment": "",
            "stop_loss_advice": "",
            "scale_up_advice": ""
        }

        lines = response_text.strip().split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 移除序号前缀
            if line.startswith(("1.", "2.", "3.")):
                line = line[2:].strip()

            if "整体态势" in line:
                if "：" in line:
                    result["overall_assessment"] = line.split("：", 1)[-1].strip()
                elif ":" in line:
                    result["overall_assessment"] = line.split(":", 1)[-1].strip()
            elif "止损建议" in line:
                if "：" in line:
                    result["stop_loss_advice"] = line.split("：", 1)[-1].strip()
                elif ":" in line:
                    result["stop_loss_advice"] = line.split(":", 1)[-1].strip()
            elif "扩量建议" in line:
                if "：" in line:
                    result["scale_up_advice"] = line.split("：", 1)[-1].strip()
                elif ":" in line:
                    result["scale_up_advice"] = line.split(":", 1)[-1].strip()

        # 如果解析失败，使用 fallback
        if not result["overall_assessment"]:
            return self._fallback_realtime(summary, stop_loss, scale_up)

        return result

    def _fallback_realtime(
        self,
        summary: Dict,
        stop_loss: List[Dict],
        scale_up: List[Dict]
    ) -> Dict[str, str]:
        """实时播报降级策略"""

        d0_roas = summary.get("d0_roas", 0)

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


def create_advisor(api_key: str = None) -> Optional[GeminiAdvisor]:
    """
    创建 GeminiAdvisor 实例的工厂函数

    如果依赖不可用，返回 None
    """
    if not GEMINI_AVAILABLE:
        return None

    try:
        return GeminiAdvisor(api_key)
    except (ImportError, ValueError):
        return None
