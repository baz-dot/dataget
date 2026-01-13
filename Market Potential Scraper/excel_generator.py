"""
Excel 报告生成器
生成《9部剧市场潜力分析报告.xlsx》
包含3个Sheet: 日数据、生命周期汇总、聚合分析排名
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class ExcelReportGenerator:
    """Excel 报告生成器"""

    def __init__(self, output_path: str = None):
        """
        初始化报告生成器

        Args:
            output_path: 输出文件路径,默认为当前目录
        """
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"9部剧市场潜力分析报告_{timestamp}.xlsx"

        self.output_path = output_path
        logger.info(f"初始化 Excel 报告生成器,输出路径: {output_path}")

    def calculate_heat_score(self, data: Dict) -> float:
        """
        计算热度得分

        公式: heat_score = (
            recent_30d_impressions * 0.4 +
            total_2y_impressions * 0.3 +
            lifecycle_days * 0.2 +
            publisher_count * 0.1
        )

        Args:
            data: 聚合后的数据字典

        Returns:
            热度得分
        """
        try:
            # 提取数据
            window_30d = data.get('window_30d', {})
            window_2y = data.get('window_2y', {})

            recent_30d_impressions = window_30d.get('total_exposure', 0)
            total_2y_impressions = window_2y.get('total_exposure', 0)
            lifecycle_days = window_2y.get('lifecycle_days', 0)
            publisher_count = len(window_2y.get('publishers', []))

            # 归一化处理 (避免数值过大)
            # 曝光量除以 1M, 生命周期天数除以 100, 发行商数量保持原值
            normalized_30d = recent_30d_impressions / 1_000_000
            normalized_2y = total_2y_impressions / 1_000_000
            normalized_lifecycle = lifecycle_days / 100
            normalized_publishers = publisher_count

            # 计算加权得分
            heat_score = (
                normalized_30d * 0.4 +
                normalized_2y * 0.3 +
                normalized_lifecycle * 0.2 +
                normalized_publishers * 0.1
            )

            return round(heat_score, 2)

        except Exception as e:
            logger.error(f"计算热度得分失败: {e}")
            return 0.0

    def determine_priority(self, data: Dict, heat_score: float) -> str:
        """
        确定优先级 (S/A/B/C)

        决策逻辑:
        - Priority S (蓝海爆款): 无或极少 ADX 记录
        - Priority A (验证过的好剧): 高2年曝光 + 长生命周期 + 低近期活跃
        - Priority B (观望剧): 中等数据
        - Priority C (红海/烂剧): 超高近期曝光 或 超短生命周期

        Args:
            data: 聚合后的数据字典
            heat_score: 热度得分

        Returns:
            优先级 (S/A/B/C)
        """
        try:
            window_30d = data.get('window_30d', {})
            window_2y = data.get('window_2y', {})

            recent_30d_impressions = window_30d.get('total_exposure', 0)
            total_2y_impressions = window_2y.get('total_exposure', 0)
            lifecycle_days = window_2y.get('lifecycle_days', 0)
            publisher_count = len(window_2y.get('publishers', []))

            # Priority S: 蓝海机会 (无数据或极少数据)
            if total_2y_impressions == 0 or total_2y_impressions < 1_000_000:
                return 'S'

            # Priority C: 红海市场 (近期超高曝光)
            if recent_30d_impressions > 10_000_000:
                return 'C'

            # Priority C: 一波流 (生命周期过短)
            if lifecycle_days > 0 and lifecycle_days < 14:
                return 'C'

            # Priority A: 验证过的好剧
            # 条件: 高2年曝光 + 长生命周期 + 低近期活跃
            if (total_2y_impressions > 50_000_000 and
                lifecycle_days > 90 and
                recent_30d_impressions < 1_000_000):
                return 'A'

            # Priority B: 其他情况
            return 'B'

        except Exception as e:
            logger.error(f"确定优先级失败: {e}")
            return 'B'

    def generate_sheet1_daily_data(self, results: List[Dict]) -> pd.DataFrame:
        """
        生成 Sheet 1: 近30天日数据

        列: 剧名, 日期, 曝光量, 素材数, 投放国家数, 投放平台数, 主要投放平台

        Args:
            results: 所有短剧的搜索结果列表

        Returns:
            DataFrame
        """
        logger.info("生成 Sheet 1: 近30天日数据...")

        rows = []

        for result in results:
            if not result.get('success'):
                continue

            drama_name = result.get('drama_name', '')
            aggregated = result.get('aggregated', {})
            window_30d = aggregated.get('window_30d', {})

            # 如果没有30天数据,添加一行空数据
            if window_30d.get('creative_count', 0) == 0:
                rows.append({
                    '剧名': drama_name,
                    '日期': '',
                    '曝光量': 0,
                    '素材数': 0,
                    '投放国家数': 0,
                    '投放平台数': 0,
                    '主要投放平台': '无数据'
                })
                continue

            # 生成近30天的每日数据 (简化版: 使用总数据)
            # 注: 如果需要真实的每日数据,需要从 API 获取更详细的时间序列数据
            total_exposure = window_30d.get('total_exposure', 0)
            creative_count = window_30d.get('creative_count', 0)
            country_count = len(window_30d.get('countries', []))
            platform_count = len(window_30d.get('platforms', []))
            platforms = window_30d.get('platforms', [])
            main_platform = ', '.join(platforms[:3]) if platforms else '未知'

            # 添加汇总行
            rows.append({
                '剧名': drama_name,
                '日期': '近30天汇总',
                '曝光量': total_exposure,
                '素材数': creative_count,
                '投放国家数': country_count,
                '投放平台数': platform_count,
                '主要投放平台': main_platform
            })

        df = pd.DataFrame(rows)
        logger.info(f"✓ Sheet 1 生成完成, 共 {len(df)} 行")
        return df

    def generate_sheet2_lifecycle_summary(self, results: List[Dict]) -> pd.DataFrame:
        """
        生成 Sheet 2: 生命周期汇总 (近2年数据)

        列: 剧名, 总曝光量, 总素材数, 投放天数, 首次投放日期, 最后投放日期,
            生命周期(天), 覆盖国家总数, 投放平台总数

        Args:
            results: 所有短剧的搜索结果列表

        Returns:
            DataFrame
        """
        logger.info("生成 Sheet 2: 生命周期汇总...")

        rows = []

        for result in results:
            if not result.get('success'):
                continue

            drama_name = result.get('drama_name', '')
            aggregated = result.get('aggregated', {})
            window_2y = aggregated.get('window_2y', {})

            rows.append({
                '剧名': drama_name,
                '总曝光量': window_2y.get('total_exposure', 0),
                '总素材数': window_2y.get('creative_count', 0),
                '投放天数': window_2y.get('active_days_count', 0),
                '首次投放日期': window_2y.get('first_seen', ''),
                '最后投放日期': window_2y.get('last_seen', ''),
                '生命周期(天)': window_2y.get('lifecycle_days', 0),
                '覆盖国家总数': len(window_2y.get('countries', [])),
                '投放平台总数': len(window_2y.get('platforms', []))
            })

        df = pd.DataFrame(rows)
        logger.info(f"✓ Sheet 2 生成完成, 共 {len(df)} 行")
        return df

    def generate_sheet3_aggregated_analysis(self, results: List[Dict]) -> pd.DataFrame:
        """
        生成 Sheet 3: 聚合分析与排名

        列: 剧名, 近30天曝光量, 2年总曝光量, 生命周期(天), 热度得分,
            主要投放平台, 优先级建议

        Args:
            results: 所有短剧的搜索结果列表

        Returns:
            DataFrame
        """
        logger.info("生成 Sheet 3: 聚合分析与排名...")

        rows = []

        for result in results:
            if not result.get('success'):
                continue

            drama_name = result.get('drama_name', '')
            aggregated = result.get('aggregated', {})
            window_30d = aggregated.get('window_30d', {})
            window_2y = aggregated.get('window_2y', {})

            # 计算热度得分
            heat_score = self.calculate_heat_score(aggregated)

            # 确定优先级
            priority = self.determine_priority(aggregated, heat_score)

            # 主要投放平台
            publishers = window_2y.get('publishers', [])
            main_publishers = ', '.join(publishers[:5]) if publishers else '无数据'

            rows.append({
                '剧名': drama_name,
                '近30天曝光量': window_30d.get('total_exposure', 0),
                '2年总曝光量': window_2y.get('total_exposure', 0),
                '生命周期(天)': window_2y.get('lifecycle_days', 0),
                '热度得分': heat_score,
                '主要投放平台': main_publishers,
                '优先级建议': priority
            })

        # 按优先级和热度得分排序
        df = pd.DataFrame(rows)
        priority_order = {'S': 0, 'A': 1, 'B': 2, 'C': 3}
        df['_priority_order'] = df['优先级建议'].map(priority_order)
        df = df.sort_values(['_priority_order', '热度得分'], ascending=[True, False])
        df = df.drop('_priority_order', axis=1)

        logger.info(f"✓ Sheet 3 生成完成, 共 {len(df)} 行")
        return df

    def generate_report(self, results: List[Dict]) -> str:
        """
        生成完整的 Excel 报告

        Args:
            results: 所有短剧的搜索结果列表

        Returns:
            输出文件路径
        """
        logger.info("="*60)
        logger.info("开始生成 Excel 报告...")
        logger.info("="*60)

        try:
            # 生成三个 Sheet
            sheet1 = self.generate_sheet1_daily_data(results)
            sheet2 = self.generate_sheet2_lifecycle_summary(results)
            sheet3 = self.generate_sheet3_aggregated_analysis(results)

            # 写入 Excel 文件
            logger.info(f"写入 Excel 文件: {self.output_path}")
            with pd.ExcelWriter(self.output_path, engine='openpyxl') as writer:
                sheet1.to_excel(writer, sheet_name='近30天日数据', index=False)
                sheet2.to_excel(writer, sheet_name='生命周期汇总', index=False)
                sheet3.to_excel(writer, sheet_name='聚合分析与排名', index=False)

            logger.info("="*60)
            logger.info(f"✓ Excel 报告生成成功!")
            logger.info(f"✓ 文件路径: {self.output_path}")
            logger.info("="*60)

            return self.output_path

        except Exception as e:
            logger.error(f"生成 Excel 报告失败: {e}")
            raise
