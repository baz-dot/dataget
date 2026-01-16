"""
XMP 投手/剪辑师报表 Excel 导出工具

从 BigQuery 查询数据，生成日报和周报 Excel 文件。
本地运行，不部署到云端。

使用方法:
    # 生成日报
    python xmp_report_excel.py --date 2026-01-12

    # 生成周报 (默认7天)
    python xmp_report_excel.py --date 2026-01-12 --weekly

    # 生成周报 (指定天数)
    python xmp_report_excel.py --date 2026-01-12 --weekly --days 14
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# 添加父目录到 path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# 剪辑师名单配置 (中文名 -> 可能的别名/英文名/姓氏)
EDITOR_NAME_MAP = {
    "谢奕俊": ["eason", "Xie", "Yijun", "谢"],
    "樊凯翱": ["kyrie", "Fan", "Kaiao", "樊"],
    "吴泽鑫": ["beita", "Wu", "Zexin", "吴"],
    "宋涵妍": ["helen", "Song", "Hanyan", "宋"],
    "聂佳欢": ["maggie", "Nie", "Jiahuan", "聂"],
    "许丹晨": ["dancey", "Xu", "Danchen", "许"],
    "李文政": ["curry", "Li", "Wenzheng", "李文政"],
    "邓玮": ["dorris", "Deng", "Wei", "邓"],
    "王俊喜": ["ethan", "Wang", "Junxi", "王"],
}

# 反向映射: 所有可能的名字 -> 标准中文名
EDITOR_ALIAS_MAP = {}
for cn_name, aliases in EDITOR_NAME_MAP.items():
    EDITOR_ALIAS_MAP[cn_name] = cn_name
    EDITOR_ALIAS_MAP[cn_name.lower()] = cn_name
    for alias in aliases:
        EDITOR_ALIAS_MAP[alias] = cn_name
        EDITOR_ALIAS_MAP[alias.lower()] = cn_name


class XMPReportExporter:
    """XMP 报表导出器"""

    def __init__(self):
        self.project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
        self.dataset_id = os.getenv('XMP_DATASET_ID', 'xmp_data')
        self.client = bigquery.Client(project=self.project_id)

    # 排除的投手 (韩国人 + eason是剪辑师)
    EXCLUDED_OPTIMIZERS = ['lyla', 'juria', 'jade', 'eason']

    def query_optimizer_stats(self, start_date: str, end_date: str) -> List[Dict]:
        """查询投手统计数据 (每天取最新 batch)"""
        excluded = "', '".join(self.EXCLUDED_OPTIMIZERS)
        query = f"""
        WITH latest_batch_per_day AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{self.project_id}.{self.dataset_id}.xmp_optimizer_stats`
            WHERE stat_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY stat_date
        )
        SELECT
            t.stat_date,
            t.channel,
            t.optimizer_name,
            t.campaign_count,
            t.spend,
            t.revenue,
            t.roas,
            t.top_campaign,
            t.top_campaign_spend,
            t.top_campaign_roas
        FROM `{self.project_id}.{self.dataset_id}.xmp_optimizer_stats` t
        JOIN latest_batch_per_day lb ON t.stat_date = lb.stat_date AND t.batch_id = lb.batch_id
        WHERE LOWER(t.optimizer_name) NOT IN ('{excluded}')
        ORDER BY t.stat_date, t.spend DESC
        """

        result = self.client.query(query).result()
        return [dict(row) for row in result]

    def query_editor_stats(self, start_date: str, end_date: str) -> List[Dict]:
        """
        从素材表获取剪辑师统计
        注意: XMP 素材报表 API 不返回渠道信息，所以剪辑师数据只有总计消耗，无 revenue/ROAS
        """
        query = f"""
        SELECT
            designer_name,
            SUM(cost) as spend,
            SUM(impression) as impressions,
            SUM(click) as clicks
        FROM `{self.project_id}.{self.dataset_id}.xmp_materials`
        WHERE cost > 0
          AND stat_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY designer_name
        ORDER BY spend DESC
        """

        result = self.client.query(query).result()

        editor_data = {}
        for row in result:
            designer = row.designer_name
            if not designer:
                continue

            editor = self._normalize_editor_name(designer)
            if not editor:
                continue

            spend = float(row.spend or 0)

            if editor not in editor_data:
                editor_data[editor] = {
                    'editor_name': editor,
                    'spend': 0,
                    'impressions': 0,
                    'clicks': 0,
                }
            editor_data[editor]['spend'] += spend
            editor_data[editor]['impressions'] += float(row.impressions or 0)
            editor_data[editor]['clicks'] += float(row.clicks or 0)

        # 计算 CTR
        for editor, data in editor_data.items():
            impressions = data['impressions']
            clicks = data['clicks']
            data['ctr'] = clicks / impressions if impressions > 0 else 0

        result_list = list(editor_data.values())
        result_list.sort(key=lambda x: x['spend'], reverse=True)
        return result_list

    def _normalize_editor_name(self, name: str) -> Optional[str]:
        """标准化剪辑师名称为中文名"""
        if not name:
            return None
        # 直接匹配中文名
        for cn_name in EDITOR_NAME_MAP.keys():
            if cn_name in name or name in cn_name:
                return cn_name
        # 匹配别名
        name_lower = name.lower()
        for alias, cn_name in EDITOR_ALIAS_MAP.items():
            if alias.lower() == name_lower or alias.lower() in name_lower:
                return cn_name
        return None

    def _extract_editor_from_name(self, name: str) -> Optional[str]:
        """从 campaign_name 中提取剪辑师名"""
        if not name:
            return None
        # 先匹配中文名
        for cn_name in EDITOR_NAME_MAP.keys():
            if cn_name in name:
                return cn_name
        # 再匹配别名 (英文名/姓氏)
        name_lower = name.lower()
        for alias, cn_name in EDITOR_ALIAS_MAP.items():
            if alias.lower() in name_lower:
                return cn_name
        return None

    def _merge_by_name(self, stats: List[Dict], name_field: str) -> List[Dict]:
        """按名称合并 Meta + TikTok 数据"""
        merged = {}
        for s in stats:
            name = s.get(name_field)
            if not name:
                continue
            if name not in merged:
                merged[name] = {
                    'name': name,
                    'meta_spend': 0, 'meta_revenue': 0,
                    'tt_spend': 0, 'tt_revenue': 0,
                }
            channel = s.get('channel', '')
            spend = float(s.get('spend', 0) or 0)
            revenue = float(s.get('revenue', 0) or 0)

            if channel == 'facebook':
                merged[name]['meta_spend'] += spend
                merged[name]['meta_revenue'] += revenue
            elif channel == 'tiktok':
                merged[name]['tt_spend'] += spend
                merged[name]['tt_revenue'] += revenue
            elif channel == 'all':
                # 素材报表不区分渠道，直接加到总计
                merged[name]['all_spend'] = merged[name].get('all_spend', 0) + spend
                merged[name]['all_revenue'] = merged[name].get('all_revenue', 0) + revenue

        result = []
        for name, d in merged.items():
            all_spend = d.get('all_spend', 0)
            all_revenue = d.get('all_revenue', 0)
            total_spend = d['meta_spend'] + d['tt_spend'] + all_spend
            total_revenue = d['meta_revenue'] + d['tt_revenue'] + all_revenue
            result.append({
                'name': name,
                'meta_spend': d['meta_spend'],
                'meta_revenue': d['meta_revenue'],
                'meta_roas': d['meta_revenue'] / d['meta_spend'] if d['meta_spend'] > 0 else 0,
                'tt_spend': d['tt_spend'],
                'tt_revenue': d['tt_revenue'],
                'tt_roas': d['tt_revenue'] / d['tt_spend'] if d['tt_spend'] > 0 else 0,
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'total_roas': total_revenue / total_spend if total_spend > 0 else 0,
            })
        result.sort(key=lambda x: x['total_spend'], reverse=True)
        return result

    def _add_labels(self, data: List[Dict], min_spend: float = 100) -> List[Dict]:
        """添加 Spend/ROAS Top1 标注"""
        if not data:
            return data

        # Spend 第一
        spend_first = max(data, key=lambda x: x['total_spend'])

        # ROAS 第一 (消耗 >= min_spend 才参与)
        qualified = [d for d in data if d['total_spend'] >= min_spend]
        roas_first = max(qualified, key=lambda x: x['total_roas']) if qualified else None

        for d in data:
            labels = []
            if d == spend_first:
                labels.append('Spend Top1')
            if d == roas_first:
                labels.append('ROAS Top1')
            d['label'] = ' | '.join(labels) if labels else ''

        return data

    def _find_best_performer(self, data: List[Dict], min_spend: float = 1000) -> Optional[Dict]:
        """找出最佳表现者 (综合 Spend 和 ROAS 排名)"""
        qualified = [d for d in data if d['total_spend'] >= min_spend]
        if not qualified:
            return None

        # 按 Spend 排名
        spend_sorted = sorted(qualified, key=lambda x: x['total_spend'], reverse=True)
        for i, s in enumerate(spend_sorted):
            s['spend_rank'] = i + 1

        # 按 ROAS 排名
        roas_sorted = sorted(qualified, key=lambda x: x['total_roas'], reverse=True)
        for i, s in enumerate(roas_sorted):
            s['roas_rank'] = i + 1

        # 综合评分
        for s in qualified:
            s['combined_score'] = s['spend_rank'] + s['roas_rank']

        best = min(qualified, key=lambda x: x['combined_score'])
        return best

    def export_daily_report(self, date_str: str, output_path: str = None) -> str:
        """
        导出日报 Excel

        Args:
            date_str: 日期 YYYY-MM-DD
            output_path: 输出路径，默认当前目录

        Returns:
            生成的文件路径
        """
        try:
            import pandas as pd
        except ImportError:
            print("[错误] 需要安装 pandas: pip install pandas openpyxl")
            return None

        print(f"[日报] 查询 {date_str} 数据...")

        # 查询数据
        optimizer_stats = self.query_optimizer_stats(date_str, date_str)
        editor_stats = self.query_editor_stats(date_str, date_str)

        if not optimizer_stats and not editor_stats:
            print(f"[日报] {date_str} 没有数据")
            return None

        # 投手数据: 合并 Meta + TikTok 并添加标注
        opt_merged = self._add_labels(self._merge_by_name(optimizer_stats, 'optimizer_name'))

        # 生成文件名
        filename = output_path or f"xmp_daily_{date_str.replace('-', '')}.xlsx"

        # 写入 Excel
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 投手日报 (有渠道分组和 ROAS)
            if opt_merged:
                opt_df = pd.DataFrame(opt_merged)
                opt_df.columns = ['投手', 'Meta Spend', 'Meta Revenue', 'Meta ROAS',
                                  'TT Spend', 'TT Revenue', 'TT ROAS',
                                  '总 Spend', '总 Revenue', '总 ROAS', '标注']
                for col in ['Meta ROAS', 'TT ROAS', '总 ROAS']:
                    opt_df[col] = opt_df[col].apply(lambda x: f"{x*100:.1f}%")
                opt_df.to_excel(writer, sheet_name='投手日报', index=False)

            # 剪辑师日报 (只有总消耗，无渠道分组，无 ROAS)
            if editor_stats:
                ed_df = pd.DataFrame(editor_stats)
                ed_df = ed_df[['editor_name', 'spend', 'impressions', 'clicks', 'ctr']]
                ed_df.columns = ['剪辑师', '消耗 ($)', '展示', '点击', 'CTR']
                ed_df['CTR'] = ed_df['CTR'].apply(lambda x: f"{x*100:.2f}%")
                ed_df.to_excel(writer, sheet_name='剪辑师日报', index=False)

        print(f"[日报] 已导出: {filename}")
        print(f"  - 投手: {len(opt_merged)} 人")
        print(f"  - 剪辑师: {len(editor_stats)} 人")
        return filename

    def export_weekly_report(self, end_date: str, days: int = 7, output_path: str = None) -> str:
        """
        导出周报 Excel

        Args:
            end_date: 结束日期 YYYY-MM-DD
            days: 天数，默认7天
            output_path: 输出路径

        Returns:
            生成的文件路径
        """
        try:
            import pandas as pd
        except ImportError:
            print("[错误] 需要安装 pandas: pip install pandas openpyxl")
            return None

        # 计算日期范围
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        start_dt = end_dt - timedelta(days=days - 1)
        start_date = start_dt.strftime('%Y-%m-%d')

        print(f"[周报] 查询 {start_date} ~ {end_date} 数据...")

        # 查询数据
        optimizer_stats = self.query_optimizer_stats(start_date, end_date)
        editor_stats = self.query_editor_stats(start_date, end_date)

        if not optimizer_stats and not editor_stats:
            print(f"[周报] {start_date} ~ {end_date} 没有数据")
            return None

        # 投手数据: 合并 Meta + TikTok 并添加标注
        opt_merged = self._add_labels(
            self._merge_by_name(optimizer_stats, 'optimizer_name'),
            min_spend=500  # 周报提高门槛
        )

        # 找最佳投手
        best_opt = self._find_best_performer(opt_merged, min_spend=1000)

        # 生成文件名
        filename = output_path or f"xmp_weekly_{start_date.replace('-', '')}_to_{end_date.replace('-', '')}.xlsx"

        # 写入 Excel
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 投手周报 (有渠道分组和 ROAS)
            if opt_merged:
                self._write_weekly_sheet(writer, opt_merged, '投手周报', '投手')

            # 剪辑师周报 (只有总消耗，无渠道分组，无 ROAS)
            if editor_stats:
                ed_df = pd.DataFrame(editor_stats)
                ed_df = ed_df[['editor_name', 'spend', 'impressions', 'clicks', 'ctr']]
                ed_df.columns = ['剪辑师', '消耗 ($)', '展示', '点击', 'CTR']
                ed_df['CTR'] = ed_df['CTR'].apply(lambda x: f"{x*100:.2f}%")
                ed_df.to_excel(writer, sheet_name='剪辑师周报', index=False)

            # 周报汇总
            self._write_summary_sheet(
                writer, opt_merged, editor_stats,
                best_opt, None,
                start_date, end_date
            )

        print(f"[周报] 已导出: {filename}")
        print(f"  - 投手: {len(opt_merged)} 人")
        print(f"  - 剪辑师: {len(editor_stats)} 人")
        if best_opt:
            print(f"  - 最佳投手: {best_opt['name']}")
        return filename

    def _write_weekly_sheet(self, writer, data: List[Dict], sheet_name: str, role_name: str):
        """写入周报明细 Sheet"""
        import pandas as pd

        df = pd.DataFrame(data)
        df = df[['name', 'meta_spend', 'meta_revenue', 'meta_roas',
                 'tt_spend', 'tt_revenue', 'tt_roas',
                 'total_spend', 'total_revenue', 'total_roas', 'label']]
        df.columns = [role_name, 'Meta Spend', 'Meta Revenue', 'Meta ROAS',
                      'TT Spend', 'TT Revenue', 'TT ROAS',
                      '总 Spend', '总 Revenue', '总 ROAS', '标注']

        # 格式化 ROAS
        for col in ['Meta ROAS', 'TT ROAS', '总 ROAS']:
            df[col] = df[col].apply(lambda x: f"{x*100:.1f}%")

        df.to_excel(writer, sheet_name=sheet_name, index=False)

    def _write_summary_sheet(self, writer, opt_data: List[Dict], editor_data: List[Dict],
                             best_opt: Optional[Dict], best_editor: Optional[Dict],
                             start_date: str, end_date: str):
        """写入周报汇总 Sheet"""
        import pandas as pd

        # 计算汇总数据
        opt_total_spend = sum(d['total_spend'] for d in opt_data)
        opt_total_revenue = sum(d['total_revenue'] for d in opt_data)
        opt_avg_roas = opt_total_revenue / opt_total_spend if opt_total_spend > 0 else 0

        editor_total_spend = sum(d['total_spend'] for d in editor_data)
        editor_total_revenue = sum(d['total_revenue'] for d in editor_data)
        editor_avg_roas = editor_total_revenue / editor_total_spend if editor_total_spend > 0 else 0

        # 构建汇总表
        summary_rows = [
            ['周报周期', f'{start_date} ~ {end_date}'],
            ['', ''],
            ['【投手汇总】', ''],
            ['投手人数', len(opt_data)],
            ['总消耗', f'${opt_total_spend:,.0f}'],
            ['总收入', f'${opt_total_revenue:,.0f}'],
            ['平均 ROAS', f'{opt_avg_roas*100:.1f}%'],
            ['', ''],
        ]

        if best_opt:
            summary_rows.extend([
                ['最佳投手', best_opt['name']],
                ['  - Spend', f"${best_opt['total_spend']:,.0f} (排名 #{best_opt.get('spend_rank', '-')})"],
                ['  - ROAS', f"{best_opt['total_roas']*100:.1f}% (排名 #{best_opt.get('roas_rank', '-')})"],
                ['  - 综合评分', best_opt.get('combined_score', '-')],
                ['', ''],
            ])

        summary_rows.extend([
            ['【剪辑师汇总】', ''],
            ['剪辑师人数', len(editor_data)],
            ['总消耗', f'${editor_total_spend:,.0f}'],
            ['总收入', f'${editor_total_revenue:,.0f}'],
            ['平均 ROAS', f'{editor_avg_roas*100:.1f}%'],
            ['', ''],
        ])

        if best_editor:
            summary_rows.extend([
                ['最佳剪辑师', best_editor['name']],
                ['  - Spend', f"${best_editor['total_spend']:,.0f} (排名 #{best_editor.get('spend_rank', '-')})"],
                ['  - ROAS', f"{best_editor['total_roas']*100:.1f}% (排名 #{best_editor.get('roas_rank', '-')})"],
                ['  - 综合评分', best_editor.get('combined_score', '-')],
            ])

        df = pd.DataFrame(summary_rows, columns=['项目', '数值'])
        df.to_excel(writer, sheet_name='周报汇总', index=False)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='XMP 投手/剪辑师报表 Excel 导出工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 生成昨日日报
  python xmp_report_excel.py

  # 生成指定日期日报
  python xmp_report_excel.py --date 2026-01-12

  # 生成周报 (默认7天)
  python xmp_report_excel.py --weekly

  # 生成14天周报
  python xmp_report_excel.py --weekly --days 14
        """
    )

    parser.add_argument('--date', help='日期 YYYY-MM-DD，默认昨天')
    parser.add_argument('--weekly', action='store_true', help='生成周报')
    parser.add_argument('--days', type=int, default=7, help='周报天数，默认7天')
    parser.add_argument('--output', '-o', help='输出文件路径')

    args = parser.parse_args()

    # 默认日期为昨天
    if args.date:
        date_str = args.date
    else:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    exporter = XMPReportExporter()

    if args.weekly:
        exporter.export_weekly_report(date_str, args.days, args.output)
    else:
        exporter.export_daily_report(date_str, args.output)


if __name__ == '__main__':
    main()
