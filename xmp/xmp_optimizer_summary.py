"""
XMP 投手数据抓取 - 使用 channel/summary API
按投手名搜索，获取 TikTok 和 Facebook 渠道的汇总数据
"""
import os
import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))

# XMP API
XMP_SUMMARY_URL = "https://xmp-api.mobvista.com/admanage/channel/summary"

# 投手列表
OPTIMIZER_LIST = [
    "echo", "felix", "hannibal", "kimi", "kino", "silas", "zane"
]

# 渠道配置
CHANNELS = ["tiktok", "facebook"]


class XMPOptimizerScraper:
    """XMP 投手数据抓取器"""

    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token

    async def fetch_optimizer_summary(
        self,
        optimizer: str,
        channel: str,
        date_str: str
    ) -> Optional[Dict]:
        """
        获取单个投手在指定渠道的汇总数据

        Args:
            optimizer: 投手名
            channel: 渠道 (tiktok/facebook)
            date_str: 日期 YYYY-MM-DD

        Returns:
            汇总数据 dict
        """
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
            "Origin": "https://xmp.mobvista.com",
            "Referer": "https://xmp.mobvista.com/"
        }

        # 构建请求
        payload = {
            "level": "campaign",
            "channel": channel,
            "start_time": date_str,
            "end_time": date_str,
            "field": "cost,purchase_value",
            "page": 1,
            "page_size": 1000,
            "report_timezone": "",
            "search": [
                {"item": "campaign", "val": optimizer, "op": "LIKE", "op_type": "OR"}
            ]
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    XMP_SUMMARY_URL,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    result = await response.json()

                    if result.get('code') != 0:
                        print(f"[XMP] API 错误: {result.get('msg')}")
                        return None

                    sum_data = result.get('data', {}).get('sum', {})

                    cost = float(sum_data.get('cost', 0) or 0)
                    revenue = float(sum_data.get('purchase_value', 0) or 0)

                    return {
                        'optimizer': optimizer,
                        'channel': channel,
                        'date': date_str,
                        'cost': cost,
                        'revenue': revenue,
                        'roas': revenue / cost if cost > 0 else 0
                    }

        except Exception as e:
            print(f"[XMP] 请求失败 ({optimizer}/{channel}): {e}")
            return None

    async def fetch_all_optimizers(self, date_str: str) -> List[Dict]:
        """
        获取所有投手在所有渠道的数据

        Args:
            date_str: 日期 YYYY-MM-DD

        Returns:
            投手数据列表
        """
        results = []

        for optimizer in OPTIMIZER_LIST:
            optimizer_data = {
                'optimizer': optimizer,
                'date': date_str,
                'tt_cost': 0,
                'tt_revenue': 0,
                'tt_roas': 0,
                'meta_cost': 0,
                'meta_revenue': 0,
                'meta_roas': 0,
                'total_cost': 0,
                'total_revenue': 0,
                'total_roas': 0
            }

            for channel in CHANNELS:
                data = await self.fetch_optimizer_summary(optimizer, channel, date_str)

                if data:
                    if channel == 'tiktok':
                        optimizer_data['tt_cost'] = data['cost']
                        optimizer_data['tt_revenue'] = data['revenue']
                        optimizer_data['tt_roas'] = data['roas']
                    else:  # facebook
                        optimizer_data['meta_cost'] = data['cost']
                        optimizer_data['meta_revenue'] = data['revenue']
                        optimizer_data['meta_roas'] = data['roas']

            # 计算总计
            optimizer_data['total_cost'] = optimizer_data['tt_cost'] + optimizer_data['meta_cost']
            optimizer_data['total_revenue'] = optimizer_data['tt_revenue'] + optimizer_data['meta_revenue']
            if optimizer_data['total_cost'] > 0:
                optimizer_data['total_roas'] = optimizer_data['total_revenue'] / optimizer_data['total_cost']

            results.append(optimizer_data)
            print(f"[XMP] {optimizer}: TT ${optimizer_data['tt_cost']:,.2f}, Meta ${optimizer_data['meta_cost']:,.2f}")

        return results


async def fetch_optimizer_data(date_str: str, bearer_token: str) -> List[Dict]:
    """获取指定日期的投手数据"""
    scraper = XMPOptimizerScraper(bearer_token)
    return await scraper.fetch_all_optimizers(date_str)


def upload_to_bigquery(data: List[Dict], date_str: str):
    """上传数据到 BigQuery"""
    from bigquery_storage import BigQueryUploader

    project_id = os.getenv('BQ_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET_ID', 'xmp_data')

    if not project_id:
        print("[BQ] 未配置 BQ_PROJECT_ID")
        return

    batch_id = datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M%S')
    uploader = BigQueryUploader(project_id, dataset_id)

    # 转换为 xmp_optimizer_stats 格式
    rows = []
    for d in data:
        rows.append({
            'stat_date': d['date'],
            'optimizer': d['optimizer'],
            'tt_spend': d['tt_cost'],
            'tt_revenue': d['tt_revenue'],
            'tt_roas': d['tt_roas'],
            'meta_spend': d['meta_cost'],
            'meta_revenue': d['meta_revenue'],
            'meta_roas': d['meta_roas'],
            'total_spend': d['total_cost'],
            'total_revenue': d['total_revenue'],
            'total_roas': d['total_roas'],
            'batch_id': batch_id
        })

    # 需要在 BigQueryUploader 中添加 upload_optimizer_stats 方法
    # 暂时打印数据
    print(f"[BQ] 准备上传 {len(rows)} 条投手数据")
    for row in rows:
        print(f"  {row['optimizer']}: TT ${row['tt_spend']:,.2f} ({row['tt_roas']:.1%}), "
              f"Meta ${row['meta_spend']:,.2f} ({row['meta_roas']:.1%})")


async def main():
    import argparse

    parser = argparse.ArgumentParser(description='XMP 投手数据抓取')
    parser.add_argument('--date', help='日期 YYYY-MM-DD，默认昨天')
    parser.add_argument('--token', help='Bearer Token')
    parser.add_argument('--upload', action='store_true', help='上传到 BigQuery')
    args = parser.parse_args()

    # 日期
    if args.date:
        date_str = args.date
    else:
        date_str = (datetime.now(BEIJING_TZ) - timedelta(days=1)).strftime('%Y-%m-%d')

    # Token
    bearer_token = args.token or os.getenv('XMP_BEARER_TOKEN')
    if not bearer_token:
        print("[XMP] 请提供 Bearer Token (--token 或 XMP_BEARER_TOKEN 环境变量)")
        return

    print(f"=" * 60)
    print(f"[XMP] 抓取投手数据: {date_str}")
    print(f"=" * 60)

    data = await fetch_optimizer_data(date_str, bearer_token)

    print(f"\n{'=' * 60}")
    print(f"[XMP] 抓取完成，共 {len(data)} 个投手")
    print(f"{'=' * 60}")

    # 打印汇总
    total_cost = sum(d['total_cost'] for d in data)
    total_revenue = sum(d['total_revenue'] for d in data)
    total_roas = total_revenue / total_cost if total_cost > 0 else 0

    print(f"\n总消耗: ${total_cost:,.2f}")
    print(f"总收入: ${total_revenue:,.2f}")
    print(f"总 ROAS: {total_roas:.1%}")

    if args.upload:
        upload_to_bigquery(data, date_str)


if __name__ == '__main__':
    asyncio.run(main())
