"""
XMP 广告系列报表数据拉取脚本
从 XMP API 获取 campaign 级别数据，解析 optimizer/drama_id，存入 BigQuery
"""

import os
import sys
import re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from xmp.xmp_api import XMPApiClient
from config.drama_mapping import get_drama_name

load_dotenv()

BEIJING_TZ = timezone(timedelta(hours=8))


def parse_campaign_name(campaign_name: str) -> Dict[str, str]:
    """
    解析 campaign_name 提取 optimizer, drama_id 等字段

    示例: app-vigloo_channel-fb_path-w2a_optimizer-hannibal_date-1227_dramaid-15000728_extra-purchase.kr.marriagelife
    """
    result = {
        'optimizer': None,
        'drama_id': None,
        'channel_code': None,
        'path': None,
    }

    if not campaign_name:
        return result

    # 解析 optimizer-xxx
    optimizer_match = re.search(r'optimizer-([^_]+)', campaign_name)
    if optimizer_match:
        result['optimizer'] = optimizer_match.group(1)

    # 解析 dramaid-xxx
    drama_match = re.search(r'dramaid-(\d+)', campaign_name)
    if drama_match:
        result['drama_id'] = drama_match.group(1)

    # 解析 channel-xxx
    channel_match = re.search(r'channel-([^_]+)', campaign_name)
    if channel_match:
        result['channel_code'] = channel_match.group(1)

    # 解析 path-xxx
    path_match = re.search(r'path-([^_]+)', campaign_name)
    if path_match:
        result['path'] = path_match.group(1)

    return result


class XMPCampaignReporter:
    """XMP 广告系列报表拉取器"""

    # 完整指标列表
    METRICS = [
        "cost",
        "impression",
        "click",
        "conversion",
        "total_purchase_value",
        "active_pay",
        "cpm",
        "cpc",
        "ctr",
        "cvr",
        "cpi",
    ]

    # 维度
    DIMENSIONS = [
        "date",
        "campaign_id",
        "campaign_name",
        "geo",
        "account_name",
    ]

    def __init__(self):
        self.client = XMPApiClient()
        self.account_owner_map = {}  # account_id -> owner_user

    def _load_account_owners(self):
        """加载账户所属人映射"""
        print("[XMP] 加载账户所属人映射...")
        channels = ['facebook', 'google', 'tiktok', 'applovin']

        for channel in channels:
            try:
                accounts = self.client.get_account_list(channel=[channel])
                for acc in accounts:
                    acc_id = acc.get('account_id')
                    owner = acc.get('owner_user', {})
                    if acc_id and owner:
                        self.account_owner_map[acc_id] = owner.get('user_name')
            except Exception as e:
                print(f"[XMP] 加载 {channel} 账户失败: {e}")

        print(f"[XMP] 已加载 {len(self.account_owner_map)} 个账户映射")

    def fetch_campaign_report(self, date: str) -> List[Dict]:
        """
        拉取指定日期的 campaign 报表数据

        Args:
            date: 日期 YYYY-MM-DD

        Returns:
            标准化后的数据列表
        """
        print(f"[XMP] 拉取 campaign 报表: {date}")

        # 拉取原始数据
        raw_data = self.client.fetch_report_data(
            start_date=date,
            end_date=date,
            metrics=self.METRICS,
            dimension=self.DIMENSIONS,
            currency='USD',
            page_size=500
        )

        if not raw_data:
            print("[XMP] 无数据")
            return []

        # 标准化数据
        result = []
        for row in raw_data:
            parsed = parse_campaign_name(row.get('campaign_name', ''))

            # 优先从 campaign_name 解析 optimizer，否则从账户映射获取
            optimizer = parsed['optimizer']
            if not optimizer:
                acc_id = row.get('account_id')
                optimizer = self.account_owner_map.get(acc_id)

            # 通过 drama_id 获取 drama_name
            drama_id = parsed['drama_id']
            drama_name = get_drama_name(drama_id)

            record = {
                'stat_date': row.get('date'),
                'country': row.get('geo'),
                'channel': row.get('module'),
                'account_id': row.get('account_id'),
                'account_name': row.get('account_name'),
                'campaign_id': row.get('campaign_id'),
                'campaign_name': row.get('campaign_name'),
                'optimizer': optimizer,
                'drama_id': drama_id,
                'drama_name': drama_name,
                'impressions': float(row.get('impression') or 0),
                'clicks': float(row.get('click') or 0),
                'spend': float(row.get('cost') or 0),
                'conversions': int(row.get('conversion') or 0),
                'revenue': float(row.get('total_purchase_value') or 0),
                'payers': int(row.get('active_pay') or 0),
                'cpm': float(row.get('cpm') or 0),
                'cpc': float(row.get('cpc') or 0),
                'ctr': float(row.get('ctr') or 0),
                'cvr': float(row.get('cvr') or 0),
                'cpi': float(row.get('cpi') or 0),
                'timezone': row.get('timezone'),
            }

            # 计算 ROAS
            if record['spend'] > 0:
                record['roas'] = record['revenue'] / record['spend']
            else:
                record['roas'] = 0

            result.append(record)

        print(f"[XMP] 标准化完成: {len(result)} 条记录")
        return result

    def fetch_today(self) -> List[Dict]:
        """拉取今日数据"""
        today = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')
        return self.fetch_campaign_report(today)

    def fetch_yesterday(self) -> List[Dict]:
        """拉取昨日数据"""
        yesterday = (datetime.now(BEIJING_TZ) - timedelta(days=1)).strftime('%Y-%m-%d')
        return self.fetch_campaign_report(yesterday)


def upload_to_bigquery(data: List[Dict], batch_id: str = None):
    """上传数据到 BigQuery"""
    from bigquery_storage import BigQueryUploader

    project_id = os.getenv('BQ_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET_ID', 'xmp_data')

    if not project_id:
        print("[BQ] 未配置 BQ_PROJECT_ID")
        return 0

    uploader = BigQueryUploader(project_id, dataset_id)
    count = uploader.upload_xmp_campaigns(data, batch_id=batch_id)
    return count


def main():
    """主函数：拉取数据并上传到 BigQuery"""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--date', help='日期 YYYY-MM-DD，默认昨天')
    parser.add_argument('--upload', action='store_true', help='上传到 BigQuery')
    args = parser.parse_args()

    reporter = XMPCampaignReporter()

    # 确定日期
    if args.date:
        date = args.date
    else:
        date = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')

    # 拉取数据
    data = reporter.fetch_campaign_report(date)

    if not data:
        print("无数据")
        return

    # 汇总
    total_spend = sum(r['spend'] for r in data)
    total_revenue = sum(r['revenue'] for r in data)
    print(f"\n=== {date} 汇总 ===")
    print(f"记录数: {len(data)}")
    print(f"总消耗: ${total_spend:,.2f}")
    print(f"总收入: ${total_revenue:,.2f}")
    if total_spend > 0:
        print(f"ROAS: {total_revenue/total_spend*100:.1f}%")

    # 上传到 BigQuery
    if args.upload:
        batch_id = datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M%S')
        count = upload_to_bigquery(data, batch_id)
        print(f"\n[BQ] 已上传 {count} 条记录，batch_id: {batch_id}")


if __name__ == '__main__':
    main()
