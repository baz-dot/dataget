"""
XMP 数据播报脚本
从 BigQuery 读取 XMP 内部 API 数据，发送飞书播报
"""

import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery

BEIJING_TZ = timezone(timedelta(hours=8))


def query_xmp_summary(project_id: str = "fleet-blend-469520-n7") -> Dict[str, Any]:
    """查询 XMP 今日汇总数据"""
    client = bigquery.Client(project=project_id)

    query = """
    WITH latest_batch AS (
        SELECT MAX(batch_id) as batch_id
        FROM xmp_data.xmp_internal_campaigns
        WHERE stat_date = CURRENT_DATE('Asia/Shanghai')
    )
    SELECT
        channel,
        COUNT(DISTINCT campaign_id) as campaign_count,
        SUM(spend) as total_spend,
        SUM(revenue) as total_revenue,
        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks
    FROM xmp_data.xmp_internal_campaigns t
    JOIN latest_batch lb ON t.batch_id = lb.batch_id
    WHERE stat_date = CURRENT_DATE('Asia/Shanghai')
    GROUP BY channel
    ORDER BY total_spend DESC
    """

    result = client.query(query).result()

    channels = []
    total_spend = 0
    total_revenue = 0

    for row in result:
        channels.append({
            'channel': row.channel,
            'campaign_count': row.campaign_count,
            'spend': row.total_spend or 0,
            'revenue': row.total_revenue or 0,
            'impressions': row.total_impressions or 0,
            'clicks': row.total_clicks or 0,
        })
        total_spend += row.total_spend or 0
        total_revenue += row.total_revenue or 0

    return {
        'channels': channels,
        'total_spend': total_spend,
        'total_revenue': total_revenue,
        'roas': total_revenue / total_spend if total_spend > 0 else 0,
        'date': datetime.now(BEIJING_TZ).strftime('%Y-%m-%d'),
        'time': datetime.now(BEIJING_TZ).strftime('%H:%M'),
    }


def query_top_campaigns(project_id: str = "fleet-blend-469520-n7", limit: int = 10) -> List[Dict]:
    """查询消耗 Top N 的 campaign"""
    client = bigquery.Client(project=project_id)

    query = f"""
    WITH latest_batch AS (
        SELECT MAX(batch_id) as batch_id
        FROM xmp_data.xmp_internal_campaigns
        WHERE stat_date = CURRENT_DATE('Asia/Shanghai')
    )
    SELECT
        channel,
        campaign_name,
        country,
        spend,
        revenue,
        SAFE_DIVIDE(revenue, spend) as roas
    FROM xmp_data.xmp_internal_campaigns t
    JOIN latest_batch lb ON t.batch_id = lb.batch_id
    WHERE stat_date = CURRENT_DATE('Asia/Shanghai')
    ORDER BY spend DESC
    LIMIT {limit}
    """

    result = client.query(query).result()

    campaigns = []
    for row in result:
        campaigns.append({
            'channel': row.channel,
            'campaign_name': row.campaign_name[:30] + '...' if len(row.campaign_name or '') > 30 else row.campaign_name,
            'country': row.country,
            'spend': row.spend or 0,
            'revenue': row.revenue or 0,
            'roas': row.roas or 0,
        })

    return campaigns


def build_report_card(summary: Dict, top_campaigns: List[Dict]) -> Dict:
    """构建飞书卡片消息"""

    # ROAS 颜色
    roas = summary['roas']
    if roas >= 0.4:
        roas_color = "green"
        roas_icon = "🟢"
    elif roas >= 0.3:
        roas_color = "orange"
        roas_icon = "🟡"
    else:
        roas_color = "red"
        roas_icon = "🔴"

    # 渠道明细
    channel_lines = []
    for ch in summary['channels']:
        ch_roas = ch['revenue'] / ch['spend'] * 100 if ch['spend'] > 0 else 0
        channel_lines.append(
            f"**{ch['channel'].upper()}**: ${ch['spend']:,.0f} | 收入 ${ch['revenue']:,.0f} | ROAS {ch_roas:.1f}%"
        )

    # Top campaigns
    campaign_lines = []
    for i, c in enumerate(top_campaigns[:5], 1):
        c_roas = c['roas'] * 100 if c['roas'] else 0
        campaign_lines.append(
            f"{i}. [{c['channel']}] {c['country']} | ${c['spend']:,.0f} | {c_roas:.1f}%"
        )

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"📊 XMP 实时数据播报 ({summary['date']} {summary['time']})"
                },
                "template": roas_color
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**💰 总消耗**: ${summary['total_spend']:,.2f}\n**💵 总收入**: ${summary['total_revenue']:,.2f}\n**{roas_icon} 整体 ROAS**: {roas*100:.1f}%"
                    }
                },
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "**📱 渠道明细**\n" + "\n".join(channel_lines)
                    }
                },
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "**🔥 消耗 Top 5**\n" + "\n".join(campaign_lines)
                    }
                },
                {
                    "tag": "note",
                    "elements": [
                        {
                            "tag": "plain_text",
                            "content": f"数据来源: XMP 内部 API | 更新时间: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')}"
                        }
                    ]
                }
            ]
        }
    }

    return card


def send_to_lark(webhook_url: str, card: Dict) -> bool:
    """发送消息到飞书"""
    try:
        resp = requests.post(webhook_url, json=card, timeout=10)
        if resp.status_code == 200:
            result = resp.json()
            if result.get('code') == 0:
                print(f"[Lark] 发送成功")
                return True
            else:
                print(f"[Lark] 发送失败: {result}")
        else:
            print(f"[Lark] HTTP 错误: {resp.status_code}")
    except Exception as e:
        print(f"[Lark] 发送异常: {e}")
    return False


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='XMP 数据播报')
    parser.add_argument('--webhook', help='飞书 Webhook URL',
                        default='https://open.larksuite.com/open-apis/bot/v2/hook/03f0693e-a0f3-424f-b3ca-a4248f886998')
    args = parser.parse_args()

    print("[XMP Report] 查询数据...")
    summary = query_xmp_summary()
    top_campaigns = query_top_campaigns()

    print(f"[XMP Report] 总消耗: ${summary['total_spend']:,.2f}")
    print(f"[XMP Report] 总收入: ${summary['total_revenue']:,.2f}")
    print(f"[XMP Report] ROAS: {summary['roas']*100:.1f}%")

    print("[XMP Report] 构建消息...")
    card = build_report_card(summary, top_campaigns)

    print("[XMP Report] 发送到飞书...")
    send_to_lark(args.webhook, card)


if __name__ == '__main__':
    main()
