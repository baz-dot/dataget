"""
上传 1月5-18号 投手和剪辑师日报到飞书文档
"""
import asyncio
import sys
import os
sys.path.insert(0, '.')

from datetime import datetime, timedelta
from dotenv import load_dotenv
from xmp.xmp_scheduler import (
    fetch_optimizer_summary_stats,
    fetch_editor_combined_stats
)
from lark.lark_doc_client import create_doc_client

load_dotenv()

# Bearer Token
BEARER_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NjkxMjY3MjEsImV4cCI6MTc3MDQyMjcyMSwibmJmIjoxNzY5MTI2NzIxLCJkYXRhIjp7ImlkIjoiMjk0ODMifX0.YvfilplE9NpuPjxHkXJR1S3xanseUswdLR0f0BAORLw"

# 飞书 Wiki token
# 格式: https://xxx.larksuite.com/wiki/WIKI_TOKEN
WIKI_TOKEN = "Gr4JwwMB4iqOpUkJSaQlCiPigAd"

# 日期范围
START_DATE = "2026-01-05"
END_DATE = "2026-01-18"


def add_labels(data_list, spend_key='total_spend', roas_key='total_roas'):
    """添加 ROAS Top1 和 Spend Top1 标注"""
    if not data_list:
        return data_list

    # 找出最大值
    max_spend = max(d.get(spend_key, 0) for d in data_list)
    max_roas = max(d.get(roas_key, 0) for d in data_list)

    for d in data_list:
        parts = []
        if d.get(spend_key, 0) == max_spend and max_spend > 0:
            parts.append('Spend Top1')
        if d.get(roas_key, 0) == max_roas and max_roas > 0:
            parts.append('ROAS Top1')
        d['label'] = ', '.join(parts) if parts else ''

    return data_list


def convert_optimizer_data(data):
    """转换投手数据格式"""
    result = []
    for d in data:
        result.append({
            'name': d.get('name', ''),
            'meta_spend': d.get('facebook_cost', 0),
            'meta_roas': d.get('facebook_roas', 0),
            'tt_spend': d.get('tiktok_cost', 0),
            'tt_roas': d.get('tiktok_roas', 0),
            'total_spend': d.get('total_cost', 0),
            'total_roas': d.get('roas', 0),
            'label': ''
        })
    return add_labels(result)


def convert_editor_data(data):
    """转换剪辑师数据格式"""
    result = []
    for d in data:
        result.append({
            'name': d.get('editor_name', ''),
            'meta_spend': d.get('meta_spend', 0),
            'meta_roas': d.get('meta_roas', 0),
            'tt_spend': d.get('tt_spend', 0),
            'tt_roas': d.get('tt_roas', 0),
            'total_spend': d.get('total_spend', 0),
            'total_roas': d.get('total_roas', 0),
            'label': ''
        })
    return add_labels(result)


async def main():
    # 创建飞书文档客户端
    try:
        client = create_doc_client()
    except ValueError as e:
        print(f"错误: {e}")
        print("请配置 LARK_APP_ID 和 LARK_APP_SECRET 环境变量")
        return

    # 获取 Wiki 节点的实际文档 token
    print("获取 Wiki 节点信息...")
    node_info = client.get_wiki_node_info(WIKI_TOKEN)
    if node_info.get("code") != 0:
        print(f"获取 Wiki 节点失败: {node_info}")
        return

    node = node_info.get("data", {}).get("node", {})
    doc_token = node.get("obj_token")
    obj_type = node.get("obj_type")
    print(f"Wiki 节点: obj_token={doc_token}, obj_type={obj_type}")

    if obj_type != "docx":
        print(f"Wiki 节点类型不是文档: {obj_type}")
        return

    # 生成日期列表
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    print(f"=" * 60)
    print(f"上传日报到飞书文档")
    print(f"日期范围: {START_DATE} 到 {END_DATE}")
    print(f"共 {len(dates)} 天")
    print(f"=" * 60)

    # 逐天获取数据并上传
    for date_str in dates:
        print(f"\n--- 处理 {date_str} ---")

        # 获取投手数据
        print(f"[{date_str}] 获取投手数据...")
        optimizer_raw = await fetch_optimizer_summary_stats(BEARER_TOKEN, date_str)
        optimizer_data = convert_optimizer_data(optimizer_raw)

        # 获取剪辑师数据
        print(f"[{date_str}] 获取剪辑师数据...")
        editor_raw = await fetch_editor_combined_stats(BEARER_TOKEN, date_str)
        editor_data = convert_editor_data(editor_raw)

        # 上传到飞书
        print(f"[{date_str}] 上传到飞书...")
        result = client.write_xmp_daily_report(
            doc_token,
            date_str,
            optimizer_data,
            editor_data
        )

        if result.get("code") == 0:
            print(f"[{date_str}] 上传成功!")
        else:
            print(f"[{date_str}] 上传失败: {result}")

    print(f"\n{'=' * 60}")
    print("全部上传完成!")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    asyncio.run(main())
