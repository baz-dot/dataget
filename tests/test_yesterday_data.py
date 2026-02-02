"""
测试昨天同整点数据查询
"""
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from bigquery_storage import BigQueryUploader

project_id = os.getenv('BQ_PROJECT_ID')
bq = BigQueryUploader(project_id, "quickbi_data")

print("=" * 60)
print("测试昨天同整点数据查询")
print("=" * 60)

# 查询实时播报数据
print(f"\n当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"当前小时: {datetime.now().hour}")

data = bq.query_realtime_report_data()

print(f"\n查询结果:")
print(f"  日期: {data.get('date')}")
print(f"  当前 batch_id: {data.get('batch_id')}")
print(f"  batch_time: {data.get('batch_time')}")

# 当前数据
summary = data.get('summary', {})
print(f"\n当前数据:")
print(f"  总消耗: ${summary.get('total_spend', 0):,.2f}")
print(f"  总收入: ${summary.get('total_media_revenue', 0):,.2f}")
print(f"  Media ROAS: {summary.get('media_roas', 0):.1%}")

# 昨天同整点数据
yesterday_summary = data.get('yesterday_summary', {})
print(f"\n昨天同整点数据:")
print(f"  总消耗: ${yesterday_summary.get('total_spend', 0):,.2f}")
print(f"  总收入: ${yesterday_summary.get('total_media_revenue', 0):,.2f}")
print(f"  Media ROAS: {yesterday_summary.get('media_roas', 0):.1%}")

# 检查是否有数据
if yesterday_summary.get('total_spend', 0) == 0:
    print("\n⚠️ 警告: 昨天同整点数据为空！")
    print("可能原因:")
    print("  1. 昨天该时刻没有数据")
    print("  2. batch_id 查询范围有问题")
    print("  3. 数据延迟")
else:
    print("\n✓ 昨天同整点数据查询成功")

    # 计算日环比
    if yesterday_summary.get('total_spend', 0) > 0:
        spend_change = ((summary.get('total_spend', 0) - yesterday_summary.get('total_spend', 0))
                       / yesterday_summary.get('total_spend', 0) * 100)
        print(f"\n日环比计算:")
        print(f"  消耗日环比: {spend_change:+.1f}%")

    if yesterday_summary.get('total_media_revenue', 0) > 0:
        revenue_change = ((summary.get('total_media_revenue', 0) - yesterday_summary.get('total_media_revenue', 0))
                         / yesterday_summary.get('total_media_revenue', 0) * 100)
        print(f"  收入日环比: {revenue_change:+.1f}%")

    if yesterday_summary.get('media_roas', 0) > 0:
        roas_change = summary.get('media_roas', 0) - yesterday_summary.get('media_roas', 0)
        print(f"  ROAS 日环比: {roas_change:+.1%}")

print("\n" + "=" * 60)
