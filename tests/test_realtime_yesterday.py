"""
测试实时播报功能 - 使用昨天的数据
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from lark.lark_bot import LarkBot
from bigquery_storage import BigQueryUploader

# 配置
webhook_url = os.getenv('LARK_WEBHOOK_URL')
secret = os.getenv('LARK_SECRET') or None
project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("测试实时播报功能 - 使用昨天数据")
print("=" * 60)
print(f"Webhook URL: {webhook_url[:50]}...")

# 初始化
bq = BigQueryUploader(project_id, "quickbi_data")
bot = LarkBot(webhook_url, secret)

# 使用昨天的数据测试
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"\n[1] 查询 {yesterday} 的实时数据...")

report_data = bq.query_realtime_report_data(date=yesterday)

print(f"\n查询结果:")
print(f"  日期: {report_data.get('date')}")
print(f"  当前时间: {report_data.get('current_hour')}")
print(f"  Batch ID: {report_data.get('batch_id')}")

summary = report_data.get('summary', {})
print(f"\n  大盘数据:")
print(f"    总消耗: ${summary.get('total_spend', 0):,.2f}")
print(f"    总收入: ${summary.get('total_revenue', 0):,.2f}")
print(f"    Media ROAS: {summary.get('media_roas', 0):.2%}")

# 发送实时播报
print(f"\n[2] 发送实时播报到飞书...")
result = bot.send_realtime_report(report_data)
print(f"  发送结果: {result}")

if result.get('StatusCode') == 0 or result.get('code') == 0:
    print("\n[OK] 发送成功！")
else:
    print(f"\n[FAIL] 发送失败: {result}")

print("\n" + "=" * 60)
print("测试完成！请检查飞书群消息")
print("=" * 60)
