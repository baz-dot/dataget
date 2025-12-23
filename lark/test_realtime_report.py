"""
测试实时播报功能 - 使用真实 BigQuery 数据
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from lark.lark_bot import LarkBot
from bigquery_storage import BigQueryUploader

# 配置
webhook_url = os.getenv('LARK_WEBHOOK_URL', 'https://open.larksuite.com/open-apis/bot/v2/hook/25092ba0-5569-4be4-8fad-d64047dfedbf')
secret = os.getenv('LARK_SECRET') or None
project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("测试实时播报功能 - 真实数据")
print("=" * 60)
print(f"Webhook URL: {webhook_url[:50]}...")
print(f"Secret: {'已配置' if secret else '未配置'}")

# 初始化 BigQuery
bq = BigQueryUploader(project_id, "quickbi_data")

# 查询当日实时数据
print(f"\n[1] 查询当日实时数据...")
realtime_data = bq.query_realtime_report_data()

print(f"\n查询结果:")
print(f"  日期: {realtime_data.get('date')}")
print(f"  当前时间: {realtime_data.get('current_hour')}")
print(f"  Batch ID: {realtime_data.get('batch_id')}")
print(f"  API更新时间: {realtime_data.get('api_update_time')}")
print(f"  数据延迟: {realtime_data.get('data_delayed')}")

summary = realtime_data.get('summary', {})
print(f"\n  大盘总览:")
print(f"    总消耗: ${summary.get('total_spend', 0):,.2f}")
print(f"    总收入: ${summary.get('total_revenue', 0):,.2f}")
print(f"    D0 ROAS: {summary.get('d0_roas', 0):.2%}")

optimizer_spend = realtime_data.get('optimizer_spend', [])
print(f"\n  投手消耗 ({len(optimizer_spend)} 人):")
for opt in optimizer_spend[:5]:
    print(f"    - {opt.get('optimizer')}: ${opt.get('spend', 0):,.2f} (ROAS: {opt.get('roas', 0):.2%})")

stop_loss = realtime_data.get('stop_loss_campaigns', [])
print(f"\n  止损预警 ({len(stop_loss)} 个):")
for camp in stop_loss[:3]:
    print(f"    - {camp.get('campaign_name', '')[:30]}: ${camp.get('spend', 0):,.2f} (ROAS: {camp.get('roas', 0):.2%})")

scale_up = realtime_data.get('scale_up_campaigns', [])
print(f"\n  扩量机会 ({len(scale_up)} 个):")
for camp in scale_up[:3]:
    print(f"    - {camp.get('campaign_name', '')[:30]}: ${camp.get('spend', 0):,.2f} (ROAS: {camp.get('roas', 0):.2%})")

country_roas = realtime_data.get('country_marginal_roas', [])
print(f"\n  国家边际ROAS ({len(country_roas)} 个):")
for c in country_roas[:5]:
    print(f"    - {c.get('country')}: ROAS {c.get('roas', 0):.2%}")

# 发送实时播报
print(f"\n[2] 获取上一小时快照数据...")
prev_snapshot = bq.get_previous_hour_snapshot()
if prev_snapshot:
    print(f"  上小时消耗: ${prev_snapshot.get('total_spend', 0):,.2f}")
    print(f"  上小时 ROAS: {prev_snapshot.get('d0_roas', 0):.2%}")
else:
    print("  暂无上小时快照数据")

print(f"\n[3] 发送实时播报到飞书...")
bot = LarkBot(webhook_url=webhook_url, secret=secret)

# 传入 prev_data 用于计算环比
result = bot.send_realtime_report(data=realtime_data, prev_data=prev_snapshot)
print(f"  发送结果: {result}")

if result.get('StatusCode') == 0 or result.get('code') == 0:
    print("\n[OK] 发送成功！")
else:
    print(f"\n[FAIL] 发送失败: {result}")

print("\n" + "=" * 60)
print("测试完成！请检查飞书群消息")
print("=" * 60)
