"""
测试实时播报功能 - 使用真实 BigQuery 数据
"""
import os
import sys
import argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from lark.lark_bot import LarkBot
from bigquery_storage import BigQueryUploader

# 命令行参数
parser = argparse.ArgumentParser(description='测试实时播报')
parser.add_argument('--webhook', '-w', type=str, help='指定 Webhook URL')
parser.add_argument('--secret', '-s', type=str, help='指定签名密钥')
parser.add_argument('--date', '-d', type=str, help='指定日期 (YYYY-MM-DD)，默认今天')
parser.add_argument('--latest-batch', '-l', action='store_true', help='使用最新 batch（而非整点 batch）')
parser.add_argument('--same-day-batch', action='store_true', help='使用同日 batch（batch_id 日期与 stat_date 相同）')
args = parser.parse_args()

# 配置 - 优先使用命令行参数
webhook_url = args.webhook or os.getenv('LARK_WEBHOOK_URL')
secret = args.secret or os.getenv('LARK_SECRET') or None
project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("测试实时播报功能 - 真实数据")
print("=" * 60)
print(f"Webhook URL: {webhook_url[:50] if webhook_url else '未配置'}...")
print(f"Secret: {'已配置' if secret else '未配置'}")

if not webhook_url:
    print("\n[错误] 未配置 Webhook URL")
    print("使用方法: python test_realtime_report.py --webhook <URL>")
    sys.exit(1)

# 初始化 BigQuery
bq = BigQueryUploader(project_id, "quickbi_data")

# 查询日期 - 优先使用命令行参数，默认今天
query_date = args.date or datetime.now().strftime('%Y-%m-%d')
use_latest = args.latest_batch
use_same_day = args.same_day_batch
print(f"\n[1] 查询 {query_date} 的实时数据... (use_latest_batch={use_latest}, use_same_day_batch={use_same_day})")
realtime_data = bq.query_realtime_report_data(date=query_date, use_latest_batch=use_latest, use_same_day_batch=use_same_day)

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
print(f"    Media ROAS: {summary.get('media_roas', 0):.2%}")

yesterday_summary = realtime_data.get('yesterday_summary', {})
if yesterday_summary:
    print(f"\n  前一日同时刻 (日环比基准):")
    print(f"    昨日消耗: ${yesterday_summary.get('total_spend', 0):,.2f}")
    print(f"    昨日收入: ${yesterday_summary.get('total_revenue', 0):,.2f}")
    print(f"    昨日 ROAS: {yesterday_summary.get('media_roas', 0):.2%}")
else:
    print(f"\n  前一日同时刻: 暂无数据")

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
print(f"\n[2] 发送实时播报到飞书...")
# 获取 API Key
gemini_api_key = os.getenv("GEMINI_API_KEY")
chatgpt_api_key = os.getenv("OPENAI_API_KEY") or gemini_api_key

bot = LarkBot(
    webhook_url=webhook_url,
    secret=secret,
    gemini_api_key=gemini_api_key,
    chatgpt_api_key=chatgpt_api_key
)

# 直接发送，环比数据已包含在 realtime_data 中
result = bot.send_realtime_report(data=realtime_data)
print(f"  发送结果: {result}")

if result.get('StatusCode') == 0 or result.get('code') == 0:
    print("\n[OK] 发送成功！")
else:
    print(f"\n[FAIL] 发送失败: {result}")

print("\n" + "=" * 60)
print("测试完成！请检查飞书群消息")
print("=" * 60)
