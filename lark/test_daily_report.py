"""
测试日报播报功能 - 使用真实 BigQuery 数据
"""
import os
import sys
import argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from lark.lark_bot import LarkBot, Daily_Job
from bigquery_storage import BigQueryUploader

# 命令行参数
parser = argparse.ArgumentParser(description='测试日报播报')
parser.add_argument('--webhook', '-w', type=str, help='指定 Webhook URL')
parser.add_argument('--secret', '-s', type=str, help='指定签名密钥')
parser.add_argument('--date', '-d', type=str, help='指定日期 (YYYY-MM-DD)，默认昨天')
parser.add_argument('--latest', '-l', action='store_true', help='使用最新 batch（而非整点 batch）')
args = parser.parse_args()

# 配置 - 优先使用命令行参数
webhook_url = args.webhook or os.getenv('LARK_WEBHOOK_URL')
secret = args.secret or os.getenv('LARK_SECRET') or None
bi_link = os.getenv('DAILY_REPORT_BI_LINK', 'https://bi.aliyun.com/product/vigloo.htm')
project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("测试日报播报功能 - 真实数据")
print("=" * 60)
print(f"Webhook URL: {webhook_url[:50] if webhook_url else '未配置'}...")
print(f"Secret: {'已配置' if secret else '未配置'}")

if not webhook_url:
    print("\n[错误] 未配置 Webhook URL")
    print("使用方法: python test_daily_report.py --webhook <URL>")
    sys.exit(1)

# 初始化 BigQuery
bq = BigQueryUploader(project_id, "quickbi_data")

# 查询日期 - 优先使用命令行参数，默认昨天
query_date = args.date or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"\n[1] 查询 {query_date} 的日报数据...")
if args.latest:
    print("    (使用最新 batch)")

report_data = bq.query_daily_report_data(date=query_date, use_latest_batch=args.latest)

print(f"\n查询结果:")
print(f"  日期: {report_data.get('date')}")
print(f"  大盘总览: {report_data.get('summary')}")
print(f"  前一天数据: {report_data.get('summary_prev')}")
print(f"  投手数量: {len(report_data.get('optimizers', []))}")
print(f"  剧集 Top5: {len(report_data.get('dramas_top5', []))}")
print(f"  国家 Top5: {len(report_data.get('countries_top5', []))}")
print(f"  放量剧目: {len(report_data.get('scale_up_dramas', []))}")
print(f"  机会市场: {len(report_data.get('opportunity_markets', []))}")

# 检查是否有真实数据
summary = report_data.get('summary', {})
total_spend = summary.get('total_spend', 0)

if total_spend == 0:
    print("\n[警告] 总消耗为0，将触发异常报警")

# 发送日报
print(f"\n[2] 发送日报到飞书...")
result = Daily_Job(
    webhook_url=webhook_url,
    secret=secret,
    data=report_data,
    bi_link=bi_link
)
print(f"  发送结果: {result}")

if result.get('StatusCode') == 0 or result.get('code') == 0:
    print("\n[OK] 发送成功！")
else:
    print(f"\n[FAIL] 发送失败: {result}")

print("\n" + "=" * 60)
print("测试完成！请检查飞书群消息")
print("=" * 60)
