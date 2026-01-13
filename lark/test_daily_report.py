"""
测试日报播报功能 - 使用真实 BigQuery 数据
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from lark.lark_bot import LarkBot, Daily_Job
from bigquery_storage import BigQueryUploader

# 配置
webhook_url = os.getenv('LARK_WEBHOOK_URL', 'https://open.larksuite.com/open-apis/bot/v2/hook/df0f480c-d0ac-43b0-bfc0-2531ce27c735')
secret = os.getenv('LARK_SECRET') or None
bi_link = os.getenv('DAILY_REPORT_BI_LINK', 'https://bi.aliyun.com/product/vigloo.htm?menuId=f438317d-6f93-4561-8fb2-e85bf2e9aea8')
project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("测试日报播报功能 - 真实数据")
print("=" * 60)
print(f"Webhook URL: {webhook_url[:50]}...")
print(f"Secret: {'已配置' if secret else '未配置'}")

# 初始化 BigQuery
bq = BigQueryUploader(project_id, "quickbi_data")

# 查询昨天的数据 (T-1)，这是日报的正常逻辑
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"\n[1] 查询 {yesterday} 的日报数据 (T-1)...")

report_data = bq.query_daily_report_data(date=yesterday)

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
