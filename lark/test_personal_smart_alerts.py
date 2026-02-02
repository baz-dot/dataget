"""
测试个人播报智能预警 - 带渠道对比和分地区明细
"""
import os
import sys
import argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from lark.lark_bot import LarkBot
from bigquery_storage import BigQueryUploader

# 命令行参数
parser = argparse.ArgumentParser(description='测试个人播报智能预警')
parser.add_argument('--webhook', '-w', type=str, required=True, help='Webhook URL')
parser.add_argument('--optimizer', '-o', type=str, default='kimi', help='投手名称')
args = parser.parse_args()

project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("测试个人播报智能预警")
print("=" * 60)
print(f"投手: {args.optimizer}")

# 初始化
bq = BigQueryUploader(project_id, "quickbi_data")
bot = LarkBot(args.webhook)

# 查询数据
print(f"\n[1] 查询 {args.optimizer} 的预警数据...")
alerts_data = bq.query_optimizer_alerts_with_benchmark(args.optimizer)

print(f"\n止损预警 ({len(alerts_data.get('stop_loss_alerts', []))} 个):")
for alert in alerts_data.get('stop_loss_alerts', [])[:3]:
    print(f"  - {alert.get('campaign_name', '')[:30]}")
    print(f"    ROAS: {alert.get('roas', 0):.0%} | 渠道大盘: {alert.get('channel_roas', 0):.0%}")
    details = alert.get('country_details', [])
    if details:
        print(f"    地区: {', '.join([d['country'] for d in details[:3]])}")

print(f"\n扩量机会 ({len(alerts_data.get('scale_up_alerts', []))} 个):")
for alert in alerts_data.get('scale_up_alerts', [])[:3]:
    print(f"  - {alert.get('campaign_name', '')[:30]}")
    print(f"    ROAS: {alert.get('roas', 0):.0%} | 渠道大盘: {alert.get('channel_roas', 0):.0%}")
    details = alert.get('country_details', [])
    if details:
        print(f"    地区: {', '.join([d['country'] for d in details[:3]])}")

# 发送
if alerts_data.get('stop_loss_alerts') or alerts_data.get('scale_up_alerts'):
    print(f"\n[2] 发送个人播报...")
    result = bot.send_optimizer_smart_alerts(alerts_data)
    print(f"  结果: {result}")
else:
    print(f"\n[2] 无预警数据，跳过发送")

print("\n" + "=" * 60)
