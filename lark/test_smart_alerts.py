"""
测试智能预警 - 带大盘对比 (Smart Alerts)
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
parser = argparse.ArgumentParser(description='测试智能预警')
parser.add_argument('--webhook', '-w', type=str, help='指定 Webhook URL')
parser.add_argument('--optimizer', '-o', type=str, default='kino', help='投手名称')
args = parser.parse_args()

# 配置
webhook_url = args.webhook or os.getenv('LARK_WEBHOOK_URL')
project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("测试智能预警 - Smart Alerts")
print("=" * 60)
print(f"投手: {args.optimizer}")

if not webhook_url:
    print("\n[错误] 未配置 Webhook URL")
    sys.exit(1)

# 初始化
bq = BigQueryUploader(project_id, "quickbi_data")
bot = LarkBot(webhook_url)

# 查询数据
print(f"\n[1] 查询 {args.optimizer} 的预警数据...")
alerts_data = bq.query_optimizer_alerts_with_benchmark(args.optimizer)

print(f"\n查询结果:")
print(f"  投手: {alerts_data.get('optimizer')}")
print(f"  止损预警: {len(alerts_data.get('stop_loss_alerts', []))} 条")
print(f"  扩量机会: {len(alerts_data.get('scale_up_alerts', []))} 条")

for alert in alerts_data.get('stop_loss_alerts', []):
    print(f"    [STOP] {alert['drama_name']}({alert['country']}): ROAS {alert['roas']:.0%} vs benchmark {alert['benchmark_roas']:.0%}")

for alert in alerts_data.get('scale_up_alerts', []):
    print(f"    [SCALE] {alert['drama_name']}({alert['country']}): ROAS {alert['roas']:.0%} vs benchmark {alert['benchmark_roas']:.0%}")

# 发送消息
print(f"\n[2] 发送智能预警...")
result = bot.send_optimizer_smart_alerts(alerts_data)
print(f"  发送结果: {result}")

if result.get('StatusCode') == 0 or result.get('code') == 0:
    print("\n[OK] 发送成功！")
else:
    print(f"\n[FAIL] 发送失败")

print("\n" + "=" * 60)
