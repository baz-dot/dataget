"""
测试重启提醒 - Zombie Alert
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
parser = argparse.ArgumentParser(description='测试重启提醒')
parser.add_argument('--webhook', '-w', type=str, help='Webhook URL')
parser.add_argument('--optimizer', '-o', type=str, default='kino', help='投手名称')
args = parser.parse_args()

# 配置
webhook_url = args.webhook or os.getenv('LARK_WEBHOOK_URL')
project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("Test Zombie Alert")
print("=" * 60)
print(f"Optimizer: {args.optimizer}")

if not webhook_url:
    print("\n[Error] No Webhook URL")
    sys.exit(1)

# 初始化
bq = BigQueryUploader(project_id, "quickbi_data")
bot = LarkBot(webhook_url)

# 查询数据
print(f"\n[1] Query zombie alerts for {args.optimizer}...")
zombie_data = bq.query_optimizer_zombie_alerts(args.optimizer)

print(f"\nResult:")
print(f"  Optimizer: {zombie_data.get('optimizer')}")
print(f"  Zombie alerts: {len(zombie_data.get('zombie_alerts', []))}")

for z in zombie_data.get('zombie_alerts', []):
    print(f"    [ZOMBIE] {z['drama_name']}({z['country']}): ROAS {z['roas']:.0%}")

# 发送消息
print(f"\n[2] Send zombie alert...")
result = bot.send_optimizer_zombie_alerts(zombie_data)
print(f"  Result: {result}")

if result.get('StatusCode') == 0 or result.get('code') == 0:
    print("\n[OK] Success!")
else:
    print(f"\n[FAIL] Failed")

print("\n" + "=" * 60)
