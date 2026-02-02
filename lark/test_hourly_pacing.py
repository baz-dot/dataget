"""
测试个人实况窗 - 小时级流水账 (Hourly Pacing)
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
parser = argparse.ArgumentParser(description='测试个人实况窗')
parser.add_argument('--webhook', '-w', type=str, help='指定 Webhook URL')
parser.add_argument('--optimizer', '-o', type=str, default='kino', help='投手名称')
parser.add_argument('--hours', '-n', type=int, default=3, help='查询小时数')
args = parser.parse_args()

# 配置
webhook_url = args.webhook or os.getenv('LARK_WEBHOOK_URL')
project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("测试个人实况窗 - Hourly Pacing")
print("=" * 60)
print(f"投手: {args.optimizer}")
print(f"小时数: {args.hours}")

if not webhook_url:
    print("\n[错误] 未配置 Webhook URL")
    sys.exit(1)

# 初始化
bq = BigQueryUploader(project_id, "quickbi_data")
bot = LarkBot(webhook_url)

# 查询数据
print(f"\n[1] 查询 {args.optimizer} 最近 {args.hours} 小时数据...")
pacing_data = bq.query_optimizer_hourly_pacing(args.optimizer, hours=args.hours)

print(f"\n查询结果:")
print(f"  投手: {pacing_data.get('optimizer')}")
print(f"  小时数据: {len(pacing_data.get('hourly_data', []))} 条")

for i, h in enumerate(pacing_data.get('hourly_data', [])):
    market = pacing_data.get('market_hourly_data', [])[i] if i < len(pacing_data.get('market_hourly_data', [])) else {}
    print(f"    {h['hour']}: 花费${h['spend']:.0f} | ROAS {h['roas']:.0%} (大盘 {market.get('roas', 0):.0%})")

# 发送消息
print(f"\n[2] 发送个人实况窗...")
result = bot.send_optimizer_hourly_pacing(pacing_data)
print(f"  发送结果: {result}")

if result.get('StatusCode') == 0 or result.get('code') == 0:
    print("\n[OK] 发送成功！")
else:
    print(f"\n[FAIL] 发送失败")

print("\n" + "=" * 60)
