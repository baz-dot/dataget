"""
测试个人专属助理
"""
import os
import sys
import argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from lark.personal_assistant import PersonalAssistant

# 命令行参数
parser = argparse.ArgumentParser(description='Test Personal Assistant')
parser.add_argument('--webhook', '-w', type=str, help='Webhook URL')
parser.add_argument('--optimizer', '-o', type=str, default='kino', help='Optimizer')
args = parser.parse_args()

project_id = os.getenv('BQ_PROJECT_ID')

print("=" * 60)
print("Test Personal Assistant")
print("=" * 60)
print(f"Optimizer: {args.optimizer}")

# 初始化
assistant = PersonalAssistant(project_id)

# 发送
print(f"\nSending to {args.optimizer}...")
results = assistant.send_to_optimizer(
    args.optimizer,
    webhook_url=args.webhook
)

print(f"\nResults:")
for key, val in results.items():
    status = val.get('code', val.get('StatusCode', -1))
    print(f"  {key}: {'OK' if status == 0 else 'FAIL'}")

print("\n" + "=" * 60)
