"""
测试实时播报 - 验证日环比修复
"""
import os
from dotenv import load_dotenv
load_dotenv()

from scheduler import BrainScheduler

print("=" * 60)
print("测试实时播报 - 验证日环比修复")
print("=" * 60)

scheduler = BrainScheduler()

print("\n发送实时播报...")
result = scheduler.send_realtime_report()

if result.get("success"):
    print("\n发送成功！请检查飞书群消息")
    print("验证要点:")
    print("  1. 截止当前总耗 - 应该有日环比")
    print("  2. 截止当前收入 - 应该有日环比")
    print("  3. 当前 Media ROAS - 应该有日环比")
else:
    print(f"\n发送失败: {result.get('error', result.get('reason', '未知'))}")

print("\n" + "=" * 60)
