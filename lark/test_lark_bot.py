"""
Lark 机器人播报测试脚本
用于验证机器人配置和消息发送功能
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
from lark.lark_bot import LarkBot

load_dotenv()


def test_all_message_types():
    """测试所有消息类型"""

    # 从环境变量获取配置
    webhook_url = os.getenv("LARK_WEBHOOK_URL")
    secret = os.getenv("LARK_WEBHOOK_SECRET")

    if not webhook_url:
        print("❌ 请先在 .env 文件中配置 LARK_WEBHOOK_URL")
        print("\n配置示例：")
        print("LARK_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxx")
        print("LARK_WEBHOOK_SECRET=your_secret_key  # 可选")
        return

    bot = LarkBot(webhook_url, secret)

    print("=" * 50)
    print("飞书机器人播报测试")
    print("=" * 50)

    # 测试 1: 简单文本消息
    print("\n[1] 发送简单文本消息...")
    result = bot.send_text("🔔 这是一条测试消息，来自数据播报系统")
    print(f"    结果: {result}")

    # 测试 2: 市场监控报告
    print("\n[2] 发送市场监控日报...")
    market_data = {
        "date": "2025-01-15",
        "impressions": 1523456,
        "clicks": 45678,
        "cost": 12580.50,
        "ctr": 0.03,
        "cpc": 0.275,
        "budget": 15000
    }
    result = bot.send_market_report(market_data)
    print(f"    结果: {result}")

    # 测试 3: 投放效果报告
    print("\n[3] 发送投放效果监控...")
    ad_data = {
        "period": "2025-01-08 ~ 2025-01-15",
        "channel": "抖音/快手/腾讯广告",
        "conversions": 3256,
        "cpa": 15.35,
        "roi": 1.85,
        "change": "↑ 12.5% (环比上周)"
    }
    result = bot.send_ad_performance_report(ad_data)
    print(f"    结果: {result}")

    # 测试 4: 告警消息
    print("\n[4] 发送告警消息...")
    result = bot.send_alert(
        alert_type="预算告警",
        message="腾讯广告渠道今日消耗已达预算 85%，请关注",
        level="warning"
    )
    print(f"    结果: {result}")

    print("\n" + "=" * 50)
    print("测试完成！请检查飞书群是否收到消息")
    print("=" * 50)


def test_at_user():
    """测试@指定用户功能"""

    webhook_url = os.getenv("LARK_WEBHOOK_URL")
    secret = os.getenv("LARK_WEBHOOK_SECRET")

    if not webhook_url:
        print("❌ 请先配置 LARK_WEBHOOK_URL")
        return

    bot = LarkBot(webhook_url, secret)

    # 注意：需要替换为实际的用户 open_id
    # 获取 open_id 的方法见下方说明
    user_ids = ["ou_xxxxxxxx"]  # 替换为实际的 open_id

    print("\n发送消息并@指定用户...")
    result = bot.send_text(
        "请查看今日数据报告",
        at_user_ids=user_ids
    )
    print(f"结果: {result}")


def test_at_all():
    """测试@所有人功能"""

    webhook_url = os.getenv("LARK_WEBHOOK_URL")
    secret = os.getenv("LARK_WEBHOOK_SECRET")

    if not webhook_url:
        print("❌ 请先配置 LARK_WEBHOOK_URL")
        return

    bot = LarkBot(webhook_url, secret)

    print("\n发送消息并@所有人...")
    result = bot.send_text(
        "⚠️ 重要通知：系统将于今晚 22:00 进行维护",
        at_all=True
    )
    print(f"结果: {result}")


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║                   飞书机器人播报测试                          ║
╠══════════════════════════════════════════════════════════════╣
║  1. test_all    - 测试所有消息类型                            ║
║  2. test_at     - 测试@指定用户                               ║
║  3. test_all_at - 测试@所有人                                 ║
╚══════════════════════════════════════════════════════════════╝
    """)

    import sys
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "test_at":
            test_at_user()
        elif cmd == "test_all_at":
            test_at_all()
        else:
            test_all_message_types()
    else:
        test_all_message_types()
