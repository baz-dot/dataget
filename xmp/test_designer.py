"""
测试: 获取 Meta designer 维度数据
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

from xmp.xmp_scheduler import XMPMultiChannelScraper


async def test_designer():
    """测试获取 designer 数据"""

    from datetime import datetime, timedelta
    date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"=== 测试 Meta Designer 数据 ===")
    print(f"日期: {date_str}")
    print()

    scraper = XMPMultiChannelScraper()

    # 获取 Meta designer 数据
    designers = await scraper.fetch_channel_designers('facebook', date_str, date_str)

    if not designers:
        print("无数据")
        return

    print(f"\n=== Designer 数据 ({len(designers)} 条) ===\n")

    for d in designers:
        roas = d['revenue'] / d['cost'] * 100 if d['cost'] > 0 else 0
        print(f"  {d['designer_name']:20} | "
              f"消耗: ${d['cost']:,.2f} | "
              f"收入: ${d['revenue']:,.2f} | "
              f"ROAS: {roas:.1f}%")

    # 汇总
    total_cost = sum(d['cost'] for d in designers)
    total_revenue = sum(d['revenue'] for d in designers)
    avg_roas = total_revenue / total_cost * 100 if total_cost > 0 else 0

    print(f"\n=== 汇总 ===")
    print(f"总消耗: ${total_cost:,.2f}")
    print(f"总收入: ${total_revenue:,.2f}")
    print(f"平均 ROAS: {avg_roas:.1f}%")

    return designers


if __name__ == '__main__':
    asyncio.run(test_designer())
