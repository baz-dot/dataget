"""
测试直接获取 2025-01-15 的剪辑师数据
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

from xmp.xmp_scheduler import XMPMultiChannelScraper


async def main():
    date_str = '2025-01-15'
    print(f"=== 测试获取 {date_str} 数据 ===\n")

    scraper = XMPMultiChannelScraper()

    # 1. 测试 Meta designer API
    print("--- Meta Designer API ---")
    meta_data = await scraper.fetch_channel_designers('facebook', date_str, date_str)
    if meta_data:
        print(f"获取 {len(meta_data)} 条 designer 数据")
        total_cost = sum(d['cost'] for d in meta_data)
        total_rev = sum(d['revenue'] for d in meta_data)
        print(f"总消耗: ${total_cost:,.2f}, 总收入: ${total_rev:,.2f}")
    else:
        print("无数据")

    print()

    # 2. 测试 TikTok ad API
    print("--- TikTok Ad API ---")
    tk_data = await scraper.fetch_channel_ads('tiktok', date_str, date_str)
    if tk_data:
        print(f"获取 {len(tk_data)} 条广告数据")
        total_cost = sum(d['cost'] for d in tk_data)
        total_rev = sum(d['revenue'] for d in tk_data)
        print(f"总消耗: ${total_cost:,.2f}, 总收入: ${total_rev:,.2f}")
    else:
        print("无数据")


if __name__ == '__main__':
    asyncio.run(main())
