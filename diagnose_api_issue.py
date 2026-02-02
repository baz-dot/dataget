"""
诊断 XMP API 问题
检查 Token 有效性和 API 返回数据量
"""
import asyncio
import os
from dotenv import load_dotenv
from xmp.xmp_scheduler import XMPMultiChannelScraper

load_dotenv()

async def diagnose_api():
    """诊断 API 问题"""
    scraper = XMPMultiChannelScraper()

    print("=" * 80)
    print("XMP API 诊断")
    print("=" * 80)

    # 1. 检查 Token 状态
    print("\n[1] 检查 Token 状态...")
    if scraper.bearer_token:
        print(f"    Token 存在: {scraper.bearer_token[:50]}...")
        print(f"    Token 时间: {scraper.token_timestamp}")
    else:
        print("    Token 不存在，需要登录")

    # 2. 测试 Meta API
    print("\n[2] 测试 Meta (Facebook) API...")
    try:
        meta_designers = await scraper.fetch_channel_designers(
            'facebook',
            '2026-01-16',
            '2026-01-16'
        )

        if meta_designers:
            print(f"    ✓ 成功获取 {len(meta_designers)} 条数据")
            total_cost = sum(d['cost'] for d in meta_designers)
            print(f"    总消耗: ${total_cost:,.2f}")

            print("\n    剪辑师列表:")
            for d in sorted(meta_designers, key=lambda x: x['cost'], reverse=True):
                print(f"      - {d['designer_name']}: ${d['cost']:,.2f}")
        else:
            print("    ✗ 未获取到数据")
    except Exception as e:
        print(f"    ✗ 请求失败: {e}")

    # 3. 测试 TikTok API
    print("\n[3] 测试 TikTok API...")
    try:
        tk_ads = await scraper.fetch_channel_ads(
            'tiktok',
            '2026-01-16',
            '2026-01-16'
        )

        if tk_ads:
            print(f"    ✓ 成功获取 {len(tk_ads)} 条广告")
            total_cost = sum(ad['cost'] for ad in tk_ads)
            print(f"    总消耗: ${total_cost:,.2f}")
        else:
            print("    ✗ 未获取到数据")
    except Exception as e:
        print(f"    ✗ 请求失败: {e}")

if __name__ == '__main__':
    asyncio.run(diagnose_api())
