"""测试获取1月5号投手数据"""
import asyncio
import sys
sys.path.insert(0, '.')

from xmp.xmp_scheduler import fetch_optimizer_summary_stats

# Bearer Token (从用户提供的请求头中获取)
BEARER_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NjkwODk3MjIsImV4cCI6MTc3MDM4NTcyMiwibmJmIjoxNzY5MDg5NzIyLCJkYXRhIjp7ImlkIjoiMjk0ODMifX0.XW4SF0xIZL3QKG3e6CB-6GfsFVhPRVQlAcqCtLSIX10"

async def main():
    print("=" * 60)
    print("测试获取 2026-01-05 投手数据")
    print("=" * 60)

    data = await fetch_optimizer_summary_stats(BEARER_TOKEN, "2026-01-05")

    print("\n" + "=" * 60)
    print("结果汇总")
    print("=" * 60)

    for d in data:
        print(f"\n{d['name']}:")
        print(f"  TikTok:   Spend ${d['tiktok_cost']:,.2f}, Revenue ${d['tiktok_revenue']:,.2f}, ROAS {d['tiktok_roas']:.1%}")
        print(f"  Facebook: Spend ${d['facebook_cost']:,.2f}, Revenue ${d['facebook_revenue']:,.2f}, ROAS {d['facebook_roas']:.1%}")
        print(f"  Total:    Spend ${d['total_cost']:,.2f}, Revenue ${d['total_revenue']:,.2f}, ROAS {d['roas']:.1%}")

if __name__ == '__main__':
    asyncio.run(main())
