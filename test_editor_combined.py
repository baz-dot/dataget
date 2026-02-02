"""测试获取1月5号剪辑师完整数据（TikTok + Facebook）"""
import asyncio
import sys
sys.path.insert(0, '.')

from xmp.xmp_scheduler import fetch_editor_combined_stats

# Bearer Token
BEARER_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NjkwOTQwOTQsImV4cCI6MTc3MDM5MDA5NCwibmJmIjoxNzY5MDk0MDk0LCJkYXRhIjp7ImlkIjoiMjk0ODMifX0.-Gx41oXVvdNnr05pwsRrg-2KXc4abgTHjVYDIGwJ6Go"

async def main():
    print("=" * 70)
    print("测试获取 2026-01-05 剪辑师完整数据")
    print("=" * 70)

    data = await fetch_editor_combined_stats(BEARER_TOKEN, "2026-01-05")

    print("\n" + "=" * 70)
    print("结果汇总")
    print("=" * 70)

    for d in data:
        print(f"\n{d['editor_name']}:")
        print(f"  TikTok:   Spend ${d['tt_spend']:,.2f}, Rev ${d['tt_revenue']:,.2f}, ROAS {d['tt_roas']:.1%}")
        print(f"  Meta:     Spend ${d['meta_spend']:,.2f}, Rev ${d['meta_revenue']:,.2f}, ROAS {d['meta_roas']:.1%}")
        print(f"  Total:    Spend ${d['total_spend']:,.2f}, Rev ${d['total_revenue']:,.2f}, ROAS {d['total_roas']:.1%}")

if __name__ == '__main__':
    asyncio.run(main())
