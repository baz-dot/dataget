"""测试获取1月5号剪辑师TikTok数据"""
import asyncio
import sys
sys.path.insert(0, '.')

from xmp.xmp_scheduler import fetch_editor_tiktok_stats

# Bearer Token
BEARER_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NjkwOTQwOTQsImV4cCI6MTc3MDM5MDA5NCwibmJmIjoxNzY5MDk0MDk0LCJkYXRhIjp7ImlkIjoiMjk0ODMifX0.-Gx41oXVvdNnr05pwsRrg-2KXc4abgTHjVYDIGwJ6Go"

async def main():
    print("=" * 60)
    print("测试获取 2026-01-05 剪辑师 TikTok 数据")
    print("=" * 60)

    data = await fetch_editor_tiktok_stats(BEARER_TOKEN, "2026-01-05")

    print("\n" + "=" * 60)
    print("结果汇总")
    print("=" * 60)

    for d in data:
        print(f"{d['editor_name']}: Spend ${d['spend']:,.2f}, Revenue ${d['revenue']:,.2f}, ROAS {d['roas']:.1%}")

if __name__ == '__main__':
    asyncio.run(main())
