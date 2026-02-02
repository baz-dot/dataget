"""
抓取指定日期的 XMP 数据并上传到 BigQuery
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

from xmp.xmp_scheduler import run_once


async def main():
    """抓取 2026-01-15 数据并上传到 BigQuery"""
    date_str = '2026-01-15'

    print(f"=== 抓取 {date_str} 数据 ===")
    print()

    result = await run_once(date_str=date_str, upload_bq=True)

    # 输出结果摘要
    campaigns = result.get('campaigns', [])
    optimizer_stats = result.get('optimizer_stats', [])
    editor_stats = result.get('editor_stats', [])

    print()
    print("=== 结果摘要 ===")
    print(f"Campaign 数据: {len(campaigns)} 条")
    print(f"投手统计: {len(optimizer_stats)} 条")
    print(f"剪辑师统计: {len(editor_stats)} 条")

    # 输出剪辑师统计详情
    if editor_stats:
        print()
        print("=== 剪辑师统计 ===")
        for stat in editor_stats[:10]:  # 只显示前10条
            roas_pct = stat['d0_roas'] * 100
            print(f"  {stat['channel']:8} | {stat['name']:20} | "
                  f"消耗: ${stat['total_cost']:,.2f} | "
                  f"收入: ${stat['total_revenue']:,.2f} | "
                  f"ROAS: {roas_pct:.1f}%")

    return result


if __name__ == '__main__':
    asyncio.run(main())
