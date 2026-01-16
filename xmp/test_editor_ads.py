"""
测试: 从广告维度数据获取剪辑师分渠道统计
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

from xmp_scheduler import (
    XMPMultiChannelScraper,
    SUPPORTED_CHANNELS,
    aggregate_editor_stats_from_ads,
)


async def test_editor_ads():
    """测试获取广告数据并聚合剪辑师统计"""

    # 使用昨天的日期
    from datetime import datetime, timedelta
    date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"=== 测试剪辑师分渠道统计 ===")
    print(f"日期: {date_str}")
    print()

    scraper = XMPMultiChannelScraper()

    # 获取各渠道广告数据
    all_ads = []
    for channel in SUPPORTED_CHANNELS:
        print(f"--- 拉取 {channel} 广告数据 ---")
        ads = await scraper.fetch_channel_ads(channel, date_str, date_str)
        if ads:
            all_ads.extend(ads)
            print(f"  获取 {len(ads)} 条广告")
        else:
            print(f"  无数据")
        print()

    print(f"=== 总计 {len(all_ads)} 条广告 ===")
    print()

    # 聚合剪辑师统计
    editor_stats = aggregate_editor_stats_from_ads(all_ads, date_str)

    print()
    print(f"=== 剪辑师统计结果 ({len(editor_stats)} 条) ===")
    print()

    for stat in editor_stats:
        roas_pct = stat['d0_roas'] * 100
        print(f"  {stat['channel']:8} | {stat['name']:6} | "
              f"消耗: ${stat['total_cost']:,.2f} | "
              f"收入: ${stat['total_revenue']:,.2f} | "
              f"ROAS: {roas_pct:.1f}% | "
              f"素材数: {stat['material_count']}")

    return editor_stats


if __name__ == '__main__':
    asyncio.run(test_editor_ads())
