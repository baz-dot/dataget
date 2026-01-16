"""
测试: 剪辑师分渠道统计 (Meta用designer API, TikTok用ad API)
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

from xmp.xmp_scheduler import (
    XMPMultiChannelScraper,
    aggregate_editor_stats_from_ads,
)


async def test_editor_stats():
    """测试剪辑师分渠道统计"""

    from datetime import datetime, timedelta
    date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"=== 测试剪辑师分渠道统计 ===")
    print(f"日期: {date_str}")
    print()

    scraper = XMPMultiChannelScraper()
    editor_stats = []

    # 1. Meta: 使用 designer 维度 API
    print("--- Meta (designer API) ---")
    meta_designers = await scraper.fetch_channel_designers('facebook', date_str, date_str)
    if meta_designers:
        for d in meta_designers:
            cost = d['cost']
            revenue = d['revenue']
            editor_stats.append({
                'stat_date': date_str,
                'channel': 'facebook',
                'name': d['designer_name'],
                'total_cost': cost,
                'total_revenue': revenue,
                'd0_roas': revenue / cost if cost > 0 else 0,
            })
        print(f"  获取 {len(meta_designers)} 条 designer 数据")
    else:
        print("  无数据")
    print()

    # 2. TikTok: 使用 ad 维度 API
    print("--- TikTok (ad API) ---")
    tk_ads = await scraper.fetch_channel_ads('tiktok', date_str, date_str)
    if tk_ads:
        tk_stats = aggregate_editor_stats_from_ads(tk_ads, date_str)
        editor_stats.extend(tk_stats)
        print(f"  从 {len(tk_ads)} 条广告提取 {len(tk_stats)} 条剪辑师数据")
    else:
        print("  无数据")
    print()

    # 按消耗排序
    editor_stats.sort(key=lambda x: x['total_cost'], reverse=True)

    # 输出结果
    print(f"=== 剪辑师统计结果 ({len(editor_stats)} 条) ===")
    print()

    for stat in editor_stats:
        roas_pct = stat['d0_roas'] * 100
        print(f"  {stat['channel']:8} | {stat['name']:20} | "
              f"消耗: ${stat['total_cost']:,.2f} | "
              f"收入: ${stat['total_revenue']:,.2f} | "
              f"ROAS: {roas_pct:.1f}%")

    # 汇总
    print()
    meta_stats = [s for s in editor_stats if s['channel'] == 'facebook']
    tk_stats = [s for s in editor_stats if s['channel'] == 'tiktok']

    if meta_stats:
        meta_cost = sum(s['total_cost'] for s in meta_stats)
        meta_rev = sum(s['total_revenue'] for s in meta_stats)
        print(f"Meta 汇总: {len(meta_stats)} 人, 消耗 ${meta_cost:,.2f}, "
              f"收入 ${meta_rev:,.2f}, ROAS {meta_rev/meta_cost*100:.1f}%")

    if tk_stats:
        tk_cost = sum(s['total_cost'] for s in tk_stats)
        tk_rev = sum(s['total_revenue'] for s in tk_stats)
        print(f"TikTok 汇总: {len(tk_stats)} 人, 消耗 ${tk_cost:,.2f}, "
              f"收入 ${tk_rev:,.2f}, ROAS {tk_rev/tk_cost*100:.1f}%")

    return editor_stats


if __name__ == '__main__':
    asyncio.run(test_editor_stats())
