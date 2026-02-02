"""
生成 1月5-18号 投手和剪辑师日报 Excel
使用 XMP API 获取数据
"""
import asyncio
import sys
sys.path.insert(0, '.')

import pandas as pd
from datetime import datetime, timedelta
from xmp.xmp_scheduler import (
    fetch_optimizer_summary_stats,
    fetch_editor_combined_stats
)

# Bearer Token
BEARER_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NjkwOTQwOTQsImV4cCI6MTc3MDM5MDA5NCwibmJmIjoxNzY5MDk0MDk0LCJkYXRhIjp7ImlkIjoiMjk0ODMifX0.-Gx41oXVvdNnr05pwsRrg-2KXc4abgTHjVYDIGwJ6Go"

# 日期范围
START_DATE = "2026-01-05"
END_DATE = "2026-01-18"


def add_labels(df, spend_col='total_spend', roas_col='total_roas'):
    """添加 ROAS Top1 和 Spend Top1 标注"""
    labels = []
    for _, row in df.iterrows():
        parts = []
        # 找当天的 Top1
        day_df = df[df['日期'] == row['日期']]
        if row[spend_col] == day_df[spend_col].max() and row[spend_col] > 0:
            parts.append('Spend Top1')
        if row[roas_col] == day_df[roas_col].max() and row[roas_col] > 0:
            parts.append('ROAS Top1')
        labels.append(', '.join(parts) if parts else '')
    return labels


async def fetch_optimizer_data(dates):
    """获取投手数据"""
    all_data = []
    for date_str in dates:
        print(f"[投手] 获取 {date_str} 数据...")
        data = await fetch_optimizer_summary_stats(BEARER_TOKEN, date_str)
        all_data.extend(data)
    return all_data


async def fetch_editor_data(dates):
    """获取剪辑师数据"""
    all_data = []
    for date_str in dates:
        print(f"[剪辑师] 获取 {date_str} 数据...")
        data = await fetch_editor_combined_stats(BEARER_TOKEN, date_str)
        all_data.extend(data)
    return all_data


async def main():
    # 生成日期列表
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    print(f"=" * 60)
    print(f"生成 {START_DATE} 到 {END_DATE} 日报")
    print(f"共 {len(dates)} 天")
    print(f"=" * 60)

    # 获取投手数据
    print("\n--- 获取投手数据 ---")
    optimizer_data = await fetch_optimizer_data(dates)

    # 获取剪辑师数据
    print("\n--- 获取剪辑师数据 ---")
    editor_data = await fetch_editor_data(dates)

    # 转换为 DataFrame
    print("\n--- 生成 Excel ---")

    # 投手 DataFrame
    opt_df = pd.DataFrame(optimizer_data)
    opt_df = opt_df.rename(columns={
        'stat_date': '日期',
        'name': '投手',
        'facebook_cost': 'Meta Spend',
        'facebook_roas': 'Meta ROAS',
        'tiktok_cost': 'TT Spend',
        'tiktok_roas': 'TT ROAS',
        'total_cost': '总 Spend',
        'roas': '总 ROAS'
    })
    opt_df = opt_df[['日期', '投手', 'Meta Spend', 'Meta ROAS', 'TT Spend', 'TT ROAS', '总 Spend', '总 ROAS']]
    opt_df['标注'] = add_labels(opt_df, '总 Spend', '总 ROAS')

    # 剪辑师 DataFrame
    ed_df = pd.DataFrame(editor_data)
    ed_df = ed_df.rename(columns={
        'stat_date': '日期',
        'editor_name': '剪辑师',
        'meta_spend': 'Meta Spend',
        'meta_roas': 'Meta ROAS',
        'tt_spend': 'TT Spend',
        'tt_roas': 'TT ROAS',
        'total_spend': '总 Spend',
        'total_roas': '总 ROAS'
    })
    ed_df = ed_df[['日期', '剪辑师', 'Meta Spend', 'Meta ROAS', 'TT Spend', 'TT ROAS', '总 Spend', '总 ROAS']]
    ed_df['标注'] = add_labels(ed_df, '总 Spend', '总 ROAS')

    # 保存到 Excel
    output_file = f'daily_report_{START_DATE}_to_{END_DATE}.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        opt_df.to_excel(writer, sheet_name='投手日报', index=False)
        ed_df.to_excel(writer, sheet_name='剪辑师日报', index=False)

    print(f"\n已保存: {output_file}")
    print(f"投手数据: {len(opt_df)} 条")
    print(f"剪辑师数据: {len(ed_df)} 条")


if __name__ == '__main__':
    asyncio.run(main())
