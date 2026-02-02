"""检查 TikTok 数据情况"""
import os
import sys
from datetime import datetime, timedelta

# 设置环境变量
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'fleet-blend-469520-n7-1a29eac22376.json'

from xmp.xmp_scheduler import query_stats_from_bq

def check_tt_data():
    """检查最近几天的 TikTok 数据"""
    print("=" * 80)
    print("检查 TikTok 数据情况")
    print("=" * 80)

    # 检查最近 7 天
    for i in range(7):
        date = (datetime.now() - timedelta(days=i+1)).strftime('%Y-%m-%d')

        try:
            data = query_stats_from_bq(date)
            optimizer_stats = data.get('optimizer_stats', [])
            editor_stats = data.get('editor_stats', [])
            
            # 按渠道统计
            fb_count = sum(1 for s in optimizer_stats if s.get('channel') == 'facebook')
            tt_count = sum(1 for s in optimizer_stats if s.get('channel') == 'tiktok')
            
            fb_spend = sum(s.get('total_cost', 0) for s in optimizer_stats if s.get('channel') == 'facebook')
            tt_spend = sum(s.get('total_cost', 0) for s in optimizer_stats if s.get('channel') == 'tiktok')
            
            print(f"\n日期: {date}")
            print(f"  投手数据: {len(optimizer_stats)} 条")
            print(f"  剪辑师数据: {len(editor_stats)} 条")
            print(f"  Facebook: {fb_count} 条, Spend: ${fb_spend:,.2f}")
            print(f"  TikTok: {tt_count} 条, Spend: ${tt_spend:,.2f}")
            
            if tt_count == 0:
                print(f"  ⚠️ 警告: {date} 没有 TikTok 数据！")
            
        except Exception as e:
            print(f"\n日期: {date}")
            print(f"  ❌ 查询失败: {e}")

if __name__ == '__main__':
    check_tt_data()

