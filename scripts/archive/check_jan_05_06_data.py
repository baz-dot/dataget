"""检查 1月5日和6日的剪辑师数据"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
client = bigquery.Client(project=PROJECT_ID)

dates = ['2026-01-05', '2026-01-06']

print("="*80)
print("检查 1月5日和6日的数据情况")
print("="*80)

for date in dates:
    print(f"\n{'='*80}")
    print(f"日期: {date}")
    print(f"{'='*80}")
    
    # 1. 检查 xmp_campaigns 表（投手数据）
    query1 = f"""
    SELECT COUNT(*) as count, MAX(batch_id) as latest_batch
    FROM `{PROJECT_ID}.xmp_data.xmp_campaigns`
    WHERE stat_date = '{date}'
    """
    result1 = list(client.query(query1).result())
    if result1:
        row = result1[0]
        print(f"\n[xmp_campaigns] 投手数据:")
        print(f"  记录数: {row.count}")
        print(f"  最新 batch: {row.latest_batch}")
    
    # 2. 检查 xmp_editor_stats 表（剪辑师统计）
    query2 = f"""
    SELECT COUNT(*) as count, MAX(batch_id) as latest_batch
    FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
    WHERE stat_date = '{date}'
    """
    result2 = list(client.query(query2).result())
    if result2:
        row = result2[0]
        print(f"\n[xmp_editor_stats] 剪辑师统计:")
        print(f"  记录数: {row.count}")
        print(f"  最新 batch: {row.latest_batch}")
        
        # 查看剪辑师名单
        if row.count > 0:
            query3 = f"""
            SELECT editor_name, channel, spend, revenue
            FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
            WHERE stat_date = '{date}' AND batch_id = '{row.latest_batch}'
            ORDER BY spend DESC
            """
            result3 = list(client.query(query3).result())
            print(f"\n  剪辑师列表:")
            for r in result3[:10]:
                print(f"    {r.editor_name:15s} | {r.channel:10s} | ${r.spend:>10,.0f} | ${r.revenue:>10,.0f}")
    
    # 3. 检查 xmp_materials 表（素材数据）
    query4 = f"""
    SELECT COUNT(*) as count
    FROM `{PROJECT_ID}.xmp_data.xmp_materials`
    WHERE stat_date = '{date}'
    """
    result4 = list(client.query(query4).result())
    if result4:
        row = result4[0]
        print(f"\n[xmp_materials] 素材数据:")
        print(f"  记录数: {row.count}")

print("\n" + "="*80)

