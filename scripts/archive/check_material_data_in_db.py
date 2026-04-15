"""检查数据库中是否有素材数据"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

# 加载环境变量
load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')

print("="*80)
print("检查数据库中的素材数据")
print("="*80)
print(f"Project: {PROJECT_ID}")
print()

client = bigquery.Client(project=PROJECT_ID)

# 1. 检查 xmp_editor_stats 表中是否有素材数据
print("[1] 检查 xmp_editor_stats 表中的素材数据:")
print("-"*80)

query1 = f"""
SELECT 
    channel,
    COUNT(*) as total_records,
    COUNT(DISTINCT editor_name) as editor_count,
    SUM(CASE WHEN material_count > 0 THEN 1 ELSE 0 END) as records_with_materials,
    SUM(material_count) as total_materials,
    SUM(spend) as total_spend,
    SUM(hot_count) as total_hot_count
FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
WHERE stat_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY channel
ORDER BY channel
"""

print("查询SQL:")
print(query1)
print()

results1 = list(client.query(query1).result())

if results1:
    print("按渠道统计:")
    for row in results1:
        print(f"\n渠道: {row.channel}")
        print(f"  总记录数: {row.total_records}")
        print(f"  剪辑师数: {row.editor_count}")
        print(f"  有素材数据的记录: {row.records_with_materials}")
        print(f"  总素材数: {row.total_materials}")
        print(f"  总消耗: ${row.total_spend:,.2f}")
        print(f"  总爆款数: {row.total_hot_count}")
else:
    print("⚠️ 没有找到数据")

print()

# 2. 查看最近一周有素材数据的记录
print("[2] 最近一周有素材数据的记录（material_count > 0）:")
print("-"*80)

query2 = f"""
SELECT 
    stat_date,
    batch_id,
    channel,
    editor_name,
    material_count,
    spend,
    revenue,
    roas,
    hot_count,
    hot_rate
FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
WHERE stat_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND material_count > 0
ORDER BY stat_date DESC, spend DESC
LIMIT 20
"""

results2 = list(client.query(query2).result())

if results2:
    print(f"找到 {len(results2)} 条有素材数据的记录:")
    print()
    for i, row in enumerate(results2, 1):
        print(f"{i}. {row.stat_date} | {row.channel} | {row.editor_name}")
        print(f"   素材数: {row.material_count} | 消耗: ${row.spend:,.2f} | ROAS: {row.roas:.1%}")
        print(f"   爆款数: {row.hot_count} | 爆款率: {row.hot_rate:.1%}")
        print()
else:
    print("⚠️ 没有找到有素材数据的记录")

print()

# 3. 查看周报查询的数据（模拟 query_weekly_report_data 的查询）
print("[3] 模拟周报查询（2026-01-12 ~ 2026-01-18）:")
print("-"*80)

query3 = f"""
SELECT
    editor_name as name,
    channel,
    SUM(material_count) as total_material_count,
    SUM(spend) as total_spend,
    SUM(revenue) as total_revenue,
    SAFE_DIVIDE(SUM(revenue), SUM(spend)) as roas,
    SUM(hot_count) as total_hot_count,
    SAFE_DIVIDE(SUM(hot_count), SUM(material_count)) as hot_rate
FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
WHERE stat_date BETWEEN '2026-01-12' AND '2026-01-18'
    AND editor_name IS NOT NULL
    AND editor_name != ''
GROUP BY editor_name, channel
HAVING SUM(spend) > 0
ORDER BY total_spend DESC
LIMIT 20
"""

results3 = list(client.query(query3).result())

if results3:
    print(f"找到 {len(results3)} 条剪辑师记录:")
    print()
    for i, row in enumerate(results3, 1):
        print(f"{i}. {row.name} ({row.channel})")
        print(f"   素材数: {row.total_material_count} | 消耗: ${row.total_spend:,.2f}")
        print(f"   ROAS: {row.roas:.1%} | 爆款数: {row.total_hot_count} | 爆款率: {row.hot_rate:.1%}")
        print()
else:
    print("⚠️ 没有找到数据")

print()

# 4. 检查是否按 editor_name 聚合导致数据丢失
print("[4] 检查按 editor_name 聚合（不分渠道）:")
print("-"*80)

query4 = f"""
SELECT
    editor_name as name,
    SUM(material_count) as total_material_count,
    SUM(spend) as total_spend,
    SUM(revenue) as total_revenue,
    SAFE_DIVIDE(SUM(revenue), SUM(spend)) as roas,
    SUM(hot_count) as total_hot_count,
    SAFE_DIVIDE(SUM(hot_count), SUM(material_count)) as hot_rate
FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
WHERE stat_date BETWEEN '2026-01-12' AND '2026-01-18'
    AND editor_name IS NOT NULL
    AND editor_name != ''
GROUP BY editor_name
HAVING SUM(spend) > 0
ORDER BY total_spend DESC
LIMIT 20
"""

results4 = list(client.query(query4).result())

if results4:
    print(f"找到 {len(results4)} 条剪辑师记录（合并所有渠道）:")
    print()
    for i, row in enumerate(results4, 1):
        print(f"{i}. {row.name}")
        print(f"   素材数: {row.total_material_count} | 消耗: ${row.total_spend:,.2f}")
        print(f"   ROAS: {row.roas:.1%} | 爆款数: {row.total_hot_count} | 爆款率: {row.hot_rate:.1%}")
        print()
else:
    print("⚠️ 没有找到数据")

print("="*80)
print("检查完成")
print("="*80)

