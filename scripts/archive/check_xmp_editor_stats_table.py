"""检查 xmp_editor_stats 表结构和数据"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

# 加载环境变量
load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')

print("="*80)
print("检查 xmp_editor_stats 表结构和数据")
print("="*80)
print(f"Project: {PROJECT_ID}")
print()

client = bigquery.Client(project=PROJECT_ID)

# 1. 检查表结构
print("[1] 表结构（字段列表）:")
print("-"*80)

table_ref = f"{PROJECT_ID}.xmp_data.xmp_editor_stats"
table = client.get_table(table_ref)

print(f"表名: {table.table_id}")
print(f"总行数: {table.num_rows:,}")
print(f"字段数: {len(table.schema)}")
print()

print("字段列表:")
for i, field in enumerate(table.schema, 1):
    print(f"  {i:2d}. {field.name:30s} {field.field_type:15s} {field.mode:10s}")

print()

# 2. 检查最近的数据
print("[2] 最近7天的数据统计:")
print("-"*80)

query = f"""
SELECT 
    stat_date,
    batch_id,
    COUNT(*) as record_count,
    COUNT(DISTINCT editor_name) as editor_count,
    SUM(spend) as total_spend,
    SUM(revenue) as total_revenue,
    SUM(material_count) as total_material_count,
    SUM(hot_count) as total_hot_count
FROM `{table_ref}`
WHERE stat_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY stat_date, batch_id
ORDER BY stat_date DESC, batch_id DESC
LIMIT 20
"""

print("查询SQL:")
print(query)
print()

results = list(client.query(query).result())

if results:
    print(f"找到 {len(results)} 条记录:")
    print()
    for row in results:
        print(f"日期: {row.stat_date} | Batch: {row.batch_id}")
        print(f"  记录数: {row.record_count}")
        print(f"  剪辑师数: {row.editor_count}")
        print(f"  总消耗: ${row.total_spend:,.2f}")
        print(f"  总收入: ${row.total_revenue:,.2f}")
        print(f"  素材数: {row.total_material_count}")
        print(f"  爆款数: {row.total_hot_count}")
        print()
else:
    print("⚠️ 没有找到数据")

# 3. 检查具体的剪辑师数据（最新一天）
print("[3] 最新一天的剪辑师详细数据:")
print("-"*80)

query2 = f"""
SELECT 
    stat_date,
    batch_id,
    editor_name,
    channel,
    spend,
    revenue,
    roas,
    material_count,
    hot_count,
    hot_rate,
    top_material,
    top_material_spend
FROM `{table_ref}`
WHERE stat_date = (SELECT MAX(stat_date) FROM `{table_ref}`)
  AND batch_id = (SELECT MAX(batch_id) FROM `{table_ref}` WHERE stat_date = (SELECT MAX(stat_date) FROM `{table_ref}`))
ORDER BY spend DESC
LIMIT 10
"""

results2 = list(client.query(query2).result())

if results2:
    print(f"找到 {len(results2)} 条剪辑师记录:")
    print()
    for i, row in enumerate(results2, 1):
        print(f"{i}. {row.editor_name} ({row.channel})")
        print(f"   消耗: ${row.spend:,.2f} | 收入: ${row.revenue:,.2f} | ROAS: {row.roas:.1%}")
        print(f"   素材数: {row.material_count} | 爆款数: {row.hot_count} | 爆款率: {row.hot_rate:.1%}")
        if row.top_material:
            print(f"   Top素材: {row.top_material} (${row.top_material_spend:,.2f})")
        print()
else:
    print("⚠️ 没有找到数据")

print("="*80)
print("检查完成")
print("="*80)

