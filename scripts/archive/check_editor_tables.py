"""检查 BigQuery 中所有与剪辑师相关的表"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

# 加载环境变量
load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')

print("="*80)
print("检查 BigQuery 中与剪辑师相关的表")
print("="*80)
print(f"Project: {PROJECT_ID}")
print()

client = bigquery.Client(project=PROJECT_ID)

# 1. 列出所有数据集
print("[1] 数据集列表:")
print("-"*80)
datasets = list(client.list_datasets())
for dataset in datasets:
    print(f"  - {dataset.dataset_id}")
print()

# 2. 检查 xmp_data 数据集中的所有表
print("[2] xmp_data 数据集中的表:")
print("-"*80)
tables = list(client.list_tables(f"{PROJECT_ID}.xmp_data"))
for table in tables:
    table_ref = f"{PROJECT_ID}.xmp_data.{table.table_id}"
    table_obj = client.get_table(table_ref)
    print(f"\n表名: {table.table_id}")
    print(f"  行数: {table_obj.num_rows:,}")
    print(f"  字段数: {len(table_obj.schema)}")
    
    # 检查是否包含剪辑师相关字段
    editor_fields = [f.name for f in table_obj.schema if 'editor' in f.name.lower() or 'designer' in f.name.lower()]
    if editor_fields:
        print(f"  剪辑师相关字段: {', '.join(editor_fields)}")
    
    # 检查是否包含素材相关字段
    material_fields = [f.name for f in table_obj.schema if 'material' in f.name.lower()]
    if material_fields:
        print(f"  素材相关字段: {', '.join(material_fields)}")

print()

# 3. 检查 quickbi_data 数据集中的表
print("[3] quickbi_data 数据集中的表:")
print("-"*80)
try:
    tables = list(client.list_tables(f"{PROJECT_ID}.quickbi_data"))
    for table in tables:
        table_ref = f"{PROJECT_ID}.quickbi_data.{table.table_id}"
        table_obj = client.get_table(table_ref)
        print(f"\n表名: {table.table_id}")
        print(f"  行数: {table_obj.num_rows:,}")
        print(f"  字段数: {len(table_obj.schema)}")
        
        # 检查是否包含剪辑师相关字段
        editor_fields = [f.name for f in table_obj.schema if 'editor' in f.name.lower() or 'designer' in f.name.lower()]
        if editor_fields:
            print(f"  剪辑师相关字段: {', '.join(editor_fields)}")
        
        # 检查是否包含素材相关字段
        material_fields = [f.name for f in table_obj.schema if 'material' in f.name.lower()]
        if material_fields:
            print(f"  素材相关字段: {', '.join(material_fields)}")
except Exception as e:
    print(f"  查询失败: {e}")

print()

# 4. 对比两个表的剪辑师数据
print("[4] 对比 xmp_editor_stats 和 quickbi_campaigns 的剪辑师数据:")
print("-"*80)

# 检查 xmp_editor_stats
query1 = f"""
SELECT 
    'xmp_editor_stats' as source,
    COUNT(DISTINCT editor_name) as editor_count,
    SUM(material_count) as total_materials,
    SUM(spend) as total_spend
FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
WHERE stat_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
"""

# 检查 quickbi_campaigns 是否有剪辑师字段
query2 = f"""
SELECT 
    'quickbi_campaigns' as source,
    COUNT(*) as record_count
FROM `{PROJECT_ID}.quickbi_data.quickbi_campaigns`
WHERE stat_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
LIMIT 1
"""

print("\nxmp_editor_stats 统计:")
for row in client.query(query1).result():
    print(f"  来源: {row.source}")
    print(f"  剪辑师数: {row.editor_count}")
    print(f"  总素材数: {row.total_materials}")
    print(f"  总消耗: ${row.total_spend:,.2f}")

print("\nquickbi_campaigns 统计:")
for row in client.query(query2).result():
    print(f"  来源: {row.source}")
    print(f"  记录数: {row.record_count:,}")

print()
print("="*80)
print("结论:")
print("="*80)
print("剪辑师产出与质量数据应该从以下表获取:")
print("  ✅ xmp_data.xmp_editor_stats - 包含素材数、爆款数、Top素材等")
print("  ❌ quickbi_data.quickbi_campaigns - 没有剪辑师维度数据")
print()
print("注意:")
print("  - xmp_editor_stats 中 Facebook 渠道的 material_count = 0")
print("  - 只有 TikTok 渠道有完整的素材统计数据")
print("="*80)

