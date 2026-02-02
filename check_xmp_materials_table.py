"""检查 xmp_materials 表结构"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
client = bigquery.Client(project=PROJECT_ID)

# 检查表结构
table_ref = f"{PROJECT_ID}.xmp_data.xmp_materials"
table = client.get_table(table_ref)

print("="*80)
print(f"表名: {table.table_id}")
print(f"总行数: {table.num_rows:,}")
print(f"字段数: {len(table.schema)}")
print()

print("字段列表:")
for i, field in enumerate(table.schema, 1):
    print(f"  {i:2d}. {field.name:30s} {field.field_type:15s} {field.mode:10s}")

print()

# 查询示例数据
query = f"""
SELECT *
FROM `{table_ref}`
LIMIT 5
"""

print("示例数据:")
results = list(client.query(query).result())
if results:
    for i, row in enumerate(results, 1):
        print(f"\n记录 {i}:")
        for key, value in dict(row).items():
            print(f"  {key}: {value}")
else:
    print("  没有数据")

print("="*80)

