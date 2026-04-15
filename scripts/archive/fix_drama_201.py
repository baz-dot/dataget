"""
修复插入失败的 drama_id: 15000201
使用参数化查询避免 SQL 转义问题
"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
dataset_id = 'xmp_data'

client = bigquery.Client(project=project_id)
table_ref = f"{project_id}.{dataset_id}.drama_mapping"

# 失败的映射
drama_id = "15000201"
drama_name = "I Slept With My Sister's Fiancé"

print(f"=== 修复插入失败的记录 ===\n")
print(f"Drama ID: {drama_id}")
print(f"Drama Name: {drama_name}\n")

# 先检查是否已存在
check_query = f"""
SELECT COUNT(*) as count
FROM `{table_ref}`
WHERE drama_id = @drama_id
"""

job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("drama_id", "STRING", drama_id)
    ]
)

result = client.query(check_query, job_config=job_config).result()
exists = list(result)[0].count > 0

if exists:
    print(f"[跳过] 记录已存在")
else:
    # 使用参数化查询插入
    insert_query = f"""
    INSERT INTO `{table_ref}` (drama_id, drama_name)
    VALUES (@drama_id, @drama_name)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("drama_id", "STRING", drama_id),
            bigquery.ScalarQueryParameter("drama_name", "STRING", drama_name)
        ]
    )

    try:
        client.query(insert_query, job_config=job_config).result()
        print(f"[成功] 插入成功！")
    except Exception as e:
        print(f"[失败] {e}")

# 验证最终结果
verify_query = f"SELECT COUNT(*) as total FROM `{table_ref}`"
result = client.query(verify_query).result()
total = list(result)[0].total

print(f"\n当前 drama_mapping 表共有 {total} 条记录")
