"""
更新 BigQuery drama_mapping 表，添加 11 个新映射
"""
import os
from dotenv import load_dotenv
from bigquery_storage import BigQueryUploader

load_dotenv()

project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
dataset_id = 'xmp_data'

uploader = BigQueryUploader(project_id=project_id, dataset_id=dataset_id)

# 11 个新映射
new_mappings = [
    ("15000002", "Class In Hypnosis"),
    ("15000031", "Honey or Money"),
    ("15000079", "Tendering Resignation"),
    ("15000201", "I Slept With My Sister's Fiancé"),
    ("15000232", "Blood Mate"),
    ("15000250", "The Night You Erased"),
    ("15000302", "I Accidentally Slept With My Professor"),
    ("15000351", "The Secret Life of Amy Bensen"),
    ("15000793", "Kiss, Only For Study!"),
    ("15000794", "A Love Good for Nothing"),
    ("15000826", "Today I Divorce My Superstar Husband"),
]

print(f"=== 更新 BigQuery drama_mapping 表 ===\n")
print(f"准备插入 {len(new_mappings)} 个新映射\n")

# 构建 INSERT 语句
table_ref = f"{project_id}.{dataset_id}.drama_mapping"

for drama_id, drama_name in new_mappings:
    # 先检查是否已存在
    check_query = f"""
    SELECT COUNT(*) as count
    FROM `{table_ref}`
    WHERE drama_id = '{drama_id}'
    """

    result = uploader.client.query(check_query).result()
    exists = list(result)[0].count > 0

    if exists:
        print(f"[跳过] {drama_id}: {drama_name} (已存在)")
        continue

    # 插入新记录
    insert_query = f"""
    INSERT INTO `{table_ref}` (drama_id, drama_name)
    VALUES ('{drama_id}', '{drama_name}')
    """

    try:
        uploader.client.query(insert_query).result()
        print(f"[成功] {drama_id}: {drama_name}")
    except Exception as e:
        print(f"[失败] {drama_id}: {drama_name} - {e}")

print(f"\n更新完成！")

# 验证更新结果
verify_query = f"""
SELECT COUNT(*) as total_count
FROM `{table_ref}`
"""

result = uploader.client.query(verify_query).result()
total_count = list(result)[0].total_count

print(f"\n当前 drama_mapping 表共有 {total_count} 条记录")
