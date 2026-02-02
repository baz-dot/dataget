"""
验证 drama_mapping 更新是否生效
检查视图中的 drama_name 是否正确显示
"""
import os
from dotenv import load_dotenv
from bigquery_storage import BigQueryUploader

load_dotenv()

project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
dataset_id = 'xmp_data'

uploader = BigQueryUploader(project_id=project_id, dataset_id=dataset_id)

# 新添加的 11 个 drama_id
new_drama_ids = [
    '15000002', '15000031', '15000079', '15000201', '15000232',
    '15000250', '15000302', '15000351', '15000793', '15000794', '15000826'
]

print("=== 验证 drama_mapping 更新结果 ===\n")

# 查询视图，检查这些 drama_id 的 drama_name 是否正确显示
query = f"""
SELECT
    drama_id,
    drama_name,
    COUNT(*) as campaign_count
FROM `{project_id}.{dataset_id}.xmp_internal_campaigns_view`
WHERE drama_id IN ({','.join([f"'{id}'" for id in new_drama_ids])})
    AND stat_date >= '2026-01-15'
GROUP BY drama_id, drama_name
ORDER BY drama_id
"""

print("查询视图中的 drama_name 字段...\n")

try:
    results = uploader.client.query(query).result()

    success_count = 0
    fail_count = 0

    for row in results:
        if row.drama_name and row.drama_name != '':
            print(f"[OK] {row.drama_id}: {row.drama_name} ({row.campaign_count} campaigns)")
            success_count += 1
        else:
            print(f"[FAIL] {row.drama_id}: <nil> ({row.campaign_count} campaigns)")
            fail_count += 1

    print(f"\n验证结果:")
    print(f"  成功: {success_count} 个 drama_id 显示正确")
    print(f"  失败: {fail_count} 个 drama_id 仍显示 <nil>")

    if fail_count == 0:
        print(f"\n✓ 所有新添加的 drama_id 都已正确显示 drama_name！")
    else:
        print(f"\n✗ 仍有 {fail_count} 个 drama_id 显示为 <nil>")

except Exception as e:
    print(f"查询失败: {e}")
