"""
找出 campaigns 中存在但 drama_mapping 中缺失的 drama_id
"""
import os
from dotenv import load_dotenv
from bigquery_storage import BigQueryUploader

load_dotenv()

project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
dataset_id = 'xmp_data'

uploader = BigQueryUploader(project_id=project_id, dataset_id=dataset_id)

# 查询缺失的 drama_id
query = f"""
WITH campaign_dramas AS (
  SELECT DISTINCT drama_id
  FROM `{project_id}.{dataset_id}.xmp_internal_campaigns`
  WHERE drama_id IS NOT NULL
    AND stat_date >= '2026-01-01'
),
mapped_dramas AS (
  SELECT drama_id
  FROM `{project_id}.{dataset_id}.drama_mapping`
)
SELECT
  c.drama_id,
  COUNT(*) as campaign_count
FROM campaign_dramas c
LEFT JOIN mapped_dramas m ON c.drama_id = m.drama_id
WHERE m.drama_id IS NULL
GROUP BY c.drama_id
ORDER BY campaign_count DESC
"""

print("=== 查找缺失的 drama_id ===\n")

try:
    results = uploader.client.query(query).result()

    missing_ids = []
    for row in results:
        missing_ids.append(row.drama_id)
        print(f"Drama ID: {row.drama_id} (出现在 {row.campaign_count} 个不同的 campaign 中)")

    print(f"\n总计缺失 {len(missing_ids)} 个 drama_id")
    print(f"缺失的 ID 列表: {', '.join(missing_ids)}")

except Exception as e:
    print(f"查询失败: {e}")
