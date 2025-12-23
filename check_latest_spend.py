"""查询最新批次的 spend 数据"""
from bigquery_storage import BigQueryUploader
import os
from dotenv import load_dotenv
load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID')
QUICKBI_DATASET = os.getenv('QUICKBI_BQ_DATASET_ID', 'quickbi_data')

bq = BigQueryUploader(PROJECT_ID, QUICKBI_DATASET)

query = f"""
SELECT
    batch_id,
    stat_date,
    SUM(spend) as total_spend,
    COUNT(*) as row_count,
    COUNT(DISTINCT campaign_id) as campaigns
FROM `{PROJECT_ID}.{QUICKBI_DATASET}.quickbi_campaigns`
WHERE stat_date = '2025-12-23'
GROUP BY batch_id, stat_date
ORDER BY batch_id DESC
LIMIT 5
"""

print('最近批次的 spend 数据 (2025-12-23):')
print('-' * 80)
for row in bq.client.query(query).result():
    print(f'批次: {row.batch_id} | spend: ${row.total_spend:,.2f} | 记录数: {row.row_count} | campaigns: {row.campaigns}')
