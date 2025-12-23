"""检查 spend 数据"""
from bigquery_storage import BigQueryUploader
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), os.getenv('BQ_DATASET_ID'))

yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f'=== 查询 {yesterday} 的原始数据 ===')

# 查询投手消耗
query = f"""
SELECT
    optimizer,
    SUM(spend) as spend,
    SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas,
    COUNT(DISTINCT campaign_id) as campaigns
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
WHERE stat_date = '{yesterday}'
  AND optimizer IS NOT NULL
  AND optimizer != ''
GROUP BY optimizer
ORDER BY spend DESC
"""

print('\n投手消耗 (BigQuery 原始数据):')
total = 0
for row in bq.client.query(query).result():
    print(f'  {row.optimizer}: ${row.spend:,.2f} | ROAS {row.roas:.1%} | {row.campaigns} campaigns')
    total += row.spend

print(f'\n投手总消耗: ${total:,.2f}')

# 查询大盘总消耗 (含无投手的数据)
query2 = f"""
SELECT SUM(spend) as total_spend
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
WHERE stat_date = '{yesterday}'
"""
for row in bq.client.query(query2).result():
    print(f'大盘总消耗 (含无投手): ${row.total_spend:,.2f}')
    diff = row.total_spend - total
    if diff > 0:
        print(f'差额 (无投手的消耗): ${diff:,.2f}')

# 检查无投手的数据
query3 = f"""
SELECT
    optimizer,
    SUM(spend) as spend,
    COUNT(*) as rows
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
WHERE stat_date = '{yesterday}'
  AND (optimizer IS NULL OR optimizer = '')
GROUP BY optimizer
"""
print('\n无投手的数据:')
for row in bq.client.query(query3).result():
    print(f'  optimizer="{row.optimizer}": ${row.spend:,.2f} ({row.rows} rows)')
