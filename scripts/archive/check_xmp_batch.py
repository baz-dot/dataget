"""查看 XMP 最新 batch 数据"""
import os, sys
from dotenv import load_dotenv
load_dotenv()
from bigquery_storage import BigQueryUploader

bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), 'xmp_data')

# 查最新5个 batch
query = """
SELECT batch_id, COUNT(*) as row_count,
       SUM(spend) as total_spend,
       SUM(revenue) as total_revenue,
       SAFE_DIVIDE(SUM(revenue), SUM(spend)) as roas,
       MIN(fetched_at) as fetched_at
FROM xmp_data.xmp_internal_campaigns
WHERE stat_date = '2026-02-27'
GROUP BY batch_id
ORDER BY batch_id DESC
LIMIT 5
"""
results = bq.client.query(query).result()
print("=== XMP 最新 batch 数据 (2026-02-27) ===\n")
for row in results:
    print(f"batch={row.batch_id} | rows={row.row_count} | spend=${row.total_spend:,.2f} | revenue=${row.total_revenue:,.2f} | roas={row.roas:.2%} | fetched={row.fetched_at}")
