from bigquery_storage import BigQueryUploader
import os
from dotenv import load_dotenv
load_dotenv()

bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), os.getenv('BQ_DATASET_ID'))

# 查看快照表的所有数据
query = f"""
SELECT snapshot_time, stat_date, hour, total_spend, d0_roas, batch_id
FROM `{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET_ID')}.hourly_snapshots`
ORDER BY snapshot_time DESC
LIMIT 20
"""
print('=== 快照表数据 ===')
for row in bq.client.query(query).result():
    print(f"{row.snapshot_time} | hour={row.hour} | spend=${row.total_spend:,.2f} | roas={row.d0_roas:.1%}")
