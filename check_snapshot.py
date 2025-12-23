from google.cloud import bigquery
import os
from dotenv import load_dotenv
load_dotenv()

client = bigquery.Client(project=os.getenv('BQ_PROJECT_ID'))

query = """
SELECT snapshot_time, hour, total_spend, d0_roas, batch_id
FROM `fleet-blend-469520-n7.quickbi_data.hourly_snapshots`
ORDER BY snapshot_time DESC
LIMIT 10
"""

print('查询快照记录...')
for row in client.query(query).result():
    print(f'  {row.snapshot_time} | hour={row.hour} | spend=${row.total_spend:,.2f} | roas={row.d0_roas:.1%} | {row.batch_id}')
