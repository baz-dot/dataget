import os
from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery
client = bigquery.Client(project='fleet-blend-469520-n7')

query = '''
SELECT
    batch_id,
    SUM(spend) as total_spend,
    COUNT(*) as record_count
FROM quickbi_data.quickbi_campaigns
WHERE stat_date = '2025-12-24'
GROUP BY batch_id
ORDER BY batch_id DESC
LIMIT 5
'''

result = client.query(query).result()
print('batch_id | total_spend | records')
print('-' * 50)
for row in result:
    print(f'{row.batch_id} | ${row.total_spend:,.2f} | {row.record_count}')
