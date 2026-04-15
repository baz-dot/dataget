"""临时脚本：查询今天所有 batch_id"""
import os
from dotenv import load_dotenv
load_dotenv()
from google.cloud import bigquery

project_id = os.getenv('BQ_PROJECT_ID')
client = bigquery.Client(project=project_id)

query = """
SELECT batch_id, COUNT(*) as rows, SUM(spend) as total_spend
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
WHERE stat_date = '2026-02-26'
GROUP BY batch_id
ORDER BY batch_id
"""
results = client.query(query).result()
print("今天所有 batch:")
for row in results:
    print(f"  {row.batch_id}  |  {row.rows} rows  |  spend: ${row.total_spend:,.2f}")
