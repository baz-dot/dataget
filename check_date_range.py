"""检查数据库中实际的日期范围"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
client = bigquery.Client(project=PROJECT_ID)

print("=== quickbi_campaigns date range ===")
query1 = f"""
SELECT MIN(stat_date) as min_date, MAX(stat_date) as max_date
FROM `{PROJECT_ID}.quickbi_data.quickbi_campaigns`
"""
for row in client.query(query1).result():
    print(f"Min: {row.min_date}, Max: {row.max_date}")

print()
print("=== xmp_editor_stats date range ===")
query2 = f"""
SELECT MIN(stat_date) as min_date, MAX(stat_date) as max_date
FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
"""
for row in client.query(query2).result():
    print(f"Min: {row.min_date}, Max: {row.max_date}")
