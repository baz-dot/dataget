"""查 235959 和 backfill 记录的写入时间"""
import os
from dotenv import load_dotenv
load_dotenv()
from google.cloud import bigquery

project_id = os.getenv('BQ_PROJECT_ID')
client = bigquery.Client(project=project_id)

# 1. 02-26 异常 batch
q1 = """
SELECT batch_id, stat_date, total_spend, total_revenue, fetched_at
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_overview`
WHERE stat_date = '2026-02-26'
  AND (batch_id LIKE '%235959%' OR batch_id LIKE '%backfill%')
ORDER BY fetched_at DESC
"""
print("[1] 02-26 异常 batch (235959/backfill):")
for row in client.query(q1).result():
    print(f"  batch={row.batch_id}  spend=${float(row.total_spend or 0):,.2f}  fetched_at={row.fetched_at}")

# 2. 所有 235959 记录
q2 = """
SELECT batch_id, stat_date, total_spend, fetched_at
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_overview`
WHERE batch_id LIKE '%235959%'
ORDER BY stat_date DESC
LIMIT 20
"""
print("\n[2] 所有 235959 batch:")
for row in client.query(q2).result():
    print(f"  date={row.stat_date}  batch={row.batch_id}  spend=${float(row.total_spend or 0):,.2f}  fetched_at={row.fetched_at}")

# 3. 所有 backfill 记录
q3 = """
SELECT batch_id, stat_date, total_spend, fetched_at
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_overview`
WHERE batch_id LIKE '%backfill%'
ORDER BY stat_date DESC
LIMIT 20
"""
print("\n[3] 所有 backfill batch:")
for row in client.query(q3).result():
    print(f"  date={row.stat_date}  batch={row.batch_id}  spend=${float(row.total_spend or 0):,.2f}  fetched_at={row.fetched_at}")

# 4. 02-26 按写入时间倒序
q4 = """
SELECT batch_id, total_spend, fetched_at
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_overview`
WHERE stat_date = '2026-02-26'
ORDER BY fetched_at DESC
LIMIT 10
"""
print("\n[4] 02-26 按写入时间倒序 (最近10条):")
for row in client.query(q4).result():
    print(f"  batch={row.batch_id}  spend=${float(row.total_spend or 0):,.2f}  fetched_at={row.fetched_at}")
