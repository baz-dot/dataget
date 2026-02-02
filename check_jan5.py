"""检查1月5号数据"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
client = bigquery.Client(project=PROJECT_ID)

print("=== 检查 2026-01-05 数据 ===")

# 剪辑师数据
query = f"""
SELECT stat_date, COUNT(*) as cnt, SUM(spend) as spend
FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
WHERE stat_date = '2026-01-05'
GROUP BY stat_date
"""
results = list(client.query(query).result())
if results:
    for r in results:
        print(f"剪辑师: {r.stat_date}, {r.cnt} 条, ${r.spend:,.2f}")
else:
    print("剪辑师: 无数据")

# 投手数据
query2 = f"""
SELECT stat_date, COUNT(*) as cnt, SUM(spend) as spend
FROM `{PROJECT_ID}.quickbi_data.quickbi_campaigns`
WHERE stat_date = '2026-01-05'
GROUP BY stat_date
"""
results2 = list(client.query(query2).result())
if results2:
    for r in results2:
        print(f"投手: {r.stat_date}, {r.cnt} 条, ${r.spend:,.2f}")
else:
    print("投手: 无数据")
