"""检查1月5日到18日的数据情况"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
client = bigquery.Client(project=PROJECT_ID)

print("="*80)
print("检查 2025年1月5日-18日 的数据情况")
print("="*80)

# 1. 检查投手数据 (quickbi_campaigns)
print("\n[1] 投手数据 (quickbi_campaigns):")
print("-"*80)

query1 = f"""
SELECT
    stat_date,
    COUNT(DISTINCT optimizer) as optimizer_count,
    SUM(spend) as total_spend
FROM `{PROJECT_ID}.quickbi_data.quickbi_campaigns`
WHERE stat_date BETWEEN '2025-01-05' AND '2025-01-18'
GROUP BY stat_date
ORDER BY stat_date
"""

for row in client.query(query1).result():
    print(f"  {row.stat_date}: {row.optimizer_count} 投手, ${row.total_spend:,.2f}")

# 2. 检查剪辑师数据 (xmp_editor_stats)
print("\n[2] 剪辑师数据 (xmp_editor_stats):")
print("-"*80)

query2 = f"""
SELECT
    stat_date,
    COUNT(DISTINCT editor_name) as editor_count,
    SUM(spend) as total_spend
FROM `{PROJECT_ID}.xmp_data.xmp_editor_stats`
WHERE stat_date BETWEEN '2025-01-05' AND '2025-01-18'
GROUP BY stat_date
ORDER BY stat_date
"""

for row in client.query(query2).result():
    print(f"  {row.stat_date}: {row.editor_count} 剪辑师, ${row.total_spend:,.2f}")

print("\n" + "="*80)
