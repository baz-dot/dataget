"""查询线上最新 batch_id 数据概况"""
import os
from dotenv import load_dotenv
load_dotenv()
from google.cloud import bigquery

project_id = os.getenv('BQ_PROJECT_ID')
client = bigquery.Client(project=project_id)

# 1. 最新 batch 汇总
query = """
WITH latest AS (
  SELECT MAX(batch_id) as bid
  FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
)
SELECT
  c.batch_id,
  c.stat_date,
  COUNT(*) as row_count,
  COUNT(DISTINCT c.optimizer) as optimizer_count,
  COUNT(DISTINCT c.drama_name) as drama_count,
  COUNT(DISTINCT c.channel) as channel_count,
  COUNT(DISTINCT c.country) as country_count,
  ROUND(SUM(c.spend), 2) as total_spend,
  ROUND(SUM(c.new_user_revenue), 2) as total_revenue,
  ROUND(SUM(c.media_user_revenue), 2) as total_media_revenue,
  ROUND(SAFE_DIVIDE(SUM(c.new_user_revenue), SUM(c.spend)) * 100, 2) as d0_roas_pct,
  SUM(c.new_users) as total_new_users,
  SUM(c.new_payers) as total_new_payers
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns` c
JOIN latest l ON c.batch_id = l.bid
GROUP BY c.batch_id, c.stat_date
ORDER BY c.stat_date DESC
"""
results = list(client.query(query).result())
print(f"{'='*60}")
print(f"最新 batch_id: {results[0].batch_id}")
print(f"fetched 时间: 2026-03-10 10:04 (北京时间)")
print(f"包含 stat_date 数量: {len(results)}")
print(f"{'='*60}\n")
for row in results:
    print(f"[stat_date: {row.stat_date}]")
    print(f"  行数: {row.row_count} | 投手: {row.optimizer_count} | 剧集: {row.drama_count} | 渠道: {row.channel_count} | 国家: {row.country_count}")
    print(f"  Spend: ${row.total_spend:,.2f} | Revenue: ${row.total_revenue:,.2f} | Media Rev: ${row.total_media_revenue:,.2f}")
    print(f"  D0 ROAS: {row.d0_roas_pct}% | 新用户: {row.total_new_users:,} | 新付费: {row.total_new_payers:,}")
    print()

# 2. 投手维度 (最新 stat_date)
latest_date = results[0].stat_date
query2 = f"""
WITH latest AS (
  SELECT MAX(batch_id) as bid
  FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
)
SELECT
  c.optimizer,
  ROUND(SUM(c.spend), 2) as spend,
  ROUND(SUM(c.new_user_revenue), 2) as revenue,
  ROUND(SUM(c.media_user_revenue), 2) as media_rev,
  ROUND(SAFE_DIVIDE(SUM(c.new_user_revenue), SUM(c.spend)) * 100, 2) as d0_roas_pct,
  COUNT(DISTINCT c.drama_name) as drama_count,
  SUM(c.new_users) as new_users
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns` c
JOIN latest l ON c.batch_id = l.bid
WHERE c.stat_date = '{latest_date}'
GROUP BY c.optimizer
HAVING SUM(c.spend) > 0
ORDER BY spend DESC
"""
results2 = list(client.query(query2).result())
print(f"{'='*60}")
print(f"投手维度 (stat_date: {latest_date})")
print(f"{'='*60}\n")
for row in results2:
    name = row.optimizer or "(空)"
    print(f"  {name}: Spend ${row.spend:,.2f} | Rev ${row.revenue:,.2f} | MediaRev ${row.media_rev:,.2f} | ROAS {row.d0_roas_pct}% | 剧集 {row.drama_count} | 新用户 {row.new_users:,}")

# 3. 有问题的数据: channel/optimizer/drama 为空但有 spend
query3 = """
WITH latest AS (
  SELECT MAX(batch_id) as bid
  FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
)
SELECT
  COUNT(*) as empty_rows,
  SUM(spend) as empty_spend,
  SUM(CASE WHEN optimizer = '' OR optimizer IS NULL THEN spend ELSE 0 END) as no_optimizer_spend,
  SUM(CASE WHEN channel = '' OR channel IS NULL THEN spend ELSE 0 END) as no_channel_spend,
  SUM(CASE WHEN drama_name = '' OR drama_name IS NULL THEN spend ELSE 0 END) as no_drama_spend
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns` c
JOIN latest l ON c.batch_id = l.bid
WHERE c.stat_date = (SELECT MAX(stat_date) FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns` c2 JOIN latest l2 ON c2.batch_id = l2.bid)
  AND (optimizer = '' OR optimizer IS NULL OR channel = '' OR channel IS NULL OR drama_name = '' OR drama_name IS NULL)
"""
results3 = list(client.query(query3).result())
print(f"\n{'='*60}")
print(f"数据完整性检查")
print(f"{'='*60}\n")
for row in results3:
    print(f"  空字段行数: {row.empty_rows}")
    print(f"  无投手的 spend: ${row.no_optimizer_spend:,.2f}")
    print(f"  无渠道的 spend: ${row.no_channel_spend:,.2f}")
    print(f"  无剧名的 spend: ${row.no_drama_spend:,.2f}")
