"""生成1月5号投手日报"""
import os
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd

load_dotenv()

PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
client = bigquery.Client(project=PROJECT_ID)

print("=== 生成 2026-01-05 投手日报 ===")

query = f"""
WITH daily_data AS (
    SELECT
        stat_date,
        optimizer,
        SUM(CASE WHEN LOWER(channel) IN ('meta', 'facebook') THEN spend ELSE 0 END) as meta_spend,
        SUM(CASE WHEN LOWER(channel) IN ('meta', 'facebook') THEN new_user_revenue ELSE 0 END) as meta_revenue,
        SUM(CASE WHEN LOWER(channel) = 'tiktok' THEN spend ELSE 0 END) as tt_spend,
        SUM(CASE WHEN LOWER(channel) = 'tiktok' THEN new_user_revenue ELSE 0 END) as tt_revenue,
        SUM(spend) as total_spend,
        SUM(new_user_revenue) as total_revenue
    FROM `{PROJECT_ID}.quickbi_data.quickbi_campaigns` t1
    WHERE stat_date = '2026-01-05'
      AND batch_id = (
          SELECT MAX(batch_id)
          FROM `{PROJECT_ID}.quickbi_data.quickbi_campaigns` t2
          WHERE t2.stat_date = t1.stat_date
      )
    GROUP BY stat_date, optimizer
),
daily_ranks AS (
    SELECT
        stat_date,
        optimizer,
        meta_spend,
        SAFE_DIVIDE(meta_revenue, meta_spend) as meta_roas,
        tt_spend,
        SAFE_DIVIDE(tt_revenue, tt_spend) as tt_roas,
        total_spend,
        SAFE_DIVIDE(total_revenue, total_spend) as total_roas,
        RANK() OVER (ORDER BY total_spend DESC) as spend_rank,
        RANK() OVER (ORDER BY SAFE_DIVIDE(total_revenue, total_spend) DESC) as roas_rank
    FROM daily_data
    WHERE total_spend > 0
)
SELECT * FROM daily_ranks
ORDER BY total_spend DESC
"""

df = client.query(query).to_dataframe()
print(f"获取 {len(df)} 条记录")

# 添加标注
labels = []
for _, row in df.iterrows():
    parts = []
    if row['spend_rank'] == 1:
        parts.append('Spend Top1')
    if row['roas_rank'] == 1:
        parts.append('ROAS Top1')
    labels.append(', '.join(parts) if parts else '')
df['标注'] = labels

# 重命名列
df = df.rename(columns={
    'stat_date': '日期',
    'optimizer': '投手',
    'meta_spend': 'Meta Spend',
    'meta_roas': 'Meta ROAS',
    'tt_spend': 'TT Spend',
    'tt_roas': 'TT ROAS',
    'total_spend': '总 Spend',
    'total_roas': '总 ROAS'
})

# 选择列
df = df[['日期', '投手', 'Meta Spend', 'Meta ROAS', 'TT Spend', 'TT ROAS', '总 Spend', '总 ROAS', '标注']]

# 保存
output_file = 'daily_report_jan5_optimizer.xlsx'
df.to_excel(output_file, index=False, sheet_name='投手日报')
print(f"已保存: {output_file}")
