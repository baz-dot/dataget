"""更新 quickbi_overview 表数据"""
from bigquery_storage import BigQueryUploader
import os
from dotenv import load_dotenv
load_dotenv()

bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), 'quickbi_data')

# 更新1月7日的 overview 数据
update_query = """
UPDATE `fleet-blend-469520-n7.quickbi_data.quickbi_overview`
SET total_revenue = 86200.0
WHERE stat_date = '2026-01-07'
"""

print('更新 quickbi_overview 表 2026-01-07 数据...')
bq.client.query(update_query).result()
print('更新完成!')

# 验证
verify_query = """
SELECT stat_date, total_revenue, batch_id
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_overview`
WHERE stat_date = '2026-01-07'
ORDER BY batch_id DESC
LIMIT 3
"""
print('\n验证更新结果:')
for row in bq.client.query(verify_query).result():
    print(f'  {row.stat_date} | ${row.total_revenue:,.2f} | {row.batch_id}')

# 计算新的收支比
spend = 113670.0
revenue = 86200.0
ratio = revenue / spend
print(f'\n新的收支比: ${revenue:,.0f} / ${spend:,.0f} = {ratio:.2%}')
