"""验证日报总消耗被 overview 覆盖的问题"""
import os
from dotenv import load_dotenv
load_dotenv()

from bigquery_storage import BigQueryUploader

project_id = os.getenv('BQ_PROJECT_ID')
bq = BigQueryUploader(project_id, 'xmp_data')

table_ref = f'{project_id}.xmp_data.xmp_internal_campaigns_view'
batch_id = bq._get_latest_batch_id(table_ref, '2026-02-25')
print(f'日报使用的 batch_id: {batch_id}\n')

# 1. XMP 原始值
for r in bq.client.query(f'SELECT SUM(spend) as s FROM `{table_ref}` WHERE stat_date="2026-02-25" AND batch_id="{batch_id}"').result():
    print(f'[Step1] XMP 原始总消耗:    ${float(r.s):,.2f}')

# 2. overview 覆盖值
for r in bq.client.query(f'SELECT total_spend FROM `{project_id}.quickbi_data.quickbi_overview` WHERE stat_date="2026-02-25" ORDER BY batch_id DESC LIMIT 1').result():
    print(f'[Step2] Overview 覆盖值:   ${float(r.total_spend):,.2f}  <-- 日报实际用的')

print('\n结论: 日报代码会用 Step2 的值覆盖 Step1，导致总消耗偏低')
