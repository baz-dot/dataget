"""验证 lark_bot 使用的收入字段"""
from lark.lark_bot import LarkBot
from bigquery_storage import BigQueryUploader
import os
from dotenv import load_dotenv

load_dotenv()

# 查询数据
bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), 'quickbi_data')
data = bq.query_realtime_report_data()

# 模拟 lark_bot 中的逻辑
summary = data.get('summary', {})
total_revenue = summary.get('total_media_revenue', 0)
total_revenue_d0 = summary.get('total_revenue', 0)

print(f'D0收入 (total_revenue): ${total_revenue_d0:,.2f}')
print(f'归因收入 (total_media_revenue): ${total_revenue:,.2f}')
print(f'差额: ${total_revenue - total_revenue_d0:,.2f}')
print(f'\n✅ lark_bot 现在使用的是: total_media_revenue = ${total_revenue:,.2f}')
