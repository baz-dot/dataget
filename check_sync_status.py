"""检查 Quick BI 数据同步状态"""
import sys
import io
import os

# 设置 UTF-8 编码
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from dotenv import load_dotenv
load_dotenv()

# 设置 GCP 凭证
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './fleet-blend-469520-n7-1a29eac22376.json'

from google.cloud import bigquery
from datetime import datetime

client = bigquery.Client(project='fleet-blend-469520-n7')

# 查询最近的 batch_id 记录
query = """
SELECT
    stat_date,
    batch_id,
    COUNT(*) as record_count,
    SUM(spend) as total_spend
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
WHERE stat_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
GROUP BY stat_date, batch_id
ORDER BY batch_id DESC
LIMIT 20
"""

print('=== 最近3天的数据同步记录 ===')
print(f'查询时间: {datetime.now()}')
print()

for row in client.query(query).result():
    print(f'日期: {row.stat_date} | batch_id: {row.batch_id} | 记录数: {row.record_count} | 总消耗: ${row.total_spend:,.2f}')

print()
print('=== 今日同步情况 ===')
today = datetime.now().strftime('%Y-%m-%d')
today_query = f"""
SELECT
    batch_id,
    COUNT(*) as record_count,
    MIN(fetched_at) as first_fetch,
    MAX(fetched_at) as last_fetch
FROM `fleet-blend-469520-n7.quickbi_data.quickbi_campaigns`
WHERE stat_date = '{today}'
GROUP BY batch_id
ORDER BY batch_id DESC
"""

rows = list(client.query(today_query).result())
if rows:
    for row in rows:
        print(f'batch_id: {row.batch_id} | 记录数: {row.record_count}')
else:
    print(f'今日 ({today}) 暂无数据同步记录')
