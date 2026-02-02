"""
测试昨天 batch 数据详情
"""
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from bigquery_storage import BigQueryUploader

project_id = os.getenv('BQ_PROJECT_ID')
bq = BigQueryUploader(project_id, "quickbi_data")

print("=" * 60)
print("测试昨天 batch 数据详情")
print("=" * 60)

# 计算昨天日期和当前小时
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
current_hour = datetime.now().hour

print(f"\n查询参数:")
print(f"  昨天日期: {yesterday}")
print(f"  当前小时: {current_hour}")

# 构建查询范围
yesterday_date_str = yesterday.replace('-', '')
yesterday_hour_start = f"{yesterday_date_str}_{current_hour:02d}0000"
yesterday_hour_end = f"{yesterday_date_str}_{current_hour:02d}1000"

print(f"  查询范围: {yesterday_hour_start} - {yesterday_hour_end}")

# 查询昨天该时刻的所有 batch
table_ref = f"{project_id}.quickbi_data.quickbi_campaigns"
query = f"""
SELECT batch_id,
       COUNT(*) as row_count,
       SUM(spend) as total_spend,
       SUM(new_user_revenue) as new_user_revenue,
       SUM(media_user_revenue) as media_user_revenue,
       SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas
FROM `{table_ref}`
WHERE stat_date = '{yesterday}'
  AND batch_id >= '{yesterday_hour_start}'
  AND batch_id <= '{yesterday_hour_end}'
GROUP BY batch_id
ORDER BY batch_id DESC
"""

print(f"\n执行查询...")
results = list(bq.client.query(query).result())

if not results:
    print(f"\n未找到昨天 {current_hour}:00-{current_hour}:10 范围内的 batch 数据")
    print("可能原因:")
    print("  1. 昨天该时刻没有数据同步")
    print("  2. 数据还未到达该时刻")
else:
    print(f"\n找到 {len(results)} 个 batch:")
    for row in results:
        print(f"\n  batch_id: {row.batch_id}")
        print(f"    行数: {row.row_count}")
        print(f"    总消耗: ${row.total_spend:,.2f}")
        print(f"    新用户收入: ${row.new_user_revenue or 0:,.2f}")
        print(f"    媒体归因收入: ${row.media_user_revenue or 0:,.2f}")
        print(f"    Media ROAS: {row.media_roas or 0:.1%}")

print("\n" + "=" * 60)
