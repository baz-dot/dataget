"""
测试修复后的昨天 batch 查询（带行数过滤）
"""
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from bigquery_storage import BigQueryUploader

project_id = os.getenv('BQ_PROJECT_ID')
bq = BigQueryUploader(project_id, "quickbi_data")

print("=" * 60)
print("测试修复后的昨天 batch 查询")
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

# 使用新的查询逻辑（带行数过滤）
table_ref = f"{project_id}.quickbi_data.quickbi_campaigns"
query = f"""
SELECT batch_id, COUNT(*) as row_count
FROM `{table_ref}`
WHERE stat_date = '{yesterday}'
  AND batch_id >= '{yesterday_hour_start}'
  AND batch_id <= '{yesterday_hour_end}'
GROUP BY batch_id
HAVING COUNT(*) < 2500
ORDER BY batch_id DESC
LIMIT 1
"""

print(f"\n执行查询（过滤行数 > 2500 的异常 batch）...")
results = list(bq.client.query(query).result())

if not results:
    print(f"\n未找到符合条件的 batch")
else:
    batch_id = results[0].batch_id
    row_count = results[0].row_count

    print(f"\n找到 batch: {batch_id}")
    print(f"  行数: {row_count}")

    # 查询该 batch 的详细数据
    detail_query = f"""
    SELECT
        SUM(spend) as total_spend,
        SUM(new_user_revenue) as new_user_revenue,
        SUM(media_user_revenue) as media_user_revenue,
        SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas
    FROM `{table_ref}`
    WHERE stat_date = '{yesterday}'
      AND batch_id = '{batch_id}'
    """

    detail = list(bq.client.query(detail_query).result())[0]
    print(f"  总消耗: ${detail.total_spend:,.2f}")
    print(f"  新用户收入: ${detail.new_user_revenue or 0:,.2f}")
    print(f"  媒体归因收入: ${detail.media_user_revenue or 0:,.2f}")
    print(f"  Media ROAS: {detail.media_roas or 0:.1%}")

print("\n" + "=" * 60)
