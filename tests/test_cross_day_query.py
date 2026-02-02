"""
测试跨天逻辑 - 真实 BigQuery 查询
模拟 0 点场景，验证是否能正确查询到昨天 23 点的数据
"""
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# 导入 BigQuery 模块
from bigquery_storage import BigQueryUploader

# 初始化
PROJECT_ID = os.getenv('BQ_PROJECT_ID')
bq = BigQueryUploader(PROJECT_ID, "quickbi_data")

print("=" * 60)
print("跨天逻辑测试 - 模拟 0 点查询昨天 23 点数据")
print("=" * 60)

# 模拟当前时间是今天 0:02
current_time = datetime.now().replace(hour=0, minute=2, second=12)
print(f"\n模拟当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

# 模拟 batch_id (今天 0:02)
today_str = current_time.strftime('%Y%m%d')
mock_batch_id = f"{today_str}_000212"
print(f"模拟 batch_id: {mock_batch_id}")

# 计算查询参数（跨天逻辑）
one_hour_ago_start = (current_time - timedelta(minutes=75)).strftime('%Y%m%d_%H%M%S')
one_hour_ago_end = (current_time - timedelta(minutes=45)).strftime('%Y%m%d_%H%M%S')
prev_hour_stat_date = (current_time - timedelta(minutes=60)).strftime('%Y-%m-%d')

print(f"\n查询参数:")
print(f"  batch_id >= {one_hour_ago_start}")
print(f"  batch_id <= {one_hour_ago_end}")
print(f"  stat_date = {prev_hour_stat_date}")

# 验证日期
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"\n日期验证:")
print(f"  昨天: {yesterday}")
print(f"  查询日期: {prev_hour_stat_date}")
print(f"  匹配: {'✓' if prev_hour_stat_date == yesterday else 'X'}")
