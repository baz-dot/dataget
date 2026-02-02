"""
导入 2025-12-24 14:00 的数据（包含 media_user_revenue）
"""
import os
import sys
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from alibabacloud_quickbi_public20220101.client import Client
from alibabacloud_quickbi_public20220101 import models
from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util.models import RuntimeOptions
from bigquery_storage import BigQueryUploader

# QuickBI 配置
ACCESS_KEY_ID = os.getenv('ALIYUN_ACCESS_KEY_ID')
ACCESS_KEY_SECRET = os.getenv('ALIYUN_ACCESS_KEY_SECRET')
API_ID = os.getenv('QUICKBI_API_ID', 'ddee1f146b3a')

print("=" * 60)
print("导入 2025-12-24 14:00 的数据")
print("=" * 60)

# 获取 12-24 的数据
stat_date = "20251224"
print(f"\n[1] 从 QuickBI 获取数据...")

config = Config(
    access_key_id=ACCESS_KEY_ID,
    access_key_secret=ACCESS_KEY_SECRET,
    endpoint='quickbi-public.cn-hangzhou.aliyuncs.com'
)
client = Client(config)

conditions = json.dumps({"stat_date": stat_date})
request = models.QueryDataServiceRequest(api_id=API_ID, conditions=conditions)
runtime = RuntimeOptions(read_timeout=180000, connect_timeout=30000)

response = client.query_data_service_with_options(request, runtime)
data = response.body.result.values if response.body.result else []

print(f"[OK] 获取到 {len(data)} 条记录")

# 检查字段
sample = data[0] if data else {}
print(f"\n[2] 检查字段...")
print(f"  包含 media_user_revenue: {'YES' if 'media_user_revenue' in sample else 'NO'}")

# 上传到 BigQuery，使用 14:00 的 batch_id
batch_id = "20251224_140959"  # 使用 0959 确保被优先选中
print(f"\n[3] 上传到 BigQuery...")
print(f"  batch_id: {batch_id}")

bq_project = os.getenv('BQ_PROJECT_ID')
bq_dataset = os.getenv('QUICKBI_BQ_DATASET_ID', 'quickbi_data')

uploader = BigQueryUploader(bq_project, bq_dataset)
count = uploader.upload_quickbi_campaigns(data, batch_id=batch_id)

print(f"\n[OK] 成功上传 {count} 条记录")
print(f"  batch_id: {batch_id}")
print("\n" + "=" * 60)
