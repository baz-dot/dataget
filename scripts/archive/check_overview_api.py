"""调用 overview API 查看原始返回数据"""
import os
import sys
import json
from datetime import datetime, timedelta, timezone

BEIJING_TZ = timezone(timedelta(hours=8))

from alibabacloud_quickbi_public20220101.client import Client
from alibabacloud_quickbi_public20220101 import models
from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util.models import RuntimeOptions
from dotenv import load_dotenv
load_dotenv()

ACCESS_KEY_ID = os.getenv('ALIYUN_ACCESS_KEY_ID')
ACCESS_KEY_SECRET = os.getenv('ALIYUN_ACCESS_KEY_SECRET')
OVERVIEW_API_ID = os.getenv('QUICKBI_OVERVIEW_API_ID', '7a15b44f69fd')

config = Config(
    access_key_id=ACCESS_KEY_ID,
    access_key_secret=ACCESS_KEY_SECRET,
    endpoint='quickbi-public.cn-hangzhou.aliyuncs.com'
)
client = Client(config)

stat_date = '20260226'
conditions = json.dumps({"stat_date": stat_date})
request = models.QueryDataServiceRequest(
    api_id=OVERVIEW_API_ID,
    conditions=conditions
)
runtime = RuntimeOptions(read_timeout=60000, connect_timeout=30000)

print(f"查询 overview API (api_id={OVERVIEW_API_ID}, stat_date={stat_date})")
print("=" * 60)

response = client.query_data_service_with_options(request, runtime)

result = response.body.result
if result:
    # 打印 result 对象的所有属性
    print(f"result 属性: {[a for a in dir(result) if not a.startswith('_')]}")

    if result.values:
        print(f"\n返回 {len(result.values)} 行数据:")
        for i, row in enumerate(result.values):
            print(f"\n--- 行 {i} ---")
            for key, value in sorted(row.items()):
                print(f"  {key}: {value}")

    if hasattr(result, 'headers') and result.headers:
        print(f"\n表头: {result.headers}")
else:
    print("无数据返回")

# 打印原始 JSON
print(f"\n原始 body to_map: {response.body.to_map()}")
