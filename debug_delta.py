"""调试投手消耗差异"""
from bigquery_storage import BigQueryUploader
import os
import json
from dotenv import load_dotenv
load_dotenv()

bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), os.getenv('BQ_DATASET_ID'))

# 1. 获取当前实时数据
print("=== 当前投手消耗 ===")
realtime_data = bq.query_realtime_report_data()
current_optimizers = {opt['optimizer']: opt['spend'] for opt in realtime_data.get('optimizer_spend', [])}
for name, spend in current_optimizers.items():
    print(f"  {name}: ${spend:,.2f}")

# 2. 获取15点快照的投手数据
print("\n=== 15点快照投手消耗 ===")
query = f"""
SELECT optimizer_data
FROM `{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET_ID')}.hourly_snapshots`
WHERE hour = 15
ORDER BY snapshot_time DESC
LIMIT 1
"""
prev_optimizers = {}
for row in bq.client.query(query).result():
    optimizer_data = json.loads(row.optimizer_data) if row.optimizer_data else []
    for opt in optimizer_data:
        prev_optimizers[opt.get('optimizer')] = opt.get('spend', 0)
        print(f"  {opt.get('optimizer')}: ${opt.get('spend', 0):,.2f}")

# 3. 计算差异
print("\n=== 消耗差异 (当前 - 15点) ===")
all_names = set(current_optimizers.keys()) | set(prev_optimizers.keys())
for name in all_names:
    current = current_optimizers.get(name, 0)
    prev = prev_optimizers.get(name, 0)
    delta = current - prev
    flag = "⚠️ 负数!" if delta < 0 else ""
    print(f"  {name}: ${current:,.2f} - ${prev:,.2f} = ${delta:,.2f} {flag}")
