"""测试与15点快照对比"""
from bigquery_storage import BigQueryUploader
from lark.lark_bot import LarkBot
import os
from dotenv import load_dotenv
load_dotenv()

bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), os.getenv('BQ_DATASET_ID'))
lark_bot = LarkBot(os.getenv('LARK_WEBHOOK_URL'), os.getenv('LARK_SECRET'))

# 1. 获取当前实时数据
print("=== 当前实时数据 ===")
realtime_data = bq.query_realtime_report_data()
print(f"当前消耗: ${realtime_data['summary'].get('total_spend', 0):,.2f}")
print(f"当前 D0 ROAS: {realtime_data['summary'].get('d0_roas', 0):.1%}")

# 2. 手动获取15点的快照
print("\n=== 15点快照数据 ===")
query = f"""
SELECT *
FROM `{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET_ID')}.hourly_snapshots`
WHERE hour = 15
ORDER BY snapshot_time DESC
LIMIT 1
"""
import json
prev_snapshot = None
for row in bq.client.query(query).result():
    prev_snapshot = {
        "snapshot_time": str(row.snapshot_time),
        "stat_date": str(row.stat_date),
        "hour": row.hour,
        "total_spend": float(row.total_spend or 0),
        "total_revenue": float(row.total_revenue or 0),
        "d0_roas": float(row.d0_roas or 0),
        "optimizer_data": json.loads(row.optimizer_data) if row.optimizer_data else [],
        "batch_id": row.batch_id
    }
    print(f"15点消耗: ${prev_snapshot['total_spend']:,.2f}")
    print(f"15点 D0 ROAS: {prev_snapshot['d0_roas']:.1%}")

# 3. 计算差异
if prev_snapshot:
    spend_diff = realtime_data['summary']['total_spend'] - prev_snapshot['total_spend']
    roas_diff = realtime_data['summary']['d0_roas'] - prev_snapshot['d0_roas']
    print(f"\n=== 环比差异 ===")
    print(f"消耗变化: +${spend_diff:,.2f}")
    print(f"ROAS变化: {roas_diff:+.1%}")

# 4. 发送实时播报（带15点对比）
print("\n=== 发送实时播报 ===")
result = lark_bot.send_realtime_report(realtime_data, prev_snapshot)
print(f"发送结果: {result}")
