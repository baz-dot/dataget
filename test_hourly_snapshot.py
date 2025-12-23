"""
测试小时快照环比功能
可以手动插入模拟数据来测试
"""
from bigquery_storage import BigQueryUploader
from lark.lark_bot import LarkBot
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

# 快照表放在 quickbi_data 数据集，和实时数据查询保持一致
bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), "quickbi_data")
lark_bot = LarkBot(os.getenv('LARK_WEBHOOK_URL'), os.getenv('LARK_SECRET'))

def insert_mock_snapshot(hours_ago: int, spend: float, roas: float, optimizer_data: list = None):
    """
    插入模拟的历史快照数据

    Args:
        hours_ago: 几小时前
        spend: 模拟消耗
        roas: 模拟ROAS
        optimizer_data: 投手数据列表
    """
    mock_time = datetime.now() - timedelta(hours=hours_ago)

    table_ref = f"{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET_ID')}.hourly_snapshots"

    rows = [{
        "snapshot_time": mock_time.isoformat(),
        "stat_date": mock_time.strftime('%Y-%m-%d'),
        "hour": mock_time.hour,
        "total_spend": spend,
        "total_revenue": spend * roas,
        "d0_roas": roas,
        "optimizer_data": json.dumps(optimizer_data or []),
        "batch_id": f"mock_{mock_time.strftime('%Y%m%d_%H%M%S')}"
    }]

    errors = bq.client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"插入失败: {errors}")
    else:
        print(f"已插入模拟快照: {mock_time} | spend=${spend:,.2f} | roas={roas:.1%}")

def test_with_mock_data():
    """使用模拟数据测试环比"""

    # 1. 插入1小时前的模拟数据 (比当前少一些)
    current_data = bq.query_realtime_report_data()
    current_spend = current_data['summary'].get('total_spend', 0)
    current_roas = current_data['summary'].get('d0_roas', 0)

    # 模拟1小时前的数据 (消耗少2000，ROAS低2%)
    mock_spend = current_spend - 2000
    mock_roas = current_roas - 0.02

    # 模拟投手数据
    mock_optimizer_data = []
    for opt in current_data.get('optimizer_spend', []):
        mock_optimizer_data.append({
            "optimizer": opt['optimizer'],
            "spend": opt['spend'] * 0.9  # 模拟1小时前消耗少10%
        })

    print("=== 插入模拟的1小时前快照 ===")
    insert_mock_snapshot(
        hours_ago=1,
        spend=mock_spend,
        roas=mock_roas,
        optimizer_data=mock_optimizer_data
    )

    # 等待 BigQuery streaming buffer
    print("\n等待3秒让数据写入...")
    import time
    time.sleep(3)

    # 2. 测试获取上一小时快照
    print("\n=== 测试获取最近快照 ===")
    prev_snapshot = bq.get_previous_hour_snapshot()
    if prev_snapshot:
        print(f"获取到快照: {prev_snapshot['snapshot_time']}")
        print(f"  消耗: ${prev_snapshot['total_spend']:,.2f}")
        print(f"  ROAS: {prev_snapshot['d0_roas']:.1%}")
    else:
        print("未获取到快照")

    # 3. 计算环比
    print("\n=== 环比计算 ===")
    if prev_snapshot:
        spend_delta = current_spend - prev_snapshot['total_spend']
        roas_delta = current_roas - prev_snapshot['d0_roas']
        print(f"消耗变化: +${spend_delta:,.2f}")
        print(f"ROAS变化: {roas_delta:+.1%}")

def show_all_snapshots():
    """显示所有快照记录"""
    print("=== 所有快照记录 ===")
    query = f"""
    SELECT snapshot_time, hour, total_spend, d0_roas, batch_id
    FROM `{os.getenv('BQ_PROJECT_ID')}.quickbi_data.hourly_snapshots`
    ORDER BY snapshot_time DESC
    LIMIT 10
    """
    for row in bq.client.query(query).result():
        print(f"  {row.snapshot_time} | hour={row.hour} | spend=${row.total_spend:,.2f} | roas={row.d0_roas:.1%} | {row.batch_id}")

def test_realtime_with_prev():
    """测试带环比的实时播报"""
    print("\n=== 测试带环比的实时播报 ===")

    realtime_data = bq.query_realtime_report_data()
    prev_snapshot = bq.get_previous_hour_snapshot()

    print(f"当前消耗: ${realtime_data['summary'].get('total_spend', 0):,.2f}")
    if prev_snapshot:
        print(f"上次消耗: ${prev_snapshot['total_spend']:,.2f}")
        print(f"差值: ${realtime_data['summary']['total_spend'] - prev_snapshot['total_spend']:,.2f}")

    # 发送带环比的播报
    result = lark_bot.send_realtime_report(realtime_data, prev_snapshot)
    print(f"\n发送结果: {result}")

    # 播报成功后保存快照
    if result.get("code") == 0 or result.get("StatusCode") == 0:
        print("\n=== 保存当前快照 ===")
        bq.save_hourly_snapshot(realtime_data)
    else:
        print("\n播报失败，不保存快照")

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "mock":
            test_with_mock_data()
        elif cmd == "show":
            show_all_snapshots()
        elif cmd == "test":
            test_realtime_with_prev()
        else:
            print("用法: python test_hourly_snapshot.py [mock|show|test]")
    else:
        print("用法:")
        print("  python test_hourly_snapshot.py mock  - 插入模拟数据并测试")
        print("  python test_hourly_snapshot.py show  - 显示所有快照")
        print("  python test_hourly_snapshot.py test  - 测试带环比的实时播报")
