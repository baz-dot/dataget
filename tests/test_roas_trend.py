"""调试 ROAS 趋势计算"""
import os
from datetime import datetime
from dotenv import load_dotenv
from bigquery_storage import BigQueryUploader

load_dotenv()

bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), 'quickbi_data')
data = bq.query_realtime_report_data()

print("=" * 60)
print("调试 ROAS 趋势计算")
print("=" * 60)

# 当前数据
summary = data.get('summary', {})
media_roas = summary.get('media_roas', 0)
total_spend = summary.get('total_spend', 0)

print(f"\n当前数据 (11:00):")
print(f"  batch_id: {data.get('batch_id')}")
print(f"  total_spend: ${total_spend:,.2f}")
print(f"  media_roas: {media_roas:.4f} ({media_roas:.2%})")

# 上一小时数据
prev_hour_summary = data.get('prev_hour_summary', {})
prev_total_spend = prev_hour_summary.get('total_spend', 0)
prev_roas = prev_hour_summary.get('media_roas', 0)

print(f"\n上一小时数据 (10:00):")
print(f"  prev_batch_id: {data.get('prev_batch_id')}")
print(f"  prev_total_spend: ${prev_total_spend:,.2f}")
print(f"  prev_roas: {prev_roas:.4f} ({prev_roas:.2%})")

# 计算趋势
if prev_hour_summary:
    hourly_spend_delta = total_spend - prev_total_spend
    roas_trend = media_roas - prev_roas

    print(f"\n计算结果:")
    print(f"  hourly_spend_delta: ${hourly_spend_delta:,.2f}")
    print(f"  roas_trend: {roas_trend:.4f} ({roas_trend:.2%})")

    # 模拟显示逻辑
    print(f"\n显示逻辑:")
    print(f"  prev_roas > 0? {prev_roas > 0}")

    if prev_roas > 0:
        roas_emoji = "↗️ 上升" if roas_trend > 0 else "↘️ 下滑" if roas_trend < 0 else "➡️ 持平"
        display_text = f"• 过去1小时 ROAS 趋势：{roas_emoji} {abs(roas_trend):.1%}"
        print(f"  显示文本: {display_text}")
    else:
        print(f"  不显示 ROAS 趋势（prev_roas = 0）")
else:
    print(f"\n❌ 没有上一小时数据")
