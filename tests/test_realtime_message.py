"""模拟实时战报消息内容"""
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# 设置 UTF-8 编码
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

load_dotenv()

from bigquery_storage import BigQueryUploader

bq = BigQueryUploader(os.getenv('BQ_PROJECT_ID'), 'quickbi_data')
data = bq.query_realtime_report_data()

# 提取关键数据
summary = data.get('summary', {})
prev_hour_summary = data.get('prev_hour_summary', {})

total_spend = summary.get('total_spend', 0)
total_revenue = summary.get('total_media_revenue', 0)
mmp_roas = summary.get('mmp_roas', 0)

prev_total_spend = prev_hour_summary.get('total_spend', 0)
prev_roas = prev_hour_summary.get('mmp_roas', 0)

# 计算趋势
hourly_spend_delta = total_spend - prev_total_spend
roas_trend = mmp_roas - prev_roas

print("=" * 60)
print("实时战报消息内容模拟")
print("=" * 60)

print(f"\n🟢 大盘健康：当前 ROAS {mmp_roas:.1%}")
print("\n⏰ 实时战报")
print(f"• 截止当前总耗：${total_spend:,.2f}")
print(f"• 截止当前收入：${total_revenue:,.2f}")
print(f"• 当前 MMP ROAS：{mmp_roas:.1%}")

if prev_total_spend > 0:
    batch_time = data.get('batch_time', '')
    prev_batch_time = data.get('prev_batch_time', '')
    hourly_spend_change_pct = (hourly_spend_delta / prev_total_spend * 100) if prev_total_spend > 0 else 0

    print(f"• 新增消耗 ({batch_time} vs {prev_batch_time})：${hourly_spend_delta:,.2f} ({hourly_spend_change_pct:+.0f}%)")

    # ROAS 趋势
    if prev_roas > 0:
        roas_emoji = "↗️ 上升" if roas_trend > 0 else "↘️ 下滑" if roas_trend < 0 else "➡️ 持平"
        print(f"• 过去1小时 ROAS 趋势：{roas_emoji} {abs(roas_trend):.1%}")
    else:
        print("• (不显示 ROAS 趋势，因为 prev_roas = 0)")
else:
    print("• (没有上小时数据)")

print("\n" + "=" * 60)
print("调试信息:")
print(f"  当前 ROAS: {mmp_roas:.4f}")
print(f"  上小时 ROAS: {prev_roas:.4f}")
print(f"  ROAS 变化: {roas_trend:.4f}")
print(f"  prev_roas > 0? {prev_roas > 0}")
