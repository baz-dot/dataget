"""测试 scheduler.py 的周报功能"""
import os
from dotenv import load_dotenv
from bigquery_storage import BigQueryUploader
from lark.lark_bot import LarkBot

# 加载环境变量
load_dotenv()

# 配置
PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
DATASET_ID = 'xmp_data'  # 使用 xmp_data 数据集（包含剪辑师数据）
WEBHOOK_URL = "https://open.larksuite.com/open-apis/bot/v2/hook/03f0693e-a0f3-424f-b3ca-a4248f886998"

print("="*80)
print("测试 scheduler.py 周报功能")
print("="*80)
print(f"Project: {PROJECT_ID}")
print(f"Dataset: {DATASET_ID}")
print(f"Webhook: {WEBHOOK_URL}")
print()

# 1. 初始化 BigQuery
print("[Step 1] 初始化 BigQuery...")
bq = BigQueryUploader(PROJECT_ID, DATASET_ID)

# 2. 查询周报数据
print("\n[Step 2] 查询周报数据...")
report_data = bq.query_weekly_report_data()

week_start = report_data.get('week_start', '')
week_end = report_data.get('week_end', '')
summary = report_data.get('summary', {})
editor_stats = report_data.get('editor_stats', [])

print(f"  周期: {week_start} ~ {week_end}")
print(f"  周总消耗: ${summary.get('week_total_spend', 0):,.2f}")
print(f"  周均 ROAS: {summary.get('week_avg_roas', 0):.1%}")
print(f"  投手数: {len(report_data.get('optimizer_weekly', []))}")
print(f"  剪辑师数: {len(editor_stats)}")

# 打印剪辑师数据预览
if editor_stats:
    print("\n  剪辑师数据预览（前5名）:")
    for i, editor in enumerate(editor_stats[:5]):
        name = editor.get('name', '')
        material_count = editor.get('total_material_count', 0)
        spend = editor.get('total_spend', 0)
        roas = editor.get('roas', 0)
        hot_count = editor.get('total_hot_count', 0)
        print(f"    {i+1}. {name}: 素材数={material_count}, 消耗=${spend:,.0f}, ROAS={roas:.1%}, 爆款={hot_count}")
else:
    print("\n  ⚠️ 没有剪辑师数据")

# 3. 初始化 LarkBot
print("\n[Step 3] 初始化 LarkBot...")
gemini_api_key = os.getenv("GEMINI_API_KEY")
chatgpt_api_key = os.getenv("OPENAI_API_KEY") or gemini_api_key

bot = LarkBot(
    webhook_url=WEBHOOK_URL,
    secret=None,  # 测试 Webhook 不需要签名
    gemini_api_key=gemini_api_key,
    chatgpt_api_key=chatgpt_api_key
)

# 4. 发送周报
print("\n[Step 4] 发送周报到飞书...")
result = bot.send_weekly_report(data=report_data)

print(f"\n发送结果: {result}")

if result.get('StatusCode') == 0 or result.get('code') == 0:
    print("\n✅ 周报发送成功！")
    print(f"请检查飞书群消息")
else:
    print(f"\n❌ 周报发送失败: {result}")

