

"""
周报测试脚本
测试周报数据查询和飞书发送功能
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from bigquery_storage import BigQueryUploader
from lark.lark_bot import LarkBot
from config.data_source import get_data_source_config

# 配置
# WEBHOOK_URL = os.getenv('LARK_WEBHOOK_URL')
WEBHOOK_URL = "https://open.larksuite.com/open-apis/bot/v2/hook/03f0693e-a0f3-424f-b3ca-a4248f886998"
SECRET = None
PROJECT_ID = os.getenv('BQ_PROJECT_ID')
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
CHATGPT_API_KEY = os.getenv("OPENAI_API_KEY") or GEMINI_API_KEY

# 从配置获取数据源
DATA_SOURCE_CONFIG = get_data_source_config()

print("="*60)
print("周报测试")
print("="*60)
print(f"数据源: {DATA_SOURCE_CONFIG}")
print(f"Project: {PROJECT_ID}")
print()

# 初始化 BigQuery
bq = BigQueryUploader(PROJECT_ID, DATA_SOURCE_CONFIG["dataset_id"])

# 查询周报数据
print("查询周报数据...")
# 使用完整一周数据 (01-06 ~ 01-12)
report_data = bq.query_weekly_report_data(week_start="2026-01-06", week_end="2026-01-12")

# 打印结果
print(f"\n周期: {report_data.get('week_start')} ~ {report_data.get('week_end')}")
print()

# 大盘汇总
summary = report_data.get('summary', {})
prev_summary = report_data.get('prev_week_summary', {})
print("【大盘汇总】")
print(f"  周总消耗: ${summary.get('week_total_spend', 0):,.2f}")
print(f"  周总营收: ${summary.get('week_total_revenue', 0):,.2f}")
print(f"  周均 ROAS: {summary.get('week_avg_roas', 0):.2%}")
print(f"  日均消耗: ${summary.get('daily_avg_spend', 0):,.2f}")
print()

# 环比
if prev_summary:
    prev_spend = prev_summary.get('week_total_spend', 0)
    prev_roas = prev_summary.get('week_avg_roas', 0)
    spend_change = (summary.get('week_total_spend', 0) - prev_spend) / prev_spend if prev_spend > 0 else 0
    roas_change = summary.get('week_avg_roas', 0) - prev_roas
    print("【周环比】")
    print(f"  消耗环比: {spend_change:+.1%}")
    print(f"  ROAS 环比: {roas_change:+.1%}")
    print()

# 日趋势
daily_stats = report_data.get('daily_stats', [])
if daily_stats:
    print(f"【日趋势】({len(daily_stats)} 天)")
    for day in daily_stats:
        print(f"  {day['date']}: ${day['spend']:,.0f} | ROAS {day['roas']:.1%}")
    print()

# 投手数据
optimizer_weekly = report_data.get('optimizer_weekly', [])
print(f"【投手周度】({len(optimizer_weekly)} 人)")
for i, opt in enumerate(optimizer_weekly[:5]):
    roas_change = opt.get('roas_change', 0)
    change_str = f" ({roas_change:+.1%})" if roas_change != 0 else ""
    print(f"  {i+1}. {opt['name']}: ${opt['spend']:,.0f} | ROAS {opt['roas']:.1%}{change_str}")
print()

# 剧集数据
top_dramas = report_data.get('top_dramas', [])
potential_dramas = report_data.get('potential_dramas', [])
declining_dramas = report_data.get('declining_dramas', [])

print(f"【头部剧集】({len(top_dramas)} 部)")
for drama in top_dramas[:3]:
    countries = ", ".join(drama.get('top_countries', [])[:3])
    print(f"  《{drama['name']}》: ${drama['spend']:,.0f} | ROAS {drama['roas']:.1%} | {countries}")

print(f"\n【潜力剧集】({len(potential_dramas)} 部)")
for drama in potential_dramas[:3]:
    print(f"  《{drama['name']}》: ${drama['spend']:,.0f} | ROAS {drama['roas']:.1%}")

print(f"\n【衰退预警】({len(declining_dramas)} 部)")
for drama in declining_dramas[:3]:
    print(f"  《{drama['name']}》: ROAS {drama['roas']:.1%} ({drama.get('roas_change', 0):+.1%})")
print()

# 市场数据
top_countries = report_data.get('top_countries', [])
emerging_markets = report_data.get('emerging_markets', [])

print(f"【主力市场】({len(top_countries)} 个)")
for country in top_countries[:5]:
    change = country.get('roas_change', 0)
    change_str = f" ({change:+.1%})" if change != 0 else ""
    print(f"  {country['name']}: ${country['spend']:,.0f} | ROAS {country['roas']:.1%}{change_str}")

print(f"\n【新兴机会】({len(emerging_markets)} 个)")
for market in emerging_markets[:3]:
    print(f"  {market['name']}: ROAS {market['roas']:.1%} (${market['spend']:,.0f})")
print()

# 发送到飞书
print("="*60)
print("发送周报到飞书...")
print("="*60)

bot = LarkBot(
    webhook_url=WEBHOOK_URL,
    secret=SECRET,
    gemini_api_key=GEMINI_API_KEY,
    chatgpt_api_key=CHATGPT_API_KEY
)

result = bot.send_weekly_report(data=report_data)

if result.get('StatusCode') == 0 or result.get('code') == 0:
    print("[OK] 周报发送成功!")
else:
    print(f"[FAIL] 周报发送失败: {result}")

# 同步写入飞书文档
print()
print("="*60)
print("写入周报到飞书文档...")
print("="*60)

from lark.lark_doc_client import create_doc_client

doc_client = create_doc_client()
# 周报文档 wiki token
WIKI_TOKEN = "DEDKwkkMliSFyVku2LHlWWC5gvq"

# 获取实际 doc_token
node_info = doc_client.get_wiki_node_info(WIKI_TOKEN)
if node_info.get('code') == 0:
    doc_token = node_info['data']['node']['obj_token']
    print(f"文档 token: {doc_token}")

    doc_result = doc_client.write_standard_weekly_report(doc_token, report_data)
    if doc_result.get('code') == 0:
        print("[OK] 周报已写入文档!")
    else:
        print(f"[FAIL] 写入文档失败: {doc_result}")
else:
    print(f"[FAIL] 获取文档信息失败: {node_info}")
