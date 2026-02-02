"""
测试剪辑师昨天的数据日报
"""
import sys
import os
from datetime import datetime, timedelta
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

# 计算昨天的日期
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

print("=" * 80)
print("测试：剪辑师昨天的数据日报")
print("=" * 80)
print()
print(f"日期: {yesterday}")
print()

# 从 BigQuery 读取数据并生成日报
print("从 BigQuery 读取数据...")
print()

import asyncio
from xmp.xmp_scheduler import query_stats_from_bq, export_stats_to_lark_doc

# 查询数据
bq_data = query_stats_from_bq(yesterday)
opt_stats = bq_data.get('optimizer_stats', [])
editor_stats = bq_data.get('editor_stats', [])
batch_id = bq_data.get('batch_id')
batch_valid = bq_data.get('batch_valid', True)

print(f"投手数据: {len(opt_stats)} 条")
print(f"剪辑师数据: {len(editor_stats)} 条")
print(f"Batch ID: {batch_id}")
print(f"数据有效: {batch_valid}")
print()

if not editor_stats:
    print(f"❌ 未找到 {yesterday} 的剪辑师数据")
    sys.exit(1)

# 显示前几条剪辑师数据
print("剪辑师数据预览（前3条）：")
print("-" * 80)
for i, stat in enumerate(editor_stats[:3]):
    print(f"{i+1}. {stat.get('name')}: "
          f"Meta Spend=${stat.get('meta_spend', 0):,.0f}, "
          f"TT Spend=${stat.get('tt_spend', 0):,.0f}, "
          f"Total=${stat.get('total_spend', 0):,.0f}")
print()

# 导出到飞书文档
doc_token = os.getenv("XMP_DOC_TOKEN")
if not doc_token:
    print("❌ 未配置 XMP_DOC_TOKEN 环境变量")
    sys.exit(1)

print(f"导出到飞书文档: {doc_token}")
print()

success = export_stats_to_lark_doc(opt_stats, editor_stats, yesterday, doc_token)

if success:
    print("✅ 日报导出成功！")
    print()
    print("请检查飞书文档，验证表格内容是否正确")
else:
    print("❌ 日报导出失败")

