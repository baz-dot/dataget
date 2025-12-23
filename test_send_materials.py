"""
从 BigQuery 查询真实素材数据并发送到飞书
"""
import os
from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery
from lark.lark_bot import LarkBot

# 初始化 BigQuery 客户端
project_id = os.getenv('BQ_PROJECT_ID')
dataset_id = os.getenv('BQ_DATASET_ID')
client = bigquery.Client(project=project_id)

# 查询 TOP 素材
table_ref = f"{project_id}.{dataset_id}.xmp_materials"
query = f"""
SELECT
    user_material_id,
    user_material_name,
    click_rate,
    conversion_rate,
    cost,
    channel
FROM `{table_ref}`
WHERE cost > 10
ORDER BY conversion_rate DESC, click_rate DESC
LIMIT 10
"""

print('正在查询 BigQuery...')
print(f'Project: {project_id}, Dataset: {dataset_id}')

try:
    result = client.query(query).result()
    materials = []
    for row in result:
        materials.append({
            'material_id': row.user_material_id,
            'material_name': row.user_material_name,
            'ctr': float(row.click_rate or 0),
            'cvr': float(row.conversion_rate or 0),
            'cost': float(row.cost or 0),
            'channel': row.channel
        })

    print(f'查询到 {len(materials)} 条素材数据')

    if materials:
        # 打印查询结果
        print('\n素材列表:')
        for i, m in enumerate(materials):
            print(f"  {i+1}. {m['material_name'][:40]}")
            print(f"     CTR: {m['ctr']:.2%} | CVR: {m['cvr']:.2%} | 消耗: ${m['cost']:.2f}")

        # 初始化机器人
        bot = LarkBot(
            webhook_url=os.getenv('LARK_WEBHOOK_URL'),
            secret=os.getenv('LARK_SECRET') or None
        )

        # 逐条发送每个素材的推荐信息
        import time as t
        for i, m in enumerate(materials):
            signal = {
                'signal_type': 'creative_refresh',
                'campaign_name': m['material_name'][:40],
                'optimizer': '系统自动',
                'message': f"CTR: {m['ctr']:.2%} | CVR: {m['cvr']:.2%} | 消耗: ${m['cost']:.2f}",
                'action': f"排名 TOP{i+1}，建议优先使用此素材进行投放",
                'metrics': {
                    'ctr': m['ctr'],
                    'cvr': m['cvr'],
                    'cost': m['cost'],
                    'channel': m['channel']
                }
            }

            result = bot.send_strategy_signal(signal)
            print(f'\n[{i+1}/{len(materials)}] 发送结果:', result)
            t.sleep(0.5)  # 避免发送过快
    else:
        print('未查询到素材数据')

except Exception as e:
    print(f'错误: {e}')
    import traceback
    traceback.print_exc()
