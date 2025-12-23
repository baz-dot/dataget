"""
测试所有飞书消息模板 - 使用真实数据
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from google.cloud import bigquery
from lark.lark_bot import LarkBot

# 初始化
project_id = os.getenv('BQ_PROJECT_ID')
dataset_id = os.getenv('BQ_DATASET_ID')
client = bigquery.Client(project=project_id)
bot = LarkBot(
    webhook_url=os.getenv('LARK_WEBHOOK_URL'),
    secret=os.getenv('LARK_SECRET') or None
)

print("=" * 60)
print("开始测试所有消息模板")
print("=" * 60)

# ============ 1. 每日投放战报 ============
print("\n[1/6] 发送每日投放战报...")

# 查询投手数据 (使用 designer_name 作为投手)
optimizer_query = f"""
SELECT
    designer_name,
    SUM(cost) as spend,
    AVG(click_rate) as avg_ctr,
    COUNT(DISTINCT user_material_id) as materials
FROM `{project_id}.{dataset_id}.xmp_materials`
WHERE cost > 0
GROUP BY designer_name
ORDER BY spend DESC
LIMIT 5
"""

total_spend = 0
optimizers = []

try:
    result = client.query(optimizer_query).result()
    for row in result:
        optimizers.append({
            'name': row.designer_name or '未知',
            'spend': float(row.spend or 0),
            'roas': float(row.avg_ctr or 0),  # 用 CTR 代替 ROAS 展示
            'new_campaigns': int(row.materials or 0)
        })
        total_spend += float(row.spend or 0)

    # 构建战报数据
    battle_data = {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'total_spend': total_spend,
        'spend_change': -0.05,  # 示例环比
        'd0_roas': 0.38,
        'roas_target': 0.40,
        'optimizers': optimizers,
        'warnings': [opt for opt in optimizers if opt['roas'] < 0.1][:2]
    }

    result = bot.send_daily_battle_report(battle_data, bi_link="https://bi.aliyun.com/product/vigloo.htm?menuId=f438317d-6f93-4561-8fb2-e85bf2e9aea8&accounttraceid=ee0ec5d2837043b595c3c6a6df78b4b3lglk")
    print(f"  发送结果: {result}")
except Exception as e:
    print(f"  错误: {e}")

time.sleep(1)

# ============ 2. 每日素材产出榜 ============
print("\n[2/6] 发送每日素材产出榜...")

# 查询剪辑师数据 (使用 designer_name)
editor_query = f"""
SELECT
    designer_name as editor,
    COUNT(*) as output,
    SUM(CASE WHEN cost > 100 THEN 1 ELSE 0 END) as hot_count,
    MAX(user_material_name) as hot_material,
    MAX(cost) as hot_spend,
    MAX(click_rate) as hot_ctr
FROM `{project_id}.{dataset_id}.xmp_materials`
GROUP BY designer_name
ORDER BY output DESC
LIMIT 5
"""

try:
    result = client.query(editor_query).result()
    editors = []
    total_creatives = 0
    hot_creatives = 0
    for row in result:
        editors.append({
            'name': row.editor or '未知',
            'output': int(row.output or 0),
            'ai_output': 0,
            'hot_material': row.hot_material[:30] if row.hot_material else '',
            'hot_spend': float(row.hot_spend or 0),
            'hot_roas': float(row.hot_ctr or 0)
        })
        total_creatives += int(row.output or 0)
        hot_creatives += int(row.hot_count or 0)

    creative_data = {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'total_creatives': total_creatives,
        'hot_creatives': hot_creatives,
        'editors': editors,
        'insight': '近期爆款素材特征：开头3秒强冲突、竖屏全屏、字幕醒目'
    }

    result = bot.send_daily_creative_report(creative_data, xmp_link="https://xmp.mobvista.com/ads_manage/summary/material")
    print(f"  发送结果: {result}")
except Exception as e:
    print(f"  错误: {e}")

time.sleep(1)

# ============ 3. 每周经营复盘 ============
print("\n[3/6] 发送每周经营复盘...")

weekly_data = {
    'week': 'W51',
    'period': '12.16 - 12.22',
    'total_spend': total_spend * 7 if total_spend else 50000,
    'spend_target': 60000,
    'avg_roas': 0.38,
    'roas_target': 0.40,
    'groups': [
        {'name': 'A组(老投手)', 'avg_spend': 2500, 'roas': 0.42, 'conclusion': '稳定产出'},
        {'name': 'B组(新投手)', 'avg_spend': 1200, 'roas': 0.35, 'conclusion': '需要培训'}
    ],
    'suggestions': [
        {'category': '素材方向', 'content': '加大真人出镜类素材产出'},
        {'category': '投放策略', 'content': '重点关注美国、日本市场'},
        {'category': '预算分配', 'content': 'A组预算上调20%，B组维持观察'}
    ]
}

try:
    result = bot.send_weekly_review(weekly_data)
    print(f"  发送结果: {result}")
except Exception as e:
    print(f"  错误: {e}")

time.sleep(1)

# ============ 4. 紧急止损预警 ============
print("\n[4/6] 发送紧急止损预警...")

# 查询低效计划
stop_loss_query = f"""
SELECT
    user_material_id as campaign_id,
    user_material_name as campaign_name,
    cost as spend,
    click_rate as ctr,
    designer_name
FROM `{project_id}.{dataset_id}.xmp_materials`
WHERE cost > 30
ORDER BY click_rate ASC
LIMIT 1
"""

try:
    result = client.query(stop_loss_query).result()
    for row in result:
        stop_loss_data = {
            'drama_name': row.campaign_name[:20] if row.campaign_name else '未知剧集',
            'campaign_id': row.campaign_id or 'unknown',
            'spend': float(row.spend or 0),
            'd0_roas': 0.05,  # 模拟低ROAS
            'cpi': 3.5,
            'cpi_baseline': 2.0,
            'judgment': '消耗已过测试线，且无明显回收，属于赔钱计划。',
            'action': '立即关停',
            'optimizer': row.designer_name or '未知'
        }

        result = bot.send_stop_loss_alert(stop_loss_data,
            media_link="https://business.facebook.com/adsmanager",
            bi_link="https://bi.aliyun.com/product/vigloo.htm?menuId=f438317d-6f93-4561-8fb2-e85bf2e9aea8&accounttraceid=ee0ec5d2837043b595c3c6a6df78b4b3lglk")
        print(f"  发送结果: {result}")
        break
except Exception as e:
    print(f"  错误: {e}")

time.sleep(1)

# ============ 5. 扩量与机会建议 ============
print("\n[5/6] 发送扩量与机会建议...")

# 查询高效计划
scale_up_query = f"""
SELECT
    user_material_id as campaign_id,
    user_material_name as campaign_name,
    cost as spend,
    click_rate as ctr,
    designer_name
FROM `{project_id}.{dataset_id}.xmp_materials`
WHERE cost > 20
ORDER BY click_rate DESC
LIMIT 1
"""

try:
    result = client.query(scale_up_query).result()
    for row in result:
        scale_up_data = {
            'drama_name': row.campaign_name[:20] if row.campaign_name else '未知剧集',
            'campaign_id': row.campaign_id or 'unknown',
            'spend': float(row.spend or 0),
            'd0_roas': 0.65,  # 模拟高ROAS
            'ctr': float(row.ctr or 0),
            'competitor_insight': '竞品同类素材近7天消耗增长40%，市场热度上升',
            'suggestions': [
                '预算上调30%，抢占流量高峰',
                '复制计划到日本、韩国市场',
                '基于此素材制作3个变体版本'
            ],
            'optimizer': row.designer_name or '未知'
        }

        result = bot.send_scale_up_suggestion(scale_up_data,
            media_link="https://business.facebook.com/adsmanager")
        print(f"  发送结果: {result}")
        break
except Exception as e:
    print(f"  错误: {e}")

time.sleep(1)

# ============ 6. 策略信号汇总 ============
print("\n[6/6] 发送策略信号汇总...")

# 从数据库查询真实数据构建信号
signals = []

# 查询低效素材 (止损信号)
stop_loss_query = f"""
SELECT user_material_name, designer_name, cost, click_rate
FROM `{project_id}.{dataset_id}.xmp_materials`
WHERE cost > 30 AND click_rate < 0.05
ORDER BY cost DESC
LIMIT 2
"""

# 查询高效素材 (扩量信号)
scale_up_query = f"""
SELECT user_material_name, designer_name, cost, click_rate
FROM `{project_id}.{dataset_id}.xmp_materials`
WHERE cost > 20 AND click_rate > 0.15
ORDER BY click_rate DESC
LIMIT 2
"""

# 查询需要优化的素材 (素材优化信号)
creative_query = f"""
SELECT user_material_name, designer_name, cost, click_rate
FROM `{project_id}.{dataset_id}.xmp_materials`
WHERE cost > 10 AND click_rate BETWEEN 0.05 AND 0.10
ORDER BY cost DESC
LIMIT 2
"""

try:
    # 止损信号
    for row in client.query(stop_loss_query).result():
        signals.append({
            'signal_type': 'stop_loss',
            'campaign_name': row.user_material_name[:30] if row.user_material_name else '未知',
            'optimizer': row.designer_name or '未知',
            'message': f'消耗 ${row.cost:.2f}，CTR {row.click_rate:.2%}',
            'action': '立即关停'
        })

    # 扩量信号
    for row in client.query(scale_up_query).result():
        signals.append({
            'signal_type': 'scale_up',
            'campaign_name': row.user_material_name[:30] if row.user_material_name else '未知',
            'optimizer': row.designer_name or '未知',
            'message': f'CTR {row.click_rate:.2%}，消耗 ${row.cost:.2f}',
            'action': '预算上调20%'
        })

    # 素材优化信号
    for row in client.query(creative_query).result():
        signals.append({
            'signal_type': 'creative_refresh',
            'campaign_name': row.user_material_name[:30] if row.user_material_name else '未知',
            'optimizer': row.designer_name or '未知',
            'message': f'CTR {row.click_rate:.2%}，有优化空间',
            'action': '更换TOP3素材'
        })

    print(f"  查询到 {len(signals)} 个信号")

    if signals:
        results = bot.send_strategy_batch(signals, group_by_optimizer=True)
        print(f"  发送结果: {results}")
    else:
        print("  无符合条件的信号")
except Exception as e:
    print(f"  错误: {e}")

print("\n" + "=" * 60)
print("所有消息模板测试完成！")
print("=" * 60)
