"""
诊断剪辑师数据采集异常
检查 2026-01-16 不同 batch 的数据差异
"""
import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def check_editor_data_by_batch():
    """检查不同 batch 的剪辑师数据"""
    project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
    client = bigquery.Client(project=project_id)

    # 查询 2026-01-16 所有 batch 的剪辑师统计
    query = f"""
    SELECT
        batch_id,
        COUNT(DISTINCT editor_name) as editor_count,
        STRING_AGG(DISTINCT editor_name, ', ' ORDER BY editor_name) as editors,
        SUM(spend) as total_spend,
        SUM(revenue) as total_revenue
    FROM `{project_id}.xmp_data.xmp_editor_stats`
    WHERE stat_date = '2026-01-16'
    GROUP BY batch_id
    ORDER BY batch_id
    """

    print("=" * 80)
    print("2026-01-16 各 batch 剪辑师数据统计")
    print("=" * 80)

    results = list(client.query(query))

    for row in results:
        batch_time = row.batch_id.split('_')[1] if '_' in row.batch_id else 'unknown'
        hour = batch_time[:2] if len(batch_time) >= 2 else '??'
        minute = batch_time[2:4] if len(batch_time) >= 4 else '??'

        print(f"\nBatch: {row.batch_id} ({hour}:{minute})")
        print(f"   剪辑师数: {row.editor_count}")
        print(f"   消耗: ${row.total_spend:,.2f}")
        print(f"   收入: ${row.total_revenue:,.2f}")
        print(f"   剪辑师: {row.editors}")

        if row.editor_count == 1:
            print("   [WARNING] 异常：只有 1 个剪辑师！")

def check_editor_details_by_batch(batch_id: str):
    """检查特定 batch 的剪辑师详细数据"""
    project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
    client = bigquery.Client(project=project_id)

    query = f"""
    SELECT
        editor_name,
        channel,
        spend as total_cost,
        revenue as total_revenue,
        roas
    FROM `{project_id}.xmp_data.xmp_editor_stats`
    WHERE stat_date = '2026-01-16' AND batch_id = '{batch_id}'
    ORDER BY spend DESC
    """

    print(f"\n{'=' * 80}")
    print(f"Batch {batch_id} 剪辑师详细数据")
    print("=" * 80)

    results = list(client.query(query))

    for row in results:
        print(f"\n剪辑师: {row.editor_name}")
        print(f"  渠道: {row.channel}")
        print(f"  消耗: ${row.total_cost:,.2f}")
        print(f"  收入: ${row.total_revenue:,.2f}")
        print(f"  ROAS: {row.roas:.2%}")

if __name__ == '__main__':
    # 1. 检查所有 batch 的剪辑师数量
    check_editor_data_by_batch()

    # 2. 检查异常 batch 的详细数据
    print("\n\n" + "=" * 80)
    print("检查异常 batch 的详细数据")
    print("=" * 80)

    # 检查早期正常的 batch
    check_editor_details_by_batch('20260116_080000')

    # 检查异常的 batch
    check_editor_details_by_batch('20260116_233231')
