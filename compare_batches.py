"""
对比 14:03 和 14:33 两个 batch 的详细数据
找出为什么剪辑师数量从 7 个降到 1 个
"""
import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def compare_editor_stats():
    """对比两个 batch 的剪辑师统计"""
    project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
    client = bigquery.Client(project=project_id)

    batches = {
        '正常': '20260116_140330',  # 14:03 - 7 个剪辑师
        '异常': '20260116_143309',  # 14:33 - 1 个剪辑师
    }

    for label, batch_id in batches.items():
        print("\n" + "=" * 80)
        print(f"{label} Batch: {batch_id}")
        print("=" * 80)

        query = f"""
        SELECT
            editor_name,
            channel,
            material_count,
            spend,
            revenue,
            roas,
            impressions,
            clicks
        FROM `{project_id}.xmp_data.xmp_editor_stats`
        WHERE stat_date = '2026-01-16' AND batch_id = '{batch_id}'
        ORDER BY channel, spend DESC
        """

        results = list(client.query(query))
        print(f"\n剪辑师数: {len(results)}")

        # 按渠道分组显示
        fb_editors = [r for r in results if r.channel == 'facebook']
        tk_editors = [r for r in results if r.channel == 'tiktok']

        print(f"\nFacebook: {len(fb_editors)} 人")
        for r in fb_editors:
            print(f"  - {r.editor_name}: ${r.spend:,.2f} (素材数: {r.material_count})")

        print(f"\nTikTok: {len(tk_editors)} 人")
        for r in tk_editors:
            print(f"  - {r.editor_name}: ${r.spend:,.2f} (素材数: {r.material_count})")

if __name__ == '__main__':
    compare_editor_stats()
