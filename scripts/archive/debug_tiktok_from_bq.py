"""
从 BigQuery 查询 TikTok campaign 数据
分析广告命名格式，找出为什么无法识别剪辑师
"""
import os
from google.cloud import bigquery
from dotenv import load_dotenv
from xmp.xmp_scheduler import extract_editor_from_ad_name

load_dotenv()

def check_tiktok_campaigns():
    """检查 TikTok campaign 数据"""
    project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
    client = bigquery.Client(project=project_id)

    # 查询 2026-01-16 的 TikTok campaign 数据
    # 分别查询早期 batch (14:03) 和晚期 batch (23:32)
    batches = ['20260116_140330', '20260116_233231']

    for batch_id in batches:
        print("\n" + "=" * 80)
        print(f"Batch: {batch_id}")
        print("=" * 80)

        query = f"""
        SELECT
            campaign_name,
            ad_name,
            channel,
            spend,
            revenue
        FROM `{project_id}.xmp_data.xmp_internal_campaigns`
        WHERE stat_date = '2026-01-16'
          AND batch_id = '{batch_id}'
          AND channel = 'tiktok'
          AND spend > 0
        ORDER BY spend DESC
        LIMIT 50
        """

        results = list(client.query(query))
        print(f"\n查询到 {len(results)} 条 TikTok 记录")

        if not results:
            print("没有数据！")
            continue

        # 统计能识别和不能识别的
        recognized = []
        unrecognized = []

        for row in results:
            ad_name = row.ad_name or ''
            editor = extract_editor_from_ad_name(ad_name)

            if editor:
                recognized.append({
                    'ad_name': ad_name,
                    'editor': editor,
                    'spend': float(row.spend)
                })
            else:
                unrecognized.append({
                    'ad_name': ad_name,
                    'spend': float(row.spend)
                })

        print(f"\n可识别: {len(recognized)} 条 (${sum(a['spend'] for a in recognized):,.2f})")
        print(f"无法识别: {len(unrecognized)} 条 (${sum(a['spend'] for a in unrecognized):,.2f})")

        # 显示可识别的广告示例
        if recognized:
            print("\n可识别的广告示例（前 5 条）:")
            for i, ad in enumerate(recognized[:5]):
                print(f"  {i+1}. [{ad['editor']}] {ad['ad_name']} (${ad['spend']:.2f})")

        # 显示无法识别的广告（重点！）
        if unrecognized:
            print("\n无法识别的广告（前 20 条）:")
            for i, ad in enumerate(unrecognized[:20]):
                print(f"  {i+1}. {ad['ad_name']} (${ad['spend']:.2f})")

if __name__ == '__main__':
    check_tiktok_campaigns()
