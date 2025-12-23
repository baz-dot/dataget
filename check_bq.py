"""检查 BigQuery 数据"""
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

project_id = os.getenv('BQ_PROJECT_ID')
client = bigquery.Client(project=project_id)

# 检查两个数据集的数据
datasets = ['quickbi_data', 'xmp_data']

for dataset in datasets:
    print(f"\n{'='*60}")
    print(f"数据集: {dataset}")
    print(f"{'='*60}")

    query = f"""
    SELECT
        stat_date,
        COUNT(*) as record_count,
        SUM(spend) as total_spend,
        SUM(new_user_revenue) as total_revenue,
        SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
    FROM `{project_id}.{dataset}.quickbi_campaigns`
    GROUP BY stat_date
    ORDER BY stat_date DESC
    LIMIT 10
    """

    try:
        result = client.query(query).result()
        print(f"{'日期':<12} {'记录数':<10} {'总消耗':<15} {'总收入':<15} {'ROAS':<10}")
        print("-" * 70)
        for row in result:
            roas_str = f"{row.roas:.2%}" if row.roas else "N/A"
            print(f"{str(row.stat_date):<12} {row.record_count:<10} ${row.total_spend:,.2f}{'':>3} ${row.total_revenue:,.2f}{'':>3} {roas_str}")
    except Exception as e:
        print(f"查询失败: {e}")
