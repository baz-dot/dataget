"""
补充历史数据中缺失的 media_user_revenue 字段
从 QuickBI API 获取数据，然后更新 BigQuery 中对应的记录
"""
import os
import sys
import json
from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from alibabacloud_quickbi_public20220101.client import Client
from alibabacloud_quickbi_public20220101 import models
from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util.models import RuntimeOptions
from google.cloud import bigquery

# QuickBI 配置
ACCESS_KEY_ID = os.getenv('ALIYUN_ACCESS_KEY_ID')
ACCESS_KEY_SECRET = os.getenv('ALIYUN_ACCESS_KEY_SECRET')
API_ID = os.getenv('QUICKBI_API_ID', 'ddee1f146b3a')

# BigQuery 配置
BQ_PROJECT = os.getenv('BQ_PROJECT_ID')
BQ_DATASET = os.getenv('QUICKBI_BQ_DATASET_ID', 'quickbi_data')
BQ_TABLE = 'quickbi_campaigns'


def fetch_quickbi_data(stat_date: str) -> List[Dict]:
    """
    从 QuickBI API 获取指定日期的数据

    Args:
        stat_date: 日期，格式 YYYYMMDD (如 20251224)

    Returns:
        数据列表
    """
    print(f"[1] 从 QuickBI 获取数据 (stat_date={stat_date})...")

    config = Config(
        access_key_id=ACCESS_KEY_ID,
        access_key_secret=ACCESS_KEY_SECRET,
        endpoint='quickbi-public.cn-hangzhou.aliyuncs.com'
    )
    client = Client(config)

    conditions = json.dumps({"stat_date": stat_date})
    request = models.QueryDataServiceRequest(api_id=API_ID, conditions=conditions)
    runtime = RuntimeOptions(read_timeout=180000, connect_timeout=30000)

    response = client.query_data_service_with_options(request, runtime)
    data = response.body.result.values if response.body.result else []

    print(f"[OK] 获取到 {len(data)} 条记录")

    # 检查是否包含 media_user_revenue 字段
    if data:
        sample = data[0]
        has_field = 'media_user_revenue' in sample
        print(f"  包含 media_user_revenue: {'YES' if has_field else 'NO'}")
        if not has_field:
            print("  [警告] API 返回的数据中没有 media_user_revenue 字段！")

    return data


def update_bigquery_media_revenue(data: List[Dict], stat_date: str, batch_id: str = None) -> int:
    """
    更新 BigQuery 中的 media_user_revenue 字段

    Args:
        data: QuickBI 返回的数据
        stat_date: 日期，格式 YYYYMMDD
        batch_id: 可选的 batch_id，用于精确匹配特定批次

    Returns:
        更新的行数
    """
    print(f"\n[2] 更新 BigQuery 中的 media_user_revenue 字段...")

    # 转换日期格式 20251224 -> 2025-12-24
    formatted_date = f"{stat_date[:4]}-{stat_date[4:6]}-{stat_date[6:8]}"

    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    updated_count = 0
    failed_count = 0

    for record in data:
        # 提取关键字段用于匹配
        campaign_id = record.get('campaign_id')
        country = record.get('country')
        channel = record.get('channel')
        media_user_revenue = record.get('media_user_revenue')

        if not campaign_id:
            continue

        # 构建 UPDATE 语句
        if batch_id:
            # 如果指定了 batch_id，精确匹配
            update_query = f"""
            UPDATE `{table_ref}`
            SET media_user_revenue = @media_user_revenue
            WHERE stat_date = @stat_date
              AND campaign_id = @campaign_id
              AND country = @country
              AND channel = @channel
              AND batch_id = @batch_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("media_user_revenue", "FLOAT64", media_user_revenue),
                    bigquery.ScalarQueryParameter("stat_date", "DATE", formatted_date),
                    bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id),
                    bigquery.ScalarQueryParameter("country", "STRING", country),
                    bigquery.ScalarQueryParameter("channel", "STRING", channel),
                    bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
                ]
            )
        else:
            # 没有指定 batch_id，更新所有匹配的记录
            update_query = f"""
            UPDATE `{table_ref}`
            SET media_user_revenue = @media_user_revenue
            WHERE stat_date = @stat_date
              AND campaign_id = @campaign_id
              AND country = @country
              AND channel = @channel
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("media_user_revenue", "FLOAT64", media_user_revenue),
                    bigquery.ScalarQueryParameter("stat_date", "DATE", formatted_date),
                    bigquery.ScalarQueryParameter("campaign_id", "STRING", campaign_id),
                    bigquery.ScalarQueryParameter("country", "STRING", country),
                    bigquery.ScalarQueryParameter("channel", "STRING", channel),
                ]
            )

        try:
            query_job = client.query(update_query, job_config=job_config)
            query_job.result()  # 等待完成
            updated_count += query_job.num_dml_affected_rows
        except Exception as e:
            failed_count += 1
            if failed_count <= 3:  # 只打印前3个错误
                print(f"  [错误] 更新失败 (campaign_id={campaign_id}): {e}")

    print(f"[OK] 更新完成")
    print(f"  成功更新: {updated_count} 行")
    if failed_count > 0:
        print(f"  失败: {failed_count} 条记录")

    return updated_count


def main():
    """
    主函数：补充历史数据的 media_user_revenue 字段
    """
    import argparse

    parser = argparse.ArgumentParser(description='补充历史数据的 media_user_revenue 字段')
    parser.add_argument('--date', required=True, help='日期，格式 YYYYMMDD (如 20251224)')
    parser.add_argument('--batch-id', help='可选：指定 batch_id，只更新特定批次的数据 (如 20251224_140959)')
    args = parser.parse_args()

    stat_date = args.date
    batch_id = args.batch_id

    print("=" * 60)
    print(f"补充历史数据的 media_user_revenue 字段")
    print(f"日期: {stat_date}")
    if batch_id:
        print(f"批次: {batch_id}")
    print("=" * 60)

    # 1. 从 QuickBI 获取数据
    data = fetch_quickbi_data(stat_date)

    if not data:
        print("\n[错误] 没有获取到数据，退出")
        return

    # 2. 更新 BigQuery
    updated_count = update_bigquery_media_revenue(data, stat_date, batch_id)

    print("\n" + "=" * 60)
    print(f"[完成] 共更新 {updated_count} 行数据")
    print("=" * 60)


if __name__ == '__main__':
    main()
