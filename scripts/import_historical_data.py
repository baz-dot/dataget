"""
导入历史数据脚本 - 确保包含 media_user_revenue 字段
用于导入 12-24 和 12-23 的数据
"""
import os
import sys
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# 北京时区 (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 先加载环境变量
load_dotenv()

# 导入 QuickBI 相关模块
from alibabacloud_quickbi_public20220101.client import Client
from alibabacloud_quickbi_public20220101 import models
from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util.models import RuntimeOptions
from bigquery_storage import BigQueryUploader

# QuickBI 配置
ACCESS_KEY_ID = os.getenv('ALIYUN_ACCESS_KEY_ID')
ACCESS_KEY_SECRET = os.getenv('ALIYUN_ACCESS_KEY_SECRET')
API_ID = os.getenv('QUICKBI_API_ID', 'ddee1f146b3a')


def fetch_quickbi_data(stat_date: str):
    """从 Quick BI 获取数据"""
    config = Config(
        access_key_id=ACCESS_KEY_ID,
        access_key_secret=ACCESS_KEY_SECRET,
        endpoint='quickbi-public.cn-hangzhou.aliyuncs.com'
    )
    client = Client(config)

    conditions = json.dumps({"stat_date": stat_date})
    request = models.QueryDataServiceRequest(
        api_id=API_ID,
        conditions=conditions
    )

    runtime = RuntimeOptions(
        read_timeout=180000,
        connect_timeout=30000
    )

    try:
        response = client.query_data_service_with_options(request, runtime)
        if response.body.result:
            return response.body.result.values or []
    except Exception as e:
        print(f"获取数据失败: {e}")

    return None


def upload_to_bigquery(data: list, project_id: str, dataset_id: str, batch_id: str):
    """上传数据到 BigQuery"""
    uploader = BigQueryUploader(project_id, dataset_id)
    count = uploader.upload_quickbi_campaigns(data, batch_id=batch_id)
    return count



def import_date(date_str: str, hour: int = 13):
    """
    导入指定日期的数据

    Args:
        date_str: 日期字符串，格式 YYYY-MM-DD
        hour: 模拟的小时数（用于生成 batch_id）
    """
    print("=" * 60)
    print(f"开始导入 {date_str} 的数据")
    print("=" * 60)

    # 转换日期格式为 YYYYMMDD
    stat_date = date_str.replace('-', '')

    # 获取数据
    print(f"\n[1] 从 QuickBI 获取 {date_str} 的数据...")
    data = fetch_quickbi_data(stat_date)

    if not data:
        print(f"[FAIL] 未获取到 {date_str} 的数据")
        return False

    print(f"[OK] 获取到 {len(data)} 条记录")

    # 检查是否包含 media_user_revenue 字段
    print(f"\n[2] 检查数据字段...")
    sample = data[0] if data else {}
    has_media_revenue = 'media_user_revenue' in sample

    print(f"  示例数据字段: {list(sample.keys())[:10]}...")
    print(f"  包含 media_user_revenue: {'YES' if has_media_revenue else 'NO'}")

    if not has_media_revenue:
        print(f"\n[WARNING] 数据中没有 media_user_revenue 字段！")
        print("  这可能导致日环比计算失败")
        response = input("  是否继续导入？(y/n): ")
        if response.lower() != 'y':
            return False

    # 统计 media_user_revenue 数据
    def safe_float(value):
        try:
            return float(value) if value else 0
        except (ValueError, TypeError):
            return 0

    media_revenue_count = sum(1 for row in data if safe_float(row.get('media_user_revenue', 0)) > 0)
    total_media_revenue = sum(safe_float(row.get('media_user_revenue', 0)) for row in data)

    print(f"\n  数据统计:")
    print(f"    总记录数: {len(data)}")
    print(f"    有媒体收入的记录: {media_revenue_count}")
    print(f"    媒体收入总计: ${total_media_revenue:,.2f}")

    # 生成 batch_id（模拟指定小时的数据）
    # 使用 0959 确保比旧的 0036 大，会被优先选中
    batch_id = f"{stat_date}_{hour:02d}0959"
    print(f"\n[3] 上传到 BigQuery...")
    print(f"  batch_id: {batch_id}")

    # 上传到 BigQuery
    bq_project = os.getenv('BQ_PROJECT_ID')
    bq_dataset = os.getenv('QUICKBI_BQ_DATASET_ID', 'quickbi_data')

    if not bq_project:
        print("[FAIL] 未配置 BQ_PROJECT_ID")
        return False

    count = upload_to_bigquery(data, bq_project, bq_dataset, batch_id)

    if count > 0:
        print(f"\n[OK] 成功导入 {date_str} 的数据")
        return True
    else:
        print(f"\n[FAIL] 导入 {date_str} 的数据失败")
        return False


def main():
    """主函数 - 导入历史数据"""
    print("=" * 60)
    print("历史数据导入工具")
    print("=" * 60)
    print("\n此工具将从 QuickBI 获取历史数据并导入到 BigQuery")
    print("确保数据包含 media_user_revenue 字段\n")

    # 要导入的日期列表
    dates_to_import = [
        ("2025-12-24", 13),  # 12-24 的 13:00 数据
        ("2025-12-23", 13),  # 12-23 的 13:00 数据
    ]

    results = []

    for date_str, hour in dates_to_import:
        success = import_date(date_str, hour)
        results.append((date_str, success))
        print("\n")

    # 汇总结果
    print("=" * 60)
    print("导入结果汇总")
    print("=" * 60)

    for date_str, success in results:
        status = "[OK] 成功" if success else "[FAIL] 失败"
        print(f"  {date_str}: {status}")

    success_count = sum(1 for _, success in results if success)
    print(f"\n总计: {success_count}/{len(results)} 成功")


if __name__ == '__main__':
    main()

