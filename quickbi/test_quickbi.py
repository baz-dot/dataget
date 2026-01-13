"""
Quick BI API 数据抓取脚本
从 Quick BI 数据服务获取广告投放数据并落库到 BigQuery
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone

# 北京时区 (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))

# 设置控制台编码为 UTF-8，避免 Windows 下的编码问题
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

from alibabacloud_quickbi_public20220101.client import Client
from alibabacloud_quickbi_public20220101 import models
from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util.models import RuntimeOptions
from dotenv import load_dotenv

# 添加父目录到路径以导入 storage 模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载环境变量
load_dotenv()

# 配置 AccessKey (从环境变量读取)
ACCESS_KEY_ID = os.getenv('ALIYUN_ACCESS_KEY_ID')
ACCESS_KEY_SECRET = os.getenv('ALIYUN_ACCESS_KEY_SECRET')

# Quick BI API 配置
API_ID = os.getenv('QUICKBI_API_ID', 'ddee1f146b3a')
OVERVIEW_API_ID = os.getenv('QUICKBI_OVERVIEW_API_ID', '7a15b44f69fd')  # 获取 total_revenue


def fetch_quickbi_data(stat_date: str = None):
    """
    从 Quick BI 获取数据

    Args:
        stat_date: 查询日期，格式 YYYYMMDD，默认为昨天

    Returns:
        数据列表
    """
    if stat_date is None:
        # 默认查询今天的数据（使用北京时间）
        stat_date = datetime.now(BEIJING_TZ).strftime('%Y%m%d')

    # 创建客户端
    config = Config(
        access_key_id=ACCESS_KEY_ID,
        access_key_secret=ACCESS_KEY_SECRET,
        endpoint='quickbi-public.cn-hangzhou.aliyuncs.com'
    )

    client = Client(config)

    # 构建请求
    conditions = json.dumps({"stat_date": stat_date})
    print(f'查询日期: {stat_date}')
    print(f'Conditions: {conditions}')

    request = models.QueryDataServiceRequest(
        api_id=API_ID,
        conditions=conditions
    )

    # 设置超时时间（数据量大时需要更长时间）
    runtime = RuntimeOptions(
        read_timeout=180000,    # 读取超时: 3分钟
        connect_timeout=30000   # 连接超时: 30秒
    )

    # 重试配置
    max_retries = 3
    retry_delays = [10, 30, 60]  # 重试间隔：10秒、30秒、60秒

    for attempt in range(max_retries):
        try:
            # 调用 API
            response = client.query_data_service_with_options(request, runtime)

            print('=' * 50)
            print('API 调用成功!')
            print('=' * 50)
            print(f'Success: {response.body.success}')

            if response.body.result:
                values = response.body.result.values or []
                print(f'获取到 {len(values)} 条记录')
                if values:
                    print(f'示例数据: {values[0]}')
                return values

        except Exception as e:
            error_msg = str(e)
            is_retryable = (
                '503' in error_msg or
                'ServiceUnavailable' in error_msg or
                'timeout' in error_msg.lower() or
                'Datasource.Sql.ExecuteFailed' in error_msg  # QuickBI SQL 执行失败也重试
            )

            if is_retryable and attempt < max_retries - 1:
                delay = retry_delays[attempt]
                print(f'[重试] 服务暂时不可用，{delay}秒后重试 ({attempt + 1}/{max_retries})...')
                print(f'[错误详情] {error_msg[:200]}')  # 打印错误详情（截取前200字符）
                import time
                time.sleep(delay)
                continue

            print('=' * 50)
            print('ERROR')
            print('=' * 50)
            print(f'Raw Error: {error_msg}')

            if 'Ram.AuthCheck.Error' in error_msg:
                print('Summary: Permission Denied!')
                print('AccessKey works, but NO Quick BI API permission')
                print('Solution: Admin needs to add AliyunQuickBIFullAccess permission')

            return None

    return None


def fetch_overview_data(stat_date: str = None):
    """
    从 Quick BI 获取 Overview 数据 (total_revenue)
    """
    if stat_date is None:
        stat_date = datetime.now(BEIJING_TZ).strftime('%Y%m%d')

    config = Config(
        access_key_id=ACCESS_KEY_ID,
        access_key_secret=ACCESS_KEY_SECRET,
        endpoint='quickbi-public.cn-hangzhou.aliyuncs.com'
    )
    client = Client(config)

    conditions = json.dumps({"stat_date": stat_date})
    request = models.QueryDataServiceRequest(
        api_id=OVERVIEW_API_ID,
        conditions=conditions
    )

    runtime = RuntimeOptions(read_timeout=60000, connect_timeout=30000)

    try:
        response = client.query_data_service_with_options(request, runtime)
        if response.body.result and response.body.result.values:
            data = response.body.result.values[0]
            total_revenue = float(data.get('total_revenue', 0) or 0)
            print(f'[Overview] total_revenue: ${total_revenue:,.2f}')
            return {'total_revenue': total_revenue}
    except Exception as e:
        print(f'[Overview] 获取失败: {e}')

    return {'total_revenue': 0}


def upload_to_gcs(data: list, bucket_name: str, batch_id: str = None):
    """上传数据到 GCS"""
    try:
        from gcs_storage import GCSUploader

        uploader = GCSUploader(bucket_name)
        blob_path = uploader.generate_quickbi_blob_path("campaigns", batch_id=batch_id)

        # 包装数据
        wrapped_data = {
            "code": 200,
            "data": {
                "total": len(data),
                "list": data
            },
            "fetched_at": datetime.utcnow().isoformat()
        }

        uploader.upload_json(wrapped_data, blob_path)
        print(f"✓ 已上传到 GCS: gs://{bucket_name}/{blob_path}")

    except ImportError:
        print("⚠ 未安装 google-cloud-storage，跳过 GCS 上传")
    except Exception as e:
        print(f"✗ GCS 上传失败: {e}")


def upload_to_bigquery(data: list, project_id: str, dataset_id: str, batch_id: str = None, overview_data: dict = None):
    """上传数据到 BigQuery"""
    try:
        from bigquery_storage import BigQueryUploader

        uploader = BigQueryUploader(project_id, dataset_id)
        count = uploader.upload_quickbi_campaigns(data, batch_id=batch_id)
        print(f"✓ 已上传 {count} 条记录到 BigQuery")

        # 上传 Overview 数据
        if overview_data and overview_data.get('total_revenue', 0) > 0:
            uploader.upload_overview_data(overview_data, batch_id=batch_id)
            print(f"✓ 已上传 Overview 数据到 BigQuery")

        if batch_id:
            print(f"  批次表: {dataset_id}.quickbi_batch_{batch_id}")
        return count

    except ImportError:
        print("⚠ 未安装 google-cloud-bigquery，跳过 BigQuery 上传")
    except Exception as e:
        print(f"✗ BigQuery 上传失败: {e}")

    return 0


def main():
    """主函数"""
    # 加载环境变量
    load_dotenv()

    # 获取查询日期（可从命令行参数或环境变量传入）
    stat_date = None
    if len(sys.argv) > 1:
        stat_date = sys.argv[1]
    elif os.getenv('FETCH_YESTERDAY', '').lower() == 'true':
        # 凌晨 1 点采集昨天的数据（用于日报）
        yesterday = datetime.now(BEIJING_TZ) - timedelta(days=1)
        stat_date = yesterday.strftime('%Y%m%d')
        print(f"[T-1 模式] 采集昨天的数据: {stat_date}")

    # 获取数据
    data = fetch_quickbi_data(stat_date)

    if not data:
        print("未获取到数据")
        return

    # 获取 Overview 数据 (total_revenue)
    overview_data = fetch_overview_data(stat_date)

    # 生成批次 ID（使用北京时间）
    batch_id = datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M%S')
    print(f"\n=== 批次 ID: {batch_id} ===")

    # GCS 配置
    gcs_bucket = os.getenv('GCS_BUCKET_NAME')
    if gcs_bucket:
        upload_to_gcs(data, gcs_bucket, batch_id)

    # BigQuery 配置
    bq_project = os.getenv('BQ_PROJECT_ID')
    bq_dataset = os.getenv('QUICKBI_BQ_DATASET_ID', 'quickbi_data')

    if bq_project and bq_dataset:
        upload_to_bigquery(data, bq_project, bq_dataset, batch_id, overview_data)
    else:
        print("⚠ 未配置 BigQuery，跳过落库")
        print("  请在 .env 文件中设置 BQ_PROJECT_ID 和 BQ_DATASET_ID")


if __name__ == '__main__':
    main()
