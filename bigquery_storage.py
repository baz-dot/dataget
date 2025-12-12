"""
BigQuery Storage Module
将数据标准化后写入 BigQuery
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Any
from google.cloud import bigquery
from dotenv import load_dotenv


class BigQueryUploader:
    """BigQuery 数据上传器"""

    def __init__(self, project_id: str, dataset_id: str, credentials_path: str = None):
        """
        初始化 BigQuery 上传器

        Args:
            project_id: GCP 项目 ID
            dataset_id: BigQuery 数据集 ID
            credentials_path: 服务账号密钥文件路径（可选）
        """
        self.project_id = project_id
        self.dataset_id = dataset_id

        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        self.client = bigquery.Client(project=project_id)

        # 自动创建数据集和表
        self._ensure_dataset_exists()

    def _ensure_dataset_exists(self, location: str = "asia-northeast3"):
        """确保数据集存在，不存在则创建"""
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
            print(f"数据集已存在: {dataset_ref}")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            dataset = self.client.create_dataset(dataset)
            print(f"✓ 已创建数据集: {dataset_ref} (位置: {location})")

    def _ensure_table_exists(self, table_id: str = "xmp_materials"):
        """确保表存在，不存在则创建"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("user_material_id", "STRING"),
            bigquery.SchemaField("user_material_name", "STRING"),
            bigquery.SchemaField("xmp_material_id", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("format", "STRING"),
            bigquery.SchemaField("designer_name", "STRING"),
            bigquery.SchemaField("impression", "INTEGER"),
            bigquery.SchemaField("click", "INTEGER"),
            bigquery.SchemaField("conversion", "INTEGER"),
            bigquery.SchemaField("cost", "FLOAT"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("ecpm", "FLOAT"),
            bigquery.SchemaField("click_rate", "FLOAT"),
            bigquery.SchemaField("conversion_rate", "FLOAT"),
            bigquery.SchemaField("material_create_time", "TIMESTAMP"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("page_number", "INTEGER"),
        ]

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            print(f"✓ 已创建表: {table_ref}")

    def upload_xmp_materials(self, data: dict, table_id: str = "xmp_materials") -> int:
        """
        上传 XMP 素材数据到 BigQuery

        Args:
            data: XMP API 响应数据（包含 pages 结构）
            table_id: 目标表 ID

        Returns:
            插入的行数
        """
        # 确保表存在
        self._ensure_table_exists(table_id)

        # 标准化数据
        rows = self._normalize_xmp_data(data)

        if not rows:
            print("没有数据需要上传")
            return 0

        # 获取表引用
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        # 插入数据
        errors = self.client.insert_rows_json(table_ref, rows)

        if errors:
            print(f"✗ BigQuery 插入错误: {errors}")
            return 0

        print(f"✓ 已上传 {len(rows)} 条记录到 BigQuery: {table_ref}")
        return len(rows)

    def _normalize_xmp_data(self, data: dict) -> List[Dict[str, Any]]:
        """
        标准化 XMP 数据，转换为 BigQuery 行格式

        Args:
            data: 原始 API 响应数据

        Returns:
            标准化后的行列表
        """
        rows = []
        fetched_at = datetime.utcnow().isoformat()

        # 从 pages 结构中提取数据
        pages = data.get('data', {}).get('pages', [])

        for page_data in pages:
            page_number = page_data.get('page', 0)
            records = page_data.get('list', [])

            for record in records:
                row = {
                    'user_material_id': self._safe_str(record.get('user_material_id')),
                    'user_material_name': self._safe_str(record.get('user_material_name')),
                    'xmp_material_id': self._safe_str(record.get('xmp_material_id')),
                    'channel': self._safe_str(record.get('channel')),
                    'format': self._safe_str(record.get('format')),
                    'designer_name': self._safe_str(record.get('designer_name')),
                    'impression': self._safe_int(record.get('impression')),
                    'click': self._safe_int(record.get('click')),
                    'conversion': self._safe_int(record.get('conversion')),
                    'cost': self._safe_float(record.get('currency_cost', record.get('cost'))),
                    'currency': self._safe_str(record.get('currency')),
                    'ecpm': self._safe_float(record.get('ecpm')),
                    'click_rate': self._safe_float(record.get('click_rate')),
                    'conversion_rate': self._safe_float(record.get('conversion_rate')),
                    'material_create_time': self._parse_timestamp(record.get('material_create_time')),
                    'fetched_at': fetched_at,
                    'page_number': page_number,
                }
                rows.append(row)

        return rows

    def _safe_str(self, value) -> str:
        """安全转换为字符串"""
        if value is None:
            return None
        return str(value)

    def _safe_int(self, value) -> int:
        """安全转换为整数"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value) -> float:
        """安全转换为浮点数"""
        if value is None:
            return None
        try:
            # 处理带逗号的数字字符串
            if isinstance(value, str):
                value = value.replace(',', '')
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_timestamp(self, value) -> str:
        """解析时间戳为 ISO 格式"""
        if value is None:
            return None
        try:
            # 如果是 Unix 时间戳（毫秒）
            if isinstance(value, (int, float)):
                if value > 1e12:  # 毫秒
                    value = value / 1000
                return datetime.utcfromtimestamp(value).isoformat()
            # 如果是字符串，尝试解析
            if isinstance(value, str):
                # 尝试常见格式
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d %H:%M:%S']:
                    try:
                        return datetime.strptime(value, fmt).isoformat()
                    except ValueError:
                        continue
                return value  # 返回原始字符串
        except Exception:
            return None



def upload_xmp_to_bigquery(data: dict, project_id: str, dataset_id: str,
                           credentials_path: str = None) -> int:
    """
    便捷函数：上传 XMP 数据到 BigQuery

    Args:
        data: XMP 数据
        project_id: GCP 项目 ID
        dataset_id: BigQuery 数据集 ID
        credentials_path: 服务账号密钥文件路径

    Returns:
        插入的行数
    """
    uploader = BigQueryUploader(project_id, dataset_id, credentials_path)
    return uploader.upload_xmp_materials(data)


if __name__ == "__main__":
    # 测试代码
    load_dotenv()

    project_id = os.getenv('BQ_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET_ID')

    if not project_id or not dataset_id:
        print("错误: 请在 .env 文件中设置 BQ_PROJECT_ID 和 BQ_DATASET_ID")
        exit(1)

    # 从本地文件读取数据测试
    try:
        with open("api_responses.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        count = upload_xmp_to_bigquery(data, project_id, dataset_id)
        print(f"测试上传完成: {count} 条记录")
    except FileNotFoundError:
        print("未找到 api_responses.json 文件，请先运行爬虫")
    except Exception as e:
        print(f"测试上传失败: {e}")
