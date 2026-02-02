"""
BigQuery Storage Module
将数据标准化后写入 BigQuery
"""

import os
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions
from dotenv import load_dotenv

# 导入日志和重试模块
try:
    from utils.logger import get_logger
    from utils.retry import retry_with_backoff, BIGQUERY_RETRY_CONFIG, RetryConfig
except ImportError:
    # 兼容直接运行
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from utils.logger import get_logger
    from utils.retry import retry_with_backoff, BIGQUERY_RETRY_CONFIG, RetryConfig

# 初始化 logger
logger = get_logger("dataget.bigquery")


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
            logger.debug(f"数据集已存在: {dataset_ref}")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            dataset = self.client.create_dataset(dataset)
            logger.info(f"已创建数据集: {dataset_ref} (位置: {location})")

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
            bigquery.SchemaField("batch_id", "STRING"),  # 批次 ID，用于区分不同爬取批次
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("page_number", "INTEGER"),
        ]

        try:
            self.client.get_table(table_ref)
            logger.debug(f"表已存在: {table_ref}")
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            logger.info(f"已创建表: {table_ref}")

    def _ensure_dataeye_table_exists(self, table_id: str = "dataeye_materials"):
        """确保 DataEye 表存在，不存在则创建"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("material_id", "INTEGER"),
            bigquery.SchemaField("media_name", "STRING"),
            bigquery.SchemaField("countries", "STRING"),  # JSON 字符串
            bigquery.SchemaField("product_name", "STRING"),
            bigquery.SchemaField("publisher_name", "STRING"),
            bigquery.SchemaField("playlet_name", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("material_type", "INTEGER"),
            bigquery.SchemaField("material_width", "INTEGER"),
            bigquery.SchemaField("material_height", "INTEGER"),
            bigquery.SchemaField("first_seen", "DATE"),
            bigquery.SchemaField("last_seen", "DATE"),
            bigquery.SchemaField("release_days", "INTEGER"),
            bigquery.SchemaField("exposure_num", "INTEGER"),
            bigquery.SchemaField("download_num", "INTEGER"),
            bigquery.SchemaField("heat_num", "INTEGER"),
            bigquery.SchemaField("video_url", "STRING"),
            bigquery.SchemaField("pic_url", "STRING"),
            bigquery.SchemaField("narration", "STRING"),
            bigquery.SchemaField("narration_zh", "STRING"),
            bigquery.SchemaField("recognize_lang", "STRING"),
            bigquery.SchemaField("fb_home_url", "STRING"),
            bigquery.SchemaField("fb_home_name", "STRING"),
            bigquery.SchemaField("batch_id", "STRING"),  # 批次 ID，用于区分不同爬取批次
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(table_ref)
            logger.debug(f"表已存在: {table_ref}")
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            logger.info(f"已创建表: {table_ref}")

    def upload_xmp_materials(self, data: dict, table_id: str = "xmp_materials", batch_id: str = None) -> int:
        """
        上传 XMP 素材数据到 BigQuery（追加模式）

        Args:
            data: XMP API 响应数据（包含 pages 结构）
            table_id: 目标表 ID（固定表名，不再按批次创建新表）
            batch_id: 批次 ID（用于标记数据来源批次，不再创建新表）

        Returns:
            插入的行数
        """
        # 确保表存在（使用固定表名，不再按批次创建新表）
        self._ensure_table_exists(table_id)

        # 标准化数据（传入 batch_id）
        rows = self._normalize_xmp_data(data, batch_id)

        if not rows:
            logger.warning("没有数据需要上传")
            return 0

        # 获取表引用
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        # 插入数据（带重试）
        errors = self._insert_rows_with_retry(table_ref, rows)

        if errors:
            logger.error(f"BigQuery 插入错误: {errors}")
            return 0

        logger.info(f"已上传 {len(rows)} 条记录到 BigQuery: {table_ref}")
        return len(rows)

    def upload_dataeye_materials(self, data: dict, table_id: str = "dataeye_materials", batch_id: str = None, table_prefix: str = None) -> int:
        """
        上传 DataEye 素材数据到 BigQuery（追加模式）

        Args:
            data: DataEye API 响应数据（包含 data.pages 结构）
            table_id: 目标表 ID（默认 dataeye_materials）
            batch_id: 批次 ID（用于标记数据来源批次，不再创建新表）
            table_prefix: 表名前缀（用于区分版本，如 dataeye_overseas 或 dataeye_china）

        Returns:
            插入的行数
        """
        # 使用固定表名：dataeye_overseas 或 dataeye_china
        # 不再每次创建新表，而是追加数据到固定表
        if table_prefix:
            table_id = table_prefix  # 直接使用前缀作为表名，如 dataeye_overseas

        # 确保表存在
        self._ensure_dataeye_table_exists(table_id)

        # 从 pages 结构中提取所有记录
        all_records = []
        pages = data.get('data', {}).get('pages', [])
        for page_data in pages:
            records = page_data.get('list', [])
            all_records.extend(records)

        # 标准化数据（传入批次 ID）
        rows = self._normalize_dataeye_data(all_records, batch_id)

        if not rows:
            logger.warning("没有数据需要上传")
            return 0

        # 获取表引用
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        # 插入数据（带重试）
        errors = self._insert_rows_with_retry(table_ref, rows)

        if errors:
            logger.error(f"BigQuery 插入错误: {errors}")
            return 0

        logger.info(f"已上传 {len(rows)} 条 DataEye 记录到 BigQuery: {table_ref}")
        return len(rows)

    def _normalize_xmp_data(self, data: dict, batch_id: str = None) -> List[Dict[str, Any]]:
        """
        标准化 XMP 数据，转换为 BigQuery 行格式

        Args:
            data: 原始 API 响应数据
            batch_id: 批次 ID

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
                    'batch_id': batch_id,  # 批次 ID
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

    def _calculate_roas(self, revenue, spend) -> float:
        """计算 ROAS = new_user_revenue / spend"""
        revenue_val = self._safe_float(revenue)
        spend_val = self._safe_float(spend)
        if revenue_val is None or spend_val is None or spend_val == 0:
            return None
        return revenue_val / spend_val

    def _calculate_ratio(self, numerator, denominator) -> float:
        """通用比率计算 = numerator / denominator"""
        num_val = self._safe_float(numerator)
        denom_val = self._safe_float(denominator)
        if num_val is None or denom_val is None or denom_val == 0:
            return None
        return num_val / denom_val

    def _calculate_cpm(self, spend, impressions) -> float:
        """计算 CPM = (spend / impressions) * 1000"""
        spend_val = self._safe_float(spend)
        impressions_val = self._safe_float(impressions)
        if spend_val is None or impressions_val is None or impressions_val == 0:
            return None
        return (spend_val / impressions_val) * 1000

    def _insert_rows_with_retry(self, table_ref: str, rows: List[Dict], max_retries: int = 3) -> List:
        """
        带重试的 BigQuery 插入操作

        Args:
            table_ref: 表引用
            rows: 要插入的行
            max_retries: 最大重试次数

        Returns:
            错误列表，空列表表示成功
        """
        last_errors = []
        for attempt in range(max_retries):
            try:
                errors = self.client.insert_rows_json(table_ref, rows)
                if not errors:
                    return []
                last_errors = errors
                # 如果是可重试的错误，等待后重试
                if attempt < max_retries - 1:
                    delay = 2 ** attempt  # 指数退避: 1, 2, 4 秒
                    logger.warning(f"BigQuery 插入失败 (尝试 {attempt + 1}/{max_retries}): {errors}, {delay}s 后重试...")
                    time.sleep(delay)
            except gcp_exceptions.ServiceUnavailable as e:
                last_errors = [str(e)]
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning(f"BigQuery 服务不可用 (尝试 {attempt + 1}/{max_retries}): {e}, {delay}s 后重试...")
                    time.sleep(delay)
            except gcp_exceptions.DeadlineExceeded as e:
                last_errors = [str(e)]
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning(f"BigQuery 请求超时 (尝试 {attempt + 1}/{max_retries}): {e}, {delay}s 后重试...")
                    time.sleep(delay)
            except Exception as e:
                logger.error(f"BigQuery 插入异常: {e}")
                return [str(e)]

        return last_errors

    def _query_with_retry(self, query: str, max_retries: int = 3):
        """
        带重试的 BigQuery 查询操作

        Args:
            query: SQL 查询语句
            max_retries: 最大重试次数

        Returns:
            查询结果
        """
        last_exception = None
        for attempt in range(max_retries):
            try:
                return self.client.query(query).result()
            except (gcp_exceptions.ServiceUnavailable, gcp_exceptions.DeadlineExceeded) as e:
                last_exception = e
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning(f"BigQuery 查询失败 (尝试 {attempt + 1}/{max_retries}): {e}, {delay}s 后重试...")
                    time.sleep(delay)
            except Exception as e:
                logger.error(f"BigQuery 查询异常: {e}")
                raise

        raise last_exception

    def _ensure_quickbi_table_exists(self, table_id: str = "quickbi_campaigns"):
        """确保 Quick BI 表存在，不存在则创建（单表模式，按 stat_date 分区）"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("stat_date", "DATE"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("drama_id", "STRING"),
            bigquery.SchemaField("drama_name", "STRING"),
            bigquery.SchemaField("genre_names", "STRING"),
            bigquery.SchemaField("optimizer", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("impressions", "FLOAT"),
            bigquery.SchemaField("clicks", "FLOAT"),
            bigquery.SchemaField("spend", "FLOAT"),
            bigquery.SchemaField("new_users", "INTEGER"),
            bigquery.SchemaField("new_payers", "INTEGER"),
            bigquery.SchemaField("new_user_revenue", "FLOAT"),
            bigquery.SchemaField("media_user_revenue", "FLOAT"),
            bigquery.SchemaField("media_iap_revenue", "FLOAT"),
            bigquery.SchemaField("media_sub_revenue", "FLOAT"),
            bigquery.SchemaField("media_ad_revenue", "FLOAT"),
            # 计算字段
            bigquery.SchemaField("cpi", "FLOAT"),
            bigquery.SchemaField("cac", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("cvr", "FLOAT"),
            bigquery.SchemaField("cpc", "FLOAT"),
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("media_d0_roas", "FLOAT"),
            bigquery.SchemaField("new_user_paying_rate", "FLOAT"),
            bigquery.SchemaField("arpu", "FLOAT"),
            # 批次追踪字段
            bigquery.SchemaField("batch_id", "STRING"),  # 批次ID: 20251222_030029
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            # 按 stat_date 分区
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="stat_date"
            )
            # 按 optimizer 和 drama_name 聚簇优化查询
            table.clustering_fields = ["optimizer", "drama_name"]
            self.client.create_table(table)
            logger.info(f"已创建分区表: {table_ref} (按 stat_date 分区)")

    def _ensure_xmp_internal_table_exists(self, table_id: str = "xmp_internal_campaigns"):
        """确保 XMP 内部 API 表存在，不存在则创建"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("stat_date", "DATE"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("drama_id", "STRING"),
            bigquery.SchemaField("drama_name", "STRING"),
            bigquery.SchemaField("optimizer", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("impressions", "FLOAT"),
            bigquery.SchemaField("clicks", "FLOAT"),
            bigquery.SchemaField("spend", "FLOAT"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("conversions", "INTEGER"),
            bigquery.SchemaField("payers", "INTEGER"),
            # 计算字段
            bigquery.SchemaField("cpi", "FLOAT"),
            bigquery.SchemaField("cpc", "FLOAT"),
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("cvr", "FLOAT"),
            bigquery.SchemaField("roas", "FLOAT"),
            bigquery.SchemaField("paying_rate", "FLOAT"),
            bigquery.SchemaField("arpu", "FLOAT"),
            # 批次追踪字段
            bigquery.SchemaField("batch_id", "STRING"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="stat_date"
            )
            table.clustering_fields = ["optimizer", "channel"]
            self.client.create_table(table)
            logger.info(f"已创建 XMP 内部 API 表: {table_ref}")

    def upload_quickbi_campaigns(self, data: List[Dict], table_id: str = "quickbi_campaigns", batch_id: str = None) -> int:
        """
        上传 Quick BI 广告数据到 BigQuery（单表模式）

        Args:
            data: Quick BI API 返回的数据列表
            table_id: 目标表 ID
            batch_id: 批次 ID，用于标记本次抓取的数据

        Returns:
            插入的行数
        """
        # 确保主表存在
        self._ensure_quickbi_table_exists(table_id)

        # 标准化数据（传入 batch_id）
        rows = self._normalize_quickbi_data(data, batch_id)

        if not rows:
            logger.warning("没有数据需要上传")
            return 0

        # 写入主表（带重试）
        main_table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        errors = self._insert_rows_with_retry(main_table_ref, rows)

        if errors:
            logger.error(f"BigQuery 插入错误: {errors}")
            return 0

        logger.info(f"已上传 {len(rows)} 条 Quick BI 记录到: {main_table_ref}")
        if batch_id:
            logger.debug(f"批次 ID: {batch_id}")

        return len(rows)

    def upload_xmp_internal_campaigns(self, data: List[Dict], table_id: str = "xmp_internal_campaigns", batch_id: str = None) -> int:
        """
        上传 XMP 内部 API 数据到 BigQuery

        Args:
            data: XMP 内部 API 返回的数据列表
            table_id: 目标表 ID
            batch_id: 批次 ID

        Returns:
            插入的行数
        """
        self._ensure_xmp_internal_table_exists(table_id)

        rows = self._normalize_xmp_internal_data(data, batch_id)

        if not rows:
            logger.warning("没有 XMP 内部 API 数据需要上传")
            return 0

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        errors = self._insert_rows_with_retry(table_ref, rows)

        if errors:
            logger.error(f"BigQuery 插入错误: {errors}")
            return 0

        logger.info(f"已上传 {len(rows)} 条 XMP 内部 API 记录到: {table_ref}")
        return len(rows)

    def _normalize_xmp_internal_data(self, data: List[Dict], batch_id: str = None) -> List[Dict[str, Any]]:
        """标准化 XMP 内部 API 数据"""
        import re
        rows = []
        fetched_at = datetime.utcnow().isoformat()

        for record in data:
            # 从 campaign_name 解析 optimizer 和 drama_id
            campaign_name = record.get('campaign_name', '')
            optimizer = None
            drama_id = None

            optimizer_match = re.search(r'optimizer-([^_]+)', campaign_name)
            if optimizer_match:
                optimizer = optimizer_match.group(1)

            drama_match = re.search(r'dramaid-(\d+)', campaign_name)
            if drama_match:
                drama_id = drama_match.group(1)

            # 计算字段
            spend = float(record.get('spend') or record.get('cost') or 0)
            revenue = float(record.get('revenue') or 0)
            # TikTok 收入明细字段
            tk_complete_payment = float(record.get('tk_complete_payment') or 0)
            tk_purchase_value = float(record.get('tk_purchase_value') or 0)
            conversions = int(record.get('conversions') or record.get('conversion') or 0)
            payers = int(record.get('payers') or 0)
            impressions = float(record.get('impressions') or record.get('impression') or 0)
            clicks = float(record.get('clicks') or record.get('click') or 0)

            row = {
                'stat_date': record.get('stat_date') or record.get('date'),
                'country': record.get('country') or record.get('geo'),
                'channel': record.get('channel'),
                'campaign_id': str(record.get('campaign_id', '')),
                'campaign_name': campaign_name,
                'drama_id': drama_id,
                'drama_name': record.get('drama_name'),
                'optimizer': optimizer,
                'status': record.get('status', ''),
                'impressions': impressions,
                'clicks': clicks,
                'spend': spend,
                'revenue': revenue,
                'conversions': conversions,
                'payers': payers,
                'cpi': spend / conversions if conversions > 0 else None,
                'cpc': spend / clicks if clicks > 0 else None,
                'cpm': (spend / impressions * 1000) if impressions > 0 else None,
                'ctr': (clicks / impressions) if impressions > 0 else None,
                'cvr': (conversions / clicks) if clicks > 0 else None,
                'roas': revenue / spend if spend > 0 else None,
                'paying_rate': payers / conversions if conversions > 0 else None,
                'arpu': revenue / conversions if conversions > 0 else None,
                'batch_id': batch_id,
                'fetched_at': fetched_at,
                'tk_complete_payment': tk_complete_payment,
                'tk_purchase_value': tk_purchase_value,
            }
            rows.append(row)

        return rows

    def _normalize_quickbi_data(self, data: List[Dict], batch_id: str = None) -> List[Dict[str, Any]]:
        """
        标准化 Quick BI 数据，转换为 BigQuery 行格式

        Args:
            data: Quick BI 原始数据列表
            batch_id: 批次 ID

        Returns:
            标准化后的行列表
        """
        rows = []
        fetched_at = datetime.utcnow().isoformat()

        for record in data:
            # 解析日期格式 (20251217 -> 2025-12-17)
            stat_date_raw = record.get('stat_date', '')
            stat_date = None
            if stat_date_raw and len(stat_date_raw) == 8:
                stat_date = f"{stat_date_raw[:4]}-{stat_date_raw[4:6]}-{stat_date_raw[6:8]}"

            row = {
                'stat_date': stat_date,
                'country': self._safe_str(record.get('country')),
                'channel': self._safe_str(record.get('channel')),
                'campaign_id': self._safe_str(record.get('campaign_id')),
                'campaign_name': self._safe_str(record.get('campaign_name')),
                'drama_id': self._safe_str(record.get('drama_id')),
                'drama_name': self._safe_str(record.get('drama_name')),
                'genre_names': self._safe_str(record.get('genre_names')),
                'optimizer': self._safe_str(record.get('optimizer')),
                'status': self._safe_str(record.get('status')),
                'impressions': self._safe_float(record.get('impressions')),
                'clicks': self._safe_float(record.get('clicks')),
                'spend': self._safe_float(record.get('spend')),
                'new_users': self._safe_int(record.get('new_users')),
                'new_payers': self._safe_int(record.get('new_payers')),
                'new_user_revenue': self._safe_float(record.get('new_user_revenue')),
                'media_user_revenue': self._safe_float(record.get('media_user_revenue')),
                'media_iap_revenue': self._safe_float(record.get('media_iap_purchases') or record.get('media_iap_revenue')),
                'media_sub_revenue': self._safe_float(record.get('media_sub_revenue')),
                'media_ad_revenue': self._safe_float(record.get('media_ad_revenue')),
                # 计算字段
                'cpi': self._calculate_ratio(record.get('spend'), record.get('new_users')),
                'cac': self._calculate_ratio(record.get('spend'), record.get('new_users')),
                'ctr': self._calculate_ratio(record.get('clicks'), record.get('impressions')),
                'cvr': self._calculate_ratio(record.get('new_users'), record.get('clicks')),
                'cpc': self._calculate_ratio(record.get('spend'), record.get('clicks')),
                'cpm': self._calculate_cpm(record.get('spend'), record.get('impressions')),
                'media_d0_roas': self._calculate_roas(record.get('media_user_revenue'), record.get('spend')),
                'new_user_paying_rate': self._calculate_ratio(record.get('new_payers'), record.get('new_users')),
                'arpu': self._calculate_ratio(record.get('new_user_revenue'), record.get('new_users')),
                # 批次追踪字段
                'batch_id': batch_id,
                'fetched_at': fetched_at,
            }
            rows.append(row)

        return rows

    def upload_overview_data(self, data: Dict, table_id: str = "quickbi_overview", batch_id: str = None, stat_date: str = None) -> bool:
        """
        上传 Overview 数据 (total_revenue, total_spend) 到 BigQuery

        Args:
            data: 包含 total_revenue 和 total_spend 的数据字典
            table_id: 表名
            batch_id: 批次ID
            stat_date: 数据日期 (格式: YYYY-MM-DD)，如果不传则使用今天
        """
        from datetime import datetime
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        # 创建表（如果不存在）- 包含 total_spend 字段
        schema = [
            bigquery.SchemaField("stat_date", "DATE"),
            bigquery.SchemaField("total_revenue", "FLOAT"),
            bigquery.SchemaField("total_spend", "FLOAT"),
            bigquery.SchemaField("batch_id", "STRING"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP"),
        ]

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            logger.info(f"创建表: {table_ref}")

        # 插入数据 - 使用传入的 stat_date 或默认今天
        data_date = stat_date if stat_date else datetime.now().strftime('%Y-%m-%d')
        row = {
            'stat_date': data_date,
            'total_revenue': data.get('total_revenue', 0),
            'total_spend': data.get('total_spend', 0),
            'batch_id': batch_id,
            'fetched_at': datetime.utcnow().isoformat(),
        }

        errors = self.client.insert_rows_json(table_ref, [row])
        if errors:
            logger.error(f"上传 Overview 数据失败: {errors}")
            return False
        return True

    def _ensure_xmp_campaigns_table_exists(self, table_id: str = "xmp_campaigns"):
        """确保 XMP campaigns 表存在"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("stat_date", "DATE"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("account_name", "STRING"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("optimizer", "STRING"),
            bigquery.SchemaField("drama_id", "STRING"),
            bigquery.SchemaField("drama_name", "STRING"),
            bigquery.SchemaField("impressions", "FLOAT"),
            bigquery.SchemaField("clicks", "FLOAT"),
            bigquery.SchemaField("spend", "FLOAT"),
            bigquery.SchemaField("conversions", "INTEGER"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("payers", "INTEGER"),
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("cpc", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("cvr", "FLOAT"),
            bigquery.SchemaField("cpi", "FLOAT"),
            bigquery.SchemaField("roas", "FLOAT"),
            bigquery.SchemaField("timezone", "STRING"),
            bigquery.SchemaField("batch_id", "STRING"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="stat_date"
            )
            table.clustering_fields = ["optimizer", "drama_id"]
            self.client.create_table(table)
            logger.info(f"已创建 XMP campaigns 表: {table_ref}")
            import time
            time.sleep(5)  # 等待表创建完成

    def upload_xmp_campaigns(self, data: List[Dict], table_id: str = "xmp_campaigns", batch_id: str = None) -> int:
        """上传 XMP campaign 数据到 BigQuery"""
        self._ensure_xmp_campaigns_table_exists(table_id)

        if not data:
            logger.warning("没有数据需要上传")
            return 0

        fetched_at = datetime.utcnow().isoformat()
        rows = []
        for record in data:
            row = {
                'stat_date': record.get('stat_date'),
                'country': record.get('country'),
                'channel': record.get('channel'),
                'account_id': record.get('account_id'),
                'account_name': record.get('account_name'),
                'campaign_id': record.get('campaign_id'),
                'campaign_name': record.get('campaign_name'),
                'optimizer': record.get('optimizer'),
                'drama_id': record.get('drama_id'),
                'drama_name': record.get('drama_name'),
                'impressions': record.get('impressions'),
                'clicks': record.get('clicks'),
                'spend': record.get('spend'),
                'conversions': record.get('conversions'),
                'revenue': record.get('revenue'),
                'payers': record.get('payers'),
                'cpm': record.get('cpm'),
                'cpc': record.get('cpc'),
                'ctr': record.get('ctr'),
                'cvr': record.get('cvr'),
                'cpi': record.get('cpi'),
                'roas': record.get('roas'),
                'timezone': record.get('timezone'),
                'batch_id': batch_id,
                'fetched_at': fetched_at,
            }
            rows.append(row)

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        errors = self._insert_rows_with_retry(table_ref, rows)

        if errors:
            logger.error(f"BigQuery 插入错误: {errors}")
            return 0

        logger.info(f"已上传 {len(rows)} 条 XMP campaign 记录")
        return len(rows)

    # ============ 剪辑师/投手统计表 ============

    def _ensure_xmp_editor_stats_table_exists(self, table_id: str = "xmp_editor_stats"):
        """确保剪辑师统计表存在"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("stat_date", "DATE"),
            bigquery.SchemaField("channel", "STRING"),  # tiktok/facebook
            bigquery.SchemaField("editor_name", "STRING"),  # 剪辑师名称
            bigquery.SchemaField("material_count", "INTEGER"),  # 素材数
            bigquery.SchemaField("spend", "FLOAT"),  # 消耗
            bigquery.SchemaField("revenue", "FLOAT"),  # 收入
            bigquery.SchemaField("roas", "FLOAT"),  # ROAS
            bigquery.SchemaField("impressions", "FLOAT"),
            bigquery.SchemaField("clicks", "FLOAT"),
            bigquery.SchemaField("hot_count", "INTEGER"),  # 爆款数
            bigquery.SchemaField("hot_rate", "FLOAT"),  # 爆款率
            bigquery.SchemaField("top_material", "STRING"),  # Top素材名称
            bigquery.SchemaField("top_material_spend", "FLOAT"),
            bigquery.SchemaField("top_material_roas", "FLOAT"),
            bigquery.SchemaField("batch_id", "STRING"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="stat_date"
            )
            table.clustering_fields = ["editor_name", "channel"]
            self.client.create_table(table)
            logger.info(f"已创建剪辑师统计表: {table_ref}")

    def _ensure_xmp_optimizer_stats_table_exists(self, table_id: str = "xmp_optimizer_stats"):
        """确保投手统计表存在"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("stat_date", "DATE"),
            bigquery.SchemaField("channel", "STRING"),  # tiktok/facebook
            bigquery.SchemaField("optimizer_name", "STRING"),  # 投手名称
            bigquery.SchemaField("campaign_count", "INTEGER"),  # 计划数
            bigquery.SchemaField("spend", "FLOAT"),  # 消耗
            bigquery.SchemaField("revenue", "FLOAT"),  # 收入
            bigquery.SchemaField("roas", "FLOAT"),  # ROAS
            bigquery.SchemaField("impressions", "FLOAT"),
            bigquery.SchemaField("clicks", "FLOAT"),
            bigquery.SchemaField("conversions", "INTEGER"),
            bigquery.SchemaField("top_campaign", "STRING"),  # Top计划名称
            bigquery.SchemaField("top_campaign_spend", "FLOAT"),
            bigquery.SchemaField("top_campaign_roas", "FLOAT"),
            bigquery.SchemaField("batch_id", "STRING"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="stat_date"
            )
            table.clustering_fields = ["optimizer_name", "channel"]
            self.client.create_table(table)
            logger.info(f"已创建投手统计表: {table_ref}")

    def upload_editor_stats(self, data: List[Dict], table_id: str = "xmp_editor_stats", batch_id: str = None) -> int:
        """
        上传剪辑师统计数据到 BigQuery

        Args:
            data: 剪辑师统计数据列表
            table_id: 目标表 ID
            batch_id: 批次 ID

        Returns:
            插入的行数
        """
        self._ensure_xmp_editor_stats_table_exists(table_id)

        if not data:
            logger.warning("没有剪辑师数据需要上传")
            return 0

        fetched_at = datetime.utcnow().isoformat()
        rows = []
        for record in data:
            row = {
                'stat_date': record.get('stat_date'),
                'channel': record.get('channel'),
                'editor_name': record.get('name') or record.get('editor_name'),
                'material_count': record.get('material_count', 0),
                'spend': record.get('total_cost') or record.get('spend', 0),
                'revenue': record.get('total_revenue') or record.get('revenue', 0),
                'roas': record.get('d0_roas') or record.get('roas', 0),
                'impressions': record.get('impressions', 0),
                'clicks': record.get('clicks', 0),
                'hot_count': record.get('hot_count', 0),
                'hot_rate': record.get('hot_rate', 0),
                'top_material': record.get('top_material', ''),
                'top_material_spend': record.get('top_material_cost') or record.get('top_material_spend', 0),
                'top_material_roas': record.get('top_material_roas', 0),
                'batch_id': batch_id,
                'fetched_at': fetched_at,
            }
            rows.append(row)

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        errors = self._insert_rows_with_retry(table_ref, rows)

        if errors:
            logger.error(f"BigQuery 插入剪辑师数据错误: {errors}")
            return 0

        logger.info(f"已上传 {len(rows)} 条剪辑师统计记录")
        return len(rows)

    def upload_optimizer_stats(self, data: List[Dict], table_id: str = "xmp_optimizer_stats", batch_id: str = None) -> int:
        """
        上传投手统计数据到 BigQuery

        Args:
            data: 投手统计数据列表
            table_id: 目标表 ID
            batch_id: 批次 ID

        Returns:
            插入的行数
        """
        self._ensure_xmp_optimizer_stats_table_exists(table_id)

        if not data:
            logger.warning("没有投手数据需要上传")
            return 0

        fetched_at = datetime.utcnow().isoformat()
        rows = []
        for record in data:
            row = {
                'stat_date': record.get('stat_date'),
                'channel': record.get('channel'),
                'optimizer_name': record.get('name') or record.get('optimizer_name'),
                'campaign_count': record.get('campaign_count', 0),
                'spend': record.get('total_cost') or record.get('spend', 0),
                'revenue': record.get('total_revenue') or record.get('revenue', 0),
                'roas': record.get('roas', 0),
                'impressions': record.get('impressions', 0),
                'clicks': record.get('clicks', 0),
                'conversions': record.get('conversions', 0),
                'top_campaign': record.get('top_campaign', ''),
                'top_campaign_spend': record.get('top_campaign_spend', 0),
                'top_campaign_roas': record.get('top_campaign_roas', 0),
                'batch_id': batch_id,
                'fetched_at': fetched_at,
            }
            rows.append(row)

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        errors = self._insert_rows_with_retry(table_ref, rows)

        if errors:
            logger.error(f"BigQuery 插入投手数据错误: {errors}")
            return 0

        logger.info(f"已上传 {len(rows)} 条投手统计记录")
        return len(rows)

    def _normalize_dataeye_data(self, data: List[Dict], batch_id: str = None) -> List[Dict[str, Any]]:
        """
        标准化 DataEye 数据，转换为 BigQuery 行格式

        Args:
            data: DataEye 原始数据列表
            batch_id: 批次 ID

        Returns:
            标准化后的行列表
        """
        rows = []
        fetched_at = datetime.utcnow().isoformat()

        for record in data:
            # 提取国家列表
            countries = record.get('countries') or []
            country_names = [c.get('countryName', '') for c in countries if isinstance(c, dict) and c.get('countryName')]

            # 提取视频和图片 URL
            video_list = record.get('videoList') or []
            pic_list = record.get('picList') or []

            # 提取嵌套对象（安全处理 None）
            media = record.get('media') or {}
            product = record.get('product') or {}
            publisher = record.get('publisher') or {}
            fb_home = record.get('fbHome') or {}

            row = {
                'id': self._safe_int(record.get('id')),
                'material_id': self._safe_int(record.get('materialId')),
                'media_name': self._safe_str(media.get('mediaName') if isinstance(media, dict) else None),
                'countries': json.dumps(country_names, ensure_ascii=False) if country_names else None,
                'product_name': self._safe_str(product.get('productName') if isinstance(product, dict) else None),
                'publisher_name': self._safe_str(publisher.get('publisherName') if isinstance(publisher, dict) else None),
                'playlet_name': self._safe_str(record.get('playletName')),
                'title': self._safe_str(record.get('title1')),
                'description': self._safe_str(record.get('title2')),
                'material_type': self._safe_int(record.get('materialType')),
                'material_width': self._safe_int(record.get('materialWidth')),
                'material_height': self._safe_int(record.get('materialHeight')),
                'first_seen': self._safe_str(record.get('firstSeen')),
                'last_seen': self._safe_str(record.get('lastSeen')),
                'release_days': self._safe_int(record.get('releaseDay')),
                'exposure_num': self._safe_int(record.get('exposureNum')),
                'download_num': self._safe_int(record.get('downloadNum')),
                'heat_num': self._safe_int(record.get('heatNum')),
                'video_url': video_list[0] if video_list else None,
                'pic_url': pic_list[0] if pic_list else None,
                'narration': self._safe_str(record.get('narration')),
                'narration_zh': self._safe_str(record.get('translateNarration')),
                'recognize_lang': self._safe_str(record.get('recognizeLang')),
                'fb_home_url': self._safe_str(fb_home.get('homeUrl')),
                'fb_home_name': self._safe_str(fb_home.get('kw')),
                'batch_id': batch_id,  # 批次 ID，用于区分不同爬取批次
                'fetched_at': fetched_at,
            }
            rows.append(row)

        return rows

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

    # ============ 查询方法（用于 Lark Bot 播报） ============

    def query_daily_stats(self, date: str = None, table_id: str = "xmp_materials") -> Dict[str, Any]:
        """
        查询每日汇总统计数据（用于 Lark Bot 市场监控报告）

        Args:
            date: 查询日期，格式 YYYY-MM-DD，默认为今天
            table_id: 表 ID

        Returns:
            包含汇总数据的字典，可直接传给 LarkBot.send_market_report()
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        query = f"""
        SELECT
            DATE(fetched_at) as date,
            SUM(impression) as impressions,
            SUM(click) as clicks,
            SUM(cost) as cost,
            SAFE_DIVIDE(SUM(click), SUM(impression)) as ctr,
            SAFE_DIVIDE(SUM(cost), SUM(click)) as cpc
        FROM `{table_ref}`
        WHERE DATE(fetched_at) = '{date}'
        GROUP BY DATE(fetched_at)
        """

        try:
            result = self.client.query(query).result()
            for row in result:
                return {
                    "date": str(row.date) if row.date else date,
                    "impressions": int(row.impressions or 0),
                    "clicks": int(row.clicks or 0),
                    "cost": float(row.cost or 0),
                    "ctr": float(row.ctr or 0),
                    "cpc": float(row.cpc or 0),
                }
            # 无数据时返回空结果
            return {
                "date": date,
                "impressions": 0,
                "clicks": 0,
                "cost": 0,
                "ctr": 0,
                "cpc": 0,
            }
        except Exception as e:
            logger.error(f"查询失败: {e}")
            return None

    def query_quickbi_daily_stats(self, date: str = None, table_id: str = "quickbi_campaigns") -> Dict[str, Any]:
        """
        查询 Quick BI 每日汇总统计数据

        Args:
            date: 查询日期，格式 YYYY-MM-DD，默认为今天
            table_id: 表 ID

        Returns:
            包含汇总数据的字典
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        query = f"""
        SELECT
            stat_date as date,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as cost,
            SUM(new_users) as new_users,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(clicks), SUM(impressions)) as ctr,
            SAFE_DIVIDE(SUM(spend), SUM(clicks)) as cpc,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{date}'
        GROUP BY stat_date
        """

        try:
            result = self.client.query(query).result()
            for row in result:
                return {
                    "date": str(row.date) if row.date else date,
                    "impressions": int(row.impressions or 0),
                    "clicks": int(row.clicks or 0),
                    "cost": float(row.cost or 0),
                    "new_users": int(row.new_users or 0),
                    "revenue": float(row.revenue or 0),
                    "ctr": float(row.ctr or 0),
                    "cpc": float(row.cpc or 0),
                    "roas": float(row.roas or 0),
                }
            return {
                "date": date,
                "impressions": 0,
                "clicks": 0,
                "cost": 0,
                "new_users": 0,
                "revenue": 0,
                "ctr": 0,
                "cpc": 0,
                "roas": 0,
            }
        except Exception as e:
            logger.error(f"查询失败: {e}")
            return None

    def query_channel_stats(self, date: str = None, table_id: str = "quickbi_campaigns") -> List[Dict[str, Any]]:
        """
        按渠道查询统计数据

        Args:
            date: 查询日期，格式 YYYY-MM-DD，默认为今天
            table_id: 表 ID

        Returns:
            各渠道统计数据列表
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        query = f"""
        SELECT
            channel,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as cost,
            SUM(new_users) as new_users,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{date}'
        GROUP BY channel
        ORDER BY cost DESC
        """

        try:
            result = self.client.query(query).result()
            channels = []
            for row in result:
                channels.append({
                    "channel": row.channel,
                    "impressions": int(row.impressions or 0),
                    "clicks": int(row.clicks or 0),
                    "cost": float(row.cost or 0),
                    "new_users": int(row.new_users or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0),
                })
            return channels
        except Exception as e:
            logger.error(f"查询失败: {e}")
            return []

    def query_custom(self, sql: str) -> List[Dict[str, Any]]:
        """
        执行自定义 SQL 查询

        Args:
            sql: SQL 查询语句

        Returns:
            查询结果列表
        """
        try:
            result = self.client.query(sql).result()
            rows = []
            for row in result:
                rows.append(dict(row.items()))
            return rows
        except Exception as e:
            logger.error(f"查询失败: {e}")
            return []

    # ============ 日报播报数据查询 ============

    def _get_absolute_latest_batch_id(self, table_ref: str, stat_date: str) -> str:
        """获取指定日期的绝对最新 batch_id（不考虑整点逻辑）"""
        query = f"""
        SELECT MAX(batch_id) as latest_batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{stat_date}'
        """
        try:
            for row in self.client.query(query).result():
                return row.latest_batch_id
        except Exception as e:
            logger.error(f"查询最新 batch_id 失败: {e}")
        return None

    def _get_same_day_batch_id(self, table_ref: str, stat_date: str) -> str:
        """获取指定日期的同日 batch_id（batch_id 日期与 stat_date 相同）"""
        # batch_id 格式: YYYYMMDD_HHMMSS，取前8位作为日期
        date_prefix = stat_date.replace('-', '')
        query = f"""
        SELECT MAX(batch_id) as latest_batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{stat_date}'
          AND batch_id LIKE '{date_prefix}%'
        """
        try:
            for row in self.client.query(query).result():
                return row.latest_batch_id
        except Exception as e:
            logger.error(f"查询同日 batch_id 失败: {e}")
        return None

    def _get_latest_batch_id(self, table_ref: str, stat_date: str) -> str:
        """
        获取指定日期的最新 batch_id

        对于今天的数据：优先取当前小时整点的 batch
        对于历史日期：直接取该日期的最新 batch

        Args:
            table_ref: 表引用
            stat_date: 统计日期 (YYYY-MM-DD)

        Returns:
            最新的 batch_id，如 20251221_230026
        """
        from datetime import datetime

        now = datetime.now()
        today = now.strftime('%Y-%m-%d')

        # 如果是历史日期，取该日期的最新 batch（包括后续日期抓取的数据）
        # 注意：QuickBI 的数据是累计数据，今天抓取的数据 stat_date 仍然是昨天
        # 所以日报应该取最新的 batch，以获取最完整、最准确的数据
        if stat_date != today:
            query = f"""
            SELECT MAX(batch_id) as latest_batch_id
            FROM `{table_ref}`
            WHERE stat_date = '{stat_date}'
            """
            try:
                for row in self.client.query(query).result():
                    if row.latest_batch_id:
                        return row.latest_batch_id
            except Exception as e:
                logger.error(f"查询历史日期 batch_id 失败: {e}")
            return None

        # 今天的数据：优先取整点(00分)的 batch，如果没有就取03分的 batch
        current_hour = now.hour

        # 1. 先查询整点(00分)的 batch
        hour_start_00 = f"{stat_date.replace('-', '')}_{current_hour:02d}0000"
        hour_end_00 = f"{stat_date.replace('-', '')}_{current_hour:02d}0500"  # 00:00-05:00

        query_00 = f"""
        SELECT MAX(batch_id) as latest_batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{stat_date}'
          AND batch_id >= '{hour_start_00}'
          AND batch_id < '{hour_end_00}'
        """

        try:
            for row in self.client.query(query_00).result():
                if row.latest_batch_id:
                    logger.debug(f"找到整点(00分) batch: {row.latest_batch_id}")
                    return row.latest_batch_id
        except Exception as e:
            logger.error(f"查询整点 batch_id 失败: {e}")

        # 2. 整点没有数据，查询03分的 batch
        hour_start_03 = f"{stat_date.replace('-', '')}_{current_hour:02d}0300"
        hour_end_03 = f"{stat_date.replace('-', '')}_{current_hour:02d}0800"  # 03:00-08:00

        query_03 = f"""
        SELECT MAX(batch_id) as latest_batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{stat_date}'
          AND batch_id >= '{hour_start_03}'
          AND batch_id < '{hour_end_03}'
        """

        try:
            for row in self.client.query(query_03).result():
                if row.latest_batch_id:
                    logger.debug(f"找到03分 batch: {row.latest_batch_id}")
                    return row.latest_batch_id
        except Exception as e:
            logger.error(f"查询03分 batch_id 失败: {e}")

        # 当前小时没有数据，查找上一小时的数据（优先整点，其次03分）
        prev_hour = (current_hour - 1) % 24
        # 跨天处理：如果当前是0点，上一小时是昨天23点
        if current_hour == 0:
            from datetime import timedelta
            yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
            prev_stat_date = yesterday
        else:
            prev_stat_date = stat_date

        # 3. 查询上一小时整点(00分)的 batch
        prev_hour_start_00 = f"{prev_stat_date.replace('-', '')}_{prev_hour:02d}0000"
        prev_hour_end_00 = f"{prev_stat_date.replace('-', '')}_{prev_hour:02d}0100"

        query_prev_00 = f"""
        SELECT MAX(batch_id) as latest_batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{prev_stat_date}'
          AND batch_id >= '{prev_hour_start_00}'
          AND batch_id < '{prev_hour_end_00}'
        """

        try:
            for row in self.client.query(query_prev_00).result():
                if row.latest_batch_id:
                    logger.debug(f"找到上一小时整点 batch: {row.latest_batch_id}")
                    return row.latest_batch_id
        except Exception as e:
            logger.error(f"查询上一小时整点 batch_id 失败: {e}")

        # 4. 查询上一小时03分的 batch
        prev_hour_start_03 = f"{prev_stat_date.replace('-', '')}_{prev_hour:02d}0300"
        prev_hour_end_03 = f"{prev_stat_date.replace('-', '')}_{prev_hour:02d}0400"

        query_prev_03 = f"""
        SELECT MAX(batch_id) as latest_batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{prev_stat_date}'
          AND batch_id >= '{prev_hour_start_03}'
          AND batch_id < '{prev_hour_end_03}'
        """

        try:
            for row in self.client.query(query_prev_03).result():
                if row.latest_batch_id:
                    logger.debug(f"找到上一小时03分 batch: {row.latest_batch_id}")
                    return row.latest_batch_id
        except Exception as e:
            logger.error(f"查询上一小时03分 batch_id 失败: {e}")

        # 5. 回退：直接取当日最新的 batch_id（支持 XMP 内部 API 等非整点数据源）
        query_fallback = f"""
        SELECT MAX(batch_id) as latest_batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{stat_date}'
        """

        try:
            for row in self.client.query(query_fallback).result():
                if row.latest_batch_id:
                    logger.debug(f"回退：找到当日最新 batch: {row.latest_batch_id}")
                    return row.latest_batch_id
        except Exception as e:
            logger.error(f"查询当日最新 batch_id 失败: {e}")

        # 都没有数据，返回 None
        logger.warning("未找到整点或03分 batch 数据")
        return None

    def query_daily_report_data(self, date: str = None, table_id: str = None, dataset_id: str = None, use_latest_batch: bool = False) -> Dict[str, Any]:
        """
        查询日报播报所需的全部数据 (T-1 日) - 单表模式

        目标：为管理层提供昨天的全盘复盘，辅助战略决策
        - 触发时间：每日 09:00
        - 取数范围：stat_date = Yesterday (T-1) 的最新批次数据

        Args:
            date: 查询日期，格式 YYYY-MM-DD，默认为昨天 (T-1)
            table_id: 表 ID，默认为 quickbi_campaigns
            dataset_id: 数据集 ID，默认为 quickbi_data
            use_latest_batch: 是否强制使用最新 batch（而非整点 batch）

        Returns:
            包含日报所需全部数据的字典
        """
        from datetime import datetime, timedelta
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        # 默认查询昨天的数据 (T-1)
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"查询日期 (T-1): {date}")

        # 计算前天日期 (T-2) 用于环比
        query_date = datetime.strptime(date, '%Y-%m-%d')
        day_before = (query_date - timedelta(days=1)).strftime('%Y-%m-%d')

        # 表引用
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 获取 T-1 和 T-2 的 batch_id
        if use_latest_batch:
            # 强制使用最新 batch
            batch_id = self._get_absolute_latest_batch_id(table_ref, date)
            batch_id_prev = self._get_absolute_latest_batch_id(table_ref, day_before)
        else:
            # 默认使用整点 batch
            batch_id = self._get_latest_batch_id(table_ref, date)
            batch_id_prev = self._get_latest_batch_id(table_ref, day_before)

        if batch_id:
            logger.debug(f"T-1 最新批次: {batch_id}")
        else:
            logger.warning(f"未找到 {date} 的数据")

        if batch_id_prev:
            logger.debug(f"T-2 最新批次: {batch_id_prev}")
        else:
            logger.warning(f"未找到 {day_before} 的数据，环比可能为空")

        result = {
            "date": date,
            "summary": {},
            "summary_prev": {},
            "optimizers": [],
            "dramas_top5": [],
            "countries_top5": [],
            "scale_up_dramas": [],
            "opportunity_markets": [],
            "top_dramas_detail": [],  # 头部剧集综合榜
            "channel_benchmark": {}  # 分渠道数据
        }

        # 构建 batch_id 过滤条件
        batch_filter = f"AND batch_id = '{batch_id}'" if batch_id else ""
        batch_filter_prev = f"AND batch_id = '{batch_id_prev}'" if batch_id_prev else ""

        # 1. 大盘总览 (T-1) - 包含平台总营收和收支比
        summary_query = f"""
        SELECT
            SUM(spend) as total_spend,
            SUM(media_user_revenue) as total_revenue,
            SUM(COALESCE(media_iap_revenue, 0) + COALESCE(media_sub_revenue, 0) + COALESCE(media_ad_revenue, 0)) as platform_total_revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
        """

        # 2. 大盘总览 (T-2) 用于环比 - 包含平台总营收
        summary_prev_query = f"""
        SELECT
            SUM(spend) as total_spend,
            SUM(media_user_revenue) as total_revenue,
            SUM(COALESCE(media_iap_revenue, 0) + COALESCE(media_sub_revenue, 0) + COALESCE(media_ad_revenue, 0)) as platform_total_revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas
        FROM `{table_ref}`
        WHERE stat_date = '{day_before}' {batch_filter_prev}
        """

        # 3. 投手排行榜
        optimizer_query = f"""
        SELECT
            optimizer,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas,
            COUNT(DISTINCT campaign_id) as campaign_count,
            MAX(campaign_name) as top_campaign
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
          AND optimizer IS NOT NULL
          AND optimizer != ''
        GROUP BY optimizer
        ORDER BY spend DESC
        """

        # 4. 剧集 Top 5
        drama_query = f"""
        SELECT
            drama_name,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
          AND drama_name IS NOT NULL
          AND drama_name != ''
        GROUP BY drama_name
        ORDER BY spend DESC
        LIMIT 5
        """

        # 5. 国家 Top 5
        country_query = f"""
        SELECT
            country,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
          AND country IS NOT NULL
          AND country != ''
        GROUP BY country
        ORDER BY spend DESC
        LIMIT 5
        """

        # 6. 放量剧目 (Spend > $1000 且 ROAS > 45%)
        scale_up_query = f"""
        SELECT drama_name, spend, roas FROM (
            SELECT
                drama_name,
                SUM(spend) as spend,
                SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
            FROM `{table_ref}`
            WHERE stat_date = '{date}' {batch_filter}
              AND drama_name IS NOT NULL
            GROUP BY drama_name
        )
        WHERE spend > 1000 AND roas > 0.45
        ORDER BY roas DESC
        LIMIT 3
        """

        # 7. 机会市场 (Spend > $100 且 ROAS > 50% 且不在主投Top3国家)
        opportunity_query = f"""
        SELECT drama_name, country, spend, roas FROM (
            SELECT
                drama_name,
                country,
                SUM(spend) as spend,
                SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
            FROM `{table_ref}`
            WHERE stat_date = '{date}' {batch_filter}
              AND country NOT IN ('US', 'KR', 'JP')
              AND drama_name IS NOT NULL
              AND country IS NOT NULL
            GROUP BY drama_name, country
        )
        WHERE spend > 100 AND roas > 0.50
        ORDER BY roas DESC
        LIMIT 3
        """

        # 8. 头部剧集综合榜 (Top 5 剧集 + 主跑语区 Top2 + Top 国家 Top3)
        top_dramas_query = f"""
        SELECT
            drama_name,
            SUM(spend) as spend,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
          AND drama_name IS NOT NULL
          AND drama_name != ''
        GROUP BY drama_name
        ORDER BY spend DESC
        LIMIT 5
        """

        # 8.1 头部剧集的语区分布 (用于获取主跑语区)
        drama_language_query = f"""
        SELECT
            drama_name,
            CASE
                WHEN country IN ('US', 'GB', 'AU', 'CA', 'NZ', 'IE') THEN 'EN'
                WHEN country IN ('ES', 'MX', 'AR', 'CO', 'PE', 'CL') THEN 'ES'
                WHEN country IN ('KR') THEN 'KR'
                WHEN country IN ('JP') THEN 'JP'
                WHEN country IN ('TW', 'HK', 'MO') THEN 'ZH-TW'
                WHEN country IN ('DE', 'AT', 'CH') THEN 'DE'
                WHEN country IN ('FR', 'BE') THEN 'FR'
                WHEN country IN ('PT', 'BR') THEN 'PT'
                WHEN country IN ('TH') THEN 'TH'
                WHEN country IN ('ID') THEN 'ID'
                WHEN country IN ('VN') THEN 'VN'
                ELSE 'OTHER'
            END as language,
            SUM(spend) as spend
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
          AND drama_name IS NOT NULL
          AND country IS NOT NULL
        GROUP BY drama_name, language
        ORDER BY drama_name, spend DESC
        """

        # 8.2 头部剧集的国家分布 (用于获取 Top 国家)
        drama_country_query = f"""
        SELECT
            drama_name,
            country,
            SUM(spend) as spend
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
          AND drama_name IS NOT NULL
          AND country IS NOT NULL
        GROUP BY drama_name, country
        ORDER BY drama_name, spend DESC
        """

        try:
            # 执行查询
            # 1. 大盘总览
            for row in self.client.query(summary_query).result():
                total_spend = float(row.total_spend or 0)
                platform_revenue = float(row.platform_total_revenue or 0)
                # 收支比 = 平台总营收 / 总消耗
                revenue_spend_ratio = platform_revenue / total_spend if total_spend > 0 else 0
                result["summary"] = {
                    "total_spend": total_spend,
                    "total_revenue": float(row.total_revenue or 0),
                    "platform_total_revenue": platform_revenue,
                    "revenue_spend_ratio": revenue_spend_ratio,
                    "media_roas": float(row.media_roas or 0),
                    "global_roas": float(row.media_roas or 0)  # 兼容旧字段名
                }

            # 2. 前一天数据
            for row in self.client.query(summary_prev_query).result():
                prev_spend = float(row.total_spend or 0)
                prev_platform_revenue = float(row.platform_total_revenue or 0)
                prev_ratio = prev_platform_revenue / prev_spend if prev_spend > 0 else 0
                result["summary_prev"] = {
                    "total_spend": prev_spend,
                    "total_revenue": float(row.total_revenue or 0),
                    "platform_total_revenue": prev_platform_revenue,
                    "revenue_spend_ratio": prev_ratio,
                    "media_roas": float(row.media_roas or 0),
                    "global_roas": float(row.media_roas or 0)  # 兼容旧字段名
                }

            # 3. 投手排行
            for row in self.client.query(optimizer_query).result():
                result["optimizers"].append({
                    "name": row.optimizer,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0),
                    "campaign_count": int(row.campaign_count or 0),
                    "top_campaign": row.top_campaign
                })

            # 4. 剧集 Top 5
            for row in self.client.query(drama_query).result():
                result["dramas_top5"].append({
                    "name": row.drama_name,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })

            # 5. 国家 Top 5
            for row in self.client.query(country_query).result():
                result["countries_top5"].append({
                    "name": row.country,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })

            # 6. 放量剧目
            for row in self.client.query(scale_up_query).result():
                result["scale_up_dramas"].append({
                    "name": row.drama_name,
                    "spend": float(row.spend or 0),
                    "roas": float(row.roas or 0)
                })

            # 7. 机会市场
            for row in self.client.query(opportunity_query).result():
                result["opportunity_markets"].append({
                    "drama_name": row.drama_name,
                    "country": row.country,
                    "spend": float(row.spend or 0),
                    "roas": float(row.roas or 0)
                })

            # 8. 头部剧集综合榜
            top_dramas = []
            for row in self.client.query(top_dramas_query).result():
                top_dramas.append({
                    "name": row.drama_name,
                    "spend": float(row.spend or 0),
                    "roas": float(row.roas or 0),
                    "top_languages": [],
                    "top_countries": []
                })

            # 8.1 获取每个剧集的语区分布
            drama_languages = {}
            for row in self.client.query(drama_language_query).result():
                drama = row.drama_name
                if drama not in drama_languages:
                    drama_languages[drama] = []
                if len(drama_languages[drama]) < 2:  # Top 2 语区
                    drama_languages[drama].append({
                        "language": row.language,
                        "spend": float(row.spend or 0)
                    })

            # 8.2 获取每个剧集的国家分布
            drama_countries = {}
            for row in self.client.query(drama_country_query).result():
                drama = row.drama_name
                if drama not in drama_countries:
                    drama_countries[drama] = []
                if len(drama_countries[drama]) < 3:  # Top 3 国家
                    drama_countries[drama].append({
                        "country": row.country,
                        "spend": float(row.spend or 0)
                    })

            # 8.3 合并数据
            for drama in top_dramas:
                name = drama["name"]
                drama["top_languages"] = drama_languages.get(name, [])
                drama["top_countries"] = drama_countries.get(name, [])

            result["top_dramas_detail"] = top_dramas

            # 9. 从 quickbi_overview 表获取真实的 platform_total_revenue 和 total_spend
            # 注意：overview 表固定在 quickbi_data 数据集中
            overview_table = f"{self.project_id}.quickbi_data.quickbi_overview"

            # 9.1 获取 T-1 日的 overview 数据 (revenue + spend)
            overview_query = f"""
            SELECT total_revenue, total_spend FROM `{overview_table}`
            WHERE stat_date = '{date}'
            ORDER BY batch_id DESC
            LIMIT 1
            """
            try:
                for row in self.client.query(overview_query).result():
                    real_total_revenue = float(row.total_revenue or 0)
                    real_total_spend = float(row.total_spend or 0) if hasattr(row, 'total_spend') else 0
                    if real_total_revenue > 0:
                        result["summary"]["platform_total_revenue"] = real_total_revenue
                    if real_total_spend > 0:
                        result["summary"]["total_spend"] = real_total_spend
                    # 重新计算收支比
                    final_spend = result["summary"].get("total_spend", 0)
                    if final_spend > 0 and real_total_revenue > 0:
                        result["summary"]["revenue_spend_ratio"] = real_total_revenue / final_spend
            except Exception as e:
                logger.warning(f"查询日报 overview 表失败，使用计算值: {e}")

            # 9.2 获取 T-2 日的 overview 数据 (用于环比)
            overview_prev_query = f"""
            SELECT total_revenue, total_spend FROM `{overview_table}`
            WHERE stat_date = '{day_before}'
            ORDER BY batch_id DESC
            LIMIT 1
            """
            try:
                for row in self.client.query(overview_prev_query).result():
                    prev_total_revenue = float(row.total_revenue or 0)
                    prev_total_spend = float(row.total_spend or 0) if hasattr(row, 'total_spend') else 0
                    if prev_total_revenue > 0:
                        result["summary_prev"]["platform_total_revenue"] = prev_total_revenue
                    if prev_total_spend > 0:
                        result["summary_prev"]["total_spend"] = prev_total_spend
                    # 重新计算收支比
                    final_prev_spend = result["summary_prev"].get("total_spend", 0)
                    if final_prev_spend > 0 and prev_total_revenue > 0:
                        result["summary_prev"]["revenue_spend_ratio"] = prev_total_revenue / final_prev_spend
            except Exception as e:
                logger.warning(f"查询日报 T-2 overview 表失败，使用计算值: {e}")

            # 10. 分渠道数据 (TikTok / Meta)
            channel_query = f"""
            SELECT
                channel,
                SUM(spend) as spend,
                SUM(media_user_revenue) as revenue,
                SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
            FROM `{table_ref}`
            WHERE stat_date = '{date}' {batch_filter}
              AND channel IS NOT NULL
              AND channel != ''
            GROUP BY channel
            """
            for row in self.client.query(channel_query).result():
                channel = row.channel
                result["channel_benchmark"][channel] = {
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                }

            return result

        except Exception as e:
            logger.error(f"查询日报数据失败: {e}")
            return result


    # ============ 实时播报数据查询 (小时级) ============

    def query_realtime_report_data(self, table_id: str = None, dataset_id: str = None, date: str = None, use_latest_batch: bool = False, use_same_day_batch: bool = False) -> Dict[str, Any]:
        """
        查询实时播报所需的全部数据 (当日累计)

        目标：为执行层提供"每小时"的监控，发现异动，即时调整
        - 触发时间：每日 9:00 - 24:00，每整点触发
        - 取数范围：当日累计数据 (stat_date = Today)

        Args:
            use_latest_batch: 是否使用绝对最新 batch（默认 False，使用整点 batch）
            use_same_day_batch: 是否使用同日 batch（batch_id 日期与 stat_date 相同）

        Returns:
            包含实时播报所需全部数据的字典
        """
        from datetime import datetime
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        today = date if date else datetime.now().strftime('%Y-%m-%d')
        current_hour = datetime.now().strftime('%H:%M')

        # 表引用
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 获取当日最新 batch_id
        if use_same_day_batch:
            batch_id = self._get_same_day_batch_id(table_ref, today)
        elif use_latest_batch:
            batch_id = self._get_absolute_latest_batch_id(table_ref, today)
        else:
            batch_id = self._get_latest_batch_id(table_ref, today)

        result = {
            "date": today,
            "current_hour": current_hour,
            "batch_id": batch_id,
            "api_update_time": None,
            "data_delayed": False,
            "summary": {},
            "yesterday_summary": {},  # 前一日同时刻数据，用于日环比
            "prev_hour_summary": {},  # 上一小时数据，用于小时环比
            "optimizer_spend": [],
            "stop_loss_campaigns": [],
            "scale_up_campaigns": [],
            "country_marginal_roas": [],
            "region_opportunity_radar": [],  # 地区机会雷达
            "channel_benchmark": {}  # 分渠道大盘 ROAS
        }

        if not batch_id:
            logger.warning(f"未找到 {today} 的数据")
            return result

        # 解析 batch_id 获取 API 更新时间 (格式: 20251222_103000)
        try:
            batch_time_str = batch_id.replace('_', '')
            api_update_time = datetime.strptime(batch_time_str, '%Y%m%d%H%M%S')
            result["api_update_time"] = api_update_time.strftime('%Y-%m-%d %H:%M:%S')
            result["batch_time"] = api_update_time.strftime('%H:%M')  # 当前batch时间点

            # 检查数据延迟 - 基于当前小时是否有数据
            now = datetime.now()
            current_hour = now.hour
            batch_hour = api_update_time.hour
            current_minute = now.minute

            # 判断延迟逻辑：
            # 1. 如果 batch 小时 == 当前小时 → 正常
            # 2. 如果 batch 小时 == 当前小时-1 且 当前分钟 < 5 → 正常（还在等同步）
            # 3. 其他情况 → 延迟
            if batch_hour == current_hour:
                # 当前小时有数据，正常
                pass
            elif batch_hour == (current_hour - 1) % 24 and current_minute < 5:
                # 上一小时数据，但当前分钟<5，还在等同步，正常
                pass
            else:
                # 延迟
                result["data_delayed"] = True
        except Exception:
            pass

        batch_filter = f"AND batch_id = '{batch_id}'"

        # 1. 大盘总览 (当日累计)
        # 平台总营收 = IAP收入 + 订阅收入 + 广告变现收入
        summary_query = f"""
        SELECT
            SUM(spend) as total_spend,
            SUM(new_user_revenue) as total_revenue,
            SUM(media_user_revenue) as total_media_revenue,
            SUM(COALESCE(media_iap_revenue, 0) + COALESCE(media_sub_revenue, 0) + COALESCE(media_ad_revenue, 0)) as platform_total_revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
        """

        # 2. 分投手消耗 (用于归因分析) - 简化查询，避免 STRUCT 问题
        optimizer_query = f"""
        SELECT
            optimizer,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
          AND optimizer IS NOT NULL
          AND optimizer != ''
        GROUP BY optimizer
        ORDER BY spend DESC
        """

        # 2.1 分投手 Top Campaign 查询
        optimizer_top_campaigns_query = f"""
        SELECT
            optimizer,
            campaign_name,
            drama_name,
            country,
            SUM(spend) as spend
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
          AND optimizer IS NOT NULL
          AND optimizer != ''
        GROUP BY optimizer, campaign_name, drama_name, country
        ORDER BY optimizer, spend DESC
        """

        # 2.2 分投手分渠道消耗查询
        optimizer_channel_query = f"""
        SELECT
            optimizer,
            channel,
            SUM(spend) as spend,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
          AND optimizer IS NOT NULL
          AND optimizer != ''
          AND channel IS NOT NULL
        GROUP BY optimizer, channel
        ORDER BY optimizer, spend DESC
        """

        # 3. 止损预警 (Spend > $300 且 ROAS < 30%) - 按 campaign 整体聚合
        stop_loss_query = f"""
        SELECT
            campaign_id,
            campaign_name,
            optimizer,
            drama_name,
            channel,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
        GROUP BY campaign_id, campaign_name, optimizer, drama_name, channel
        HAVING spend > 300 AND (revenue = 0 OR SAFE_DIVIDE(revenue, spend) < 0.30)
        ORDER BY spend DESC
        LIMIT 10
        """

        # 3.1 止损计划的分地区明细 (用于展示花费占比和分地区 ROAS)
        stop_loss_country_detail_query = f"""
        SELECT
            campaign_id,
            country,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
        GROUP BY campaign_id, country
        """

        # 4. 扩量机会 (Spend > $300 且 ROAS > 50%) - 按 campaign 整体聚合
        scale_up_query = f"""
        SELECT
            campaign_id,
            campaign_name,
            optimizer,
            drama_name,
            channel,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
        GROUP BY campaign_id, campaign_name, optimizer, drama_name, channel
        HAVING spend > 300 AND SAFE_DIVIDE(revenue, spend) > 0.50
        ORDER BY roas DESC
        LIMIT 10
        """

        # 5. 分国家边际 ROAS (用于地区观察)
        country_query = f"""
        SELECT
            country,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
          AND country IS NOT NULL
          AND country != ''
        GROUP BY country
        HAVING spend > 100
        ORDER BY roas DESC
        """

        # 5.1 分渠道大盘 ROAS (用于对比个人 vs 大盘)
        channel_benchmark_query = f"""
        SELECT
            channel,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
          AND channel IS NOT NULL
        GROUP BY channel
        """

        # 5.1.1 分地区分渠道消耗查询
        country_channel_query = f"""
        SELECT
            country,
            channel,
            SUM(spend) as spend,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
          AND country IS NOT NULL
          AND country != ''
          AND channel IS NOT NULL
        GROUP BY country, channel
        ORDER BY country, spend DESC
        """

        # 5.2 地区机会雷达 - 高 ROAS 国家的核心驱动剧集
        country_drama_query = f"""
        SELECT
            country,
            drama_name,
            SUM(spend) as spend,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
          AND country IS NOT NULL
          AND drama_name IS NOT NULL
        GROUP BY country, drama_name
        ORDER BY country, spend DESC
        """

        # 6. 前一日同时刻数据 (用于日环比)
        # 目标：找到昨天当前整点的 batch，用于日环比对比
        # 例如：现在 12:30，查找昨天 12:00-12:10 范围内的 batch
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        now = datetime.now()
        current_hour_int = now.hour

        # 查询昨天当前整点的 batch_id（整点前后10分钟范围）
        yesterday_date_str = yesterday.replace('-', '')
        yesterday_hour_start = f"{yesterday_date_str}_{current_hour_int:02d}0000"
        yesterday_hour_end = f"{yesterday_date_str}_{current_hour_int:02d}1000"

        yesterday_batch_query = f"""
        SELECT batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{yesterday}'
          AND batch_id >= '{yesterday_hour_start}'
          AND batch_id <= '{yesterday_hour_end}'
        ORDER BY batch_id DESC
        LIMIT 1
        """

        yesterday_summary_query = None
        try:
            yesterday_batch_id = None
            for row in self.client.query(yesterday_batch_query).result():
                yesterday_batch_id = row.batch_id

            if yesterday_batch_id:
                yesterday_summary_query = f"""
                SELECT
                    SUM(spend) as total_spend,
                    SUM(new_user_revenue) as total_revenue,
                    SUM(media_user_revenue) as total_media_revenue,
                    SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas
                FROM `{table_ref}`
                WHERE stat_date = '{yesterday}' AND batch_id = '{yesterday_batch_id}'
                """
        except Exception as e:
            logger.error(f"查询前一日 batch_id 失败: {e}")

        # 7. 上一小时数据 (用于小时环比) - 查找当前整点和上一整点的数据
        # 目标：计算当前整点到上一整点的差值（如 14:00 - 13:00）
        prev_hour_batch_query = None
        try:
            from datetime import datetime as dt
            now = dt.now()
            current_hour = now.hour
            current_date = today
            
            # 上一小时整点（支持跨天）
            prev_hour = (current_hour - 1) % 24
            if current_hour == 0:
                from datetime import timedelta
                prev_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                prev_date = current_date
            
            # 查询范围：整点前后15分钟（00:00-15:00）
            prev_hour_start = f"{prev_date.replace('-', '')}_{prev_hour:02d}0000"
            prev_hour_end = f"{prev_date.replace('-', '')}_{prev_hour:02d}1000"
            prev_hour_stat_date = prev_date


            prev_hour_batch_query = f"""
            SELECT batch_id
            FROM `{table_ref}`
            WHERE stat_date = '{prev_hour_stat_date}'
              AND batch_id >= '{prev_hour_start}'
              AND batch_id <= '{prev_hour_end}'
            ORDER BY batch_id DESC
            LIMIT 1
            """
        except Exception as e:
            logger.error(f"解析 batch_id 时间失败: {e}")

        prev_hour_summary_query = None
        prev_hour_optimizer_query = None
        if prev_hour_batch_query:
            try:
                prev_batch_id = None
                for row in self.client.query(prev_hour_batch_query).result():
                    prev_batch_id = row.batch_id

                if prev_batch_id:
                    result["prev_batch_id"] = prev_batch_id
                    # 解析 prev_batch_id 获取时间
                    try:
                        prev_batch_time_str = prev_batch_id.replace('_', '')
                        prev_batch_time = datetime.strptime(prev_batch_time_str, '%Y%m%d%H%M%S')
                        result["prev_batch_time"] = prev_batch_time.strftime('%H:%M')
                    except:
                        pass

                    prev_hour_summary_query = f"""
                    SELECT
                        SUM(spend) as total_spend,
                        SUM(new_user_revenue) as total_revenue,
                        SUM(media_user_revenue) as total_media_revenue,
                        SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas
                    FROM `{table_ref}`
                    WHERE stat_date = '{today}' AND batch_id = '{prev_batch_id}'
                    """

                    prev_hour_optimizer_query = f"""
                    SELECT
                        optimizer,
                        SUM(spend) as spend
                    FROM `{table_ref}`
                    WHERE stat_date = '{today}' AND batch_id = '{prev_batch_id}'
                      AND optimizer IS NOT NULL AND optimizer != ''
                    GROUP BY optimizer
                    """
            except Exception as e:
                logger.error(f"查询上一小时 batch_id 失败: {e}")

        try:
            # 执行查询
            # 1. 大盘总览
            for row in self.client.query(summary_query).result():
                total_spend = float(row.total_spend or 0)
                platform_revenue = float(row.platform_total_revenue or 0)
                # 收支比 = 平台总营收 / 总消耗
                revenue_spend_ratio = platform_revenue / total_spend if total_spend > 0 else 0

                result["summary"] = {
                    "total_spend": total_spend,
                    "total_revenue": float(row.total_revenue or 0),
                    "total_media_revenue": float(row.total_media_revenue or 0),
                    "platform_total_revenue": platform_revenue,
                    "revenue_spend_ratio": revenue_spend_ratio,
                    "media_roas": float(row.media_roas or 0)
                }

            # 1.1 从 quickbi_overview 表获取真实的 total_revenue
            # 注意：overview 表固定在 quickbi_data 数据集中
            overview_table = f"{self.project_id}.quickbi_data.quickbi_overview"
            overview_query = f"""
            SELECT total_revenue FROM `{overview_table}`
            WHERE stat_date = '{today}'
            ORDER BY batch_id DESC
            LIMIT 1
            """
            try:
                for row in self.client.query(overview_query).result():
                    real_total_revenue = float(row.total_revenue or 0)
                    if real_total_revenue > 0:
                        result["summary"]["platform_total_revenue"] = real_total_revenue
                        result["summary"]["revenue_spend_ratio"] = real_total_revenue / total_spend if total_spend > 0 else 0
            except Exception as e:
                logger.warning(f"查询 overview 表失败，使用计算值: {e}")

            # 2. 分投手消耗
            for row in self.client.query(optimizer_query).result():
                result["optimizer_spend"].append({
                    "optimizer": row.optimizer,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0),
                    "top_campaigns": []  # 稍后填充
                })

            # 2.1 获取每个投手的 Top Campaign
            optimizer_campaigns = {}
            for row in self.client.query(optimizer_top_campaigns_query).result():
                opt = row.optimizer
                if opt not in optimizer_campaigns:
                    optimizer_campaigns[opt] = []
                if len(optimizer_campaigns[opt]) < 3:  # 只取 Top 3
                    optimizer_campaigns[opt].append({
                        "name": row.campaign_name,
                        "drama_name": row.drama_name,
                        "country": row.country,
                        "spend": float(row.spend or 0)
                    })

            # 填充 top_campaigns
            for opt_data in result["optimizer_spend"]:
                opt_name = opt_data["optimizer"]
                if opt_name in optimizer_campaigns:
                    opt_data["top_campaigns"] = optimizer_campaigns[opt_name]

            # 2.2 获取每个投手的分渠道消耗
            optimizer_channels = {}
            for row in self.client.query(optimizer_channel_query).result():
                opt = row.optimizer
                if opt not in optimizer_channels:
                    optimizer_channels[opt] = {}
                optimizer_channels[opt][row.channel] = {
                    "spend": float(row.spend or 0),
                    "roas": float(row.roas or 0)
                }

            # 填充 channel_spend
            for opt_data in result["optimizer_spend"]:
                opt_name = opt_data["optimizer"]
                opt_data["channel_spend"] = optimizer_channels.get(opt_name, {})

            # 3. 止损预警
            for row in self.client.query(stop_loss_query).result():
                result["stop_loss_campaigns"].append({
                    "campaign_id": row.campaign_id,
                    "campaign_name": row.campaign_name,
                    "optimizer": row.optimizer,
                    "drama_name": row.drama_name,
                    "channel": row.channel,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0),
                    "country_details": []  # 稍后填充
                })

            # 4. 扩量机会
            for row in self.client.query(scale_up_query).result():
                result["scale_up_campaigns"].append({
                    "campaign_id": row.campaign_id,
                    "campaign_name": row.campaign_name,
                    "optimizer": row.optimizer,
                    "drama_name": row.drama_name,
                    "channel": row.channel,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0),
                    "country_details": []  # 稍后填充
                })

            # 4.1 获取止损/扩量计划的分地区明细
            campaign_country_details = {}
            for row in self.client.query(stop_loss_country_detail_query).result():
                cid = row.campaign_id
                if cid not in campaign_country_details:
                    campaign_country_details[cid] = []
                campaign_country_details[cid].append({
                    "country": row.country,
                    "spend": float(row.spend or 0),
                    "roas": float(row.roas or 0)
                })

            # 填充分地区明细到止损/扩量计划
            for campaign in result["stop_loss_campaigns"]:
                cid = campaign["campaign_id"]
                if cid in campaign_country_details:
                    details = sorted(campaign_country_details[cid], key=lambda x: x["spend"], reverse=True)[:5]
                    total_spend = campaign["spend"]
                    for d in details:
                        d["spend_ratio"] = d["spend"] / total_spend if total_spend > 0 else 0
                    campaign["country_details"] = details

            for campaign in result["scale_up_campaigns"]:
                cid = campaign["campaign_id"]
                if cid in campaign_country_details:
                    details = sorted(campaign_country_details[cid], key=lambda x: x["spend"], reverse=True)[:5]
                    total_spend = campaign["spend"]
                    for d in details:
                        d["spend_ratio"] = d["spend"] / total_spend if total_spend > 0 else 0
                    campaign["country_details"] = details


            # 5. 分国家边际 ROAS
            for row in self.client.query(country_query).result():
                result["country_marginal_roas"].append({
                    "country": row.country,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })

            # 5.1.1 获取分地区分渠道消耗
            country_channels = {}
            for row in self.client.query(country_channel_query).result():
                country = row.country
                if country not in country_channels:
                    country_channels[country] = {}
                country_channels[country][row.channel] = {
                    "spend": float(row.spend or 0),
                    "roas": float(row.roas or 0)
                }

            # 填充 channel_spend 到地区数据
            for country_data in result["country_marginal_roas"]:
                country_name = country_data["country"]
                country_data["channel_spend"] = country_channels.get(country_name, {})

            # 5.1 分渠道大盘 ROAS
            for row in self.client.query(channel_benchmark_query).result():
                channel = row.channel
                result["channel_benchmark"][channel] = {
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                }

            # 5.2 地区机会雷达 - 获取每个国家的核心驱动剧集
            country_dramas = {}
            for row in self.client.query(country_drama_query).result():
                country = row.country
                if country not in country_dramas:
                    country_dramas[country] = []
                if len(country_dramas[country]) < 1:  # 只取消耗最高的剧集
                    country_dramas[country].append({
                        "drama_name": row.drama_name,
                        "spend": float(row.spend or 0),
                        "roas": float(row.roas or 0)
                    })

            # 筛选高 ROAS 国家 (ROAS > 50%) 作为机会雷达
            for country_data in result["country_marginal_roas"]:
                country = country_data["country"]
                roas = country_data["roas"]
                spend = country_data["spend"]
                if roas > 0.50 and spend > 100:
                    top_drama = country_dramas.get(country, [{}])[0]
                    drama_name = top_drama.get("drama_name", "")
                    drama_spend = top_drama.get("spend", 0)
                    # 计算该剧在该国的消耗占比
                    drama_ratio = drama_spend / spend if spend > 0 else 0
                    result["region_opportunity_radar"].append({
                        "country": country,
                        "roas": roas,
                        "spend": spend,
                        "core_drama": drama_name,
                        "drama_spend_ratio": drama_ratio
                    })

            # 按 ROAS 排序，取 Top 3
            result["region_opportunity_radar"].sort(key=lambda x: x["roas"], reverse=True)
            result["region_opportunity_radar"] = result["region_opportunity_radar"][:3]

            # 6. 前一日同时刻数据
            if yesterday_summary_query:
                for row in self.client.query(yesterday_summary_query).result():
                    result["yesterday_summary"] = {
                        "total_spend": float(row.total_spend or 0),
                        "total_revenue": float(row.total_revenue or 0),
                        "total_media_revenue": float(row.total_media_revenue or 0),
                        "media_roas": float(row.media_roas or 0)
                    }

            # 7. 上一小时数据
            if prev_hour_summary_query:
                for row in self.client.query(prev_hour_summary_query).result():
                    result["prev_hour_summary"] = {
                        "total_spend": float(row.total_spend or 0),
                        "total_revenue": float(row.total_revenue or 0),
                        "total_media_revenue": float(row.total_media_revenue or 0),
                        "media_roas": float(row.media_roas or 0)
                    }

            # 7.1 上一小时分投手数据
            if prev_hour_optimizer_query:
                optimizer_data = []
                for row in self.client.query(prev_hour_optimizer_query).result():
                    optimizer_data.append({
                        "optimizer": row.optimizer,
                        "spend": float(row.spend or 0)
                    })
                result["prev_hour_summary"]["optimizer_data"] = optimizer_data

            return result

        except Exception as e:
            logger.error(f"查询实时播报数据失败: {e}")
            return result

    def get_previous_batch_data(self, table_id: str = None, dataset_id: str = None) -> Dict[str, Any]:
        """
        获取上一个整点的 batch 数据（用于计算小时环比）

        逻辑：只查询上一小时整点后 0-5 分钟的批次
        例如：当前 10:40，查询 09:00-09:05 的批次

        Returns:
            上一个整点 batch 的汇总数据，包含 total_spend, d0_roas, optimizer_data
        """
        from datetime import datetime
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        today = datetime.now().strftime('%Y-%m-%d')
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 获取当前小时
        now = datetime.now()
        current_hour = now.hour

        # 计算上一小时
        prev_hour = (current_hour - 1) % 24

        # 跨天处理
        if current_hour == 0:
            from datetime import timedelta
            yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
            prev_stat_date = yesterday
        else:
            prev_stat_date = today

        # 查询上一小时整点后 0-5 分钟的批次
        prev_hour_start = f"{prev_stat_date.replace('-', '')}_{prev_hour:02d}0000"
        prev_hour_end = f"{prev_stat_date.replace('-', '')}_{prev_hour:02d}0500"

        batch_query = f"""
        SELECT MAX(batch_id) as latest_batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{prev_stat_date}'
          AND batch_id >= '{prev_hour_start}'
          AND batch_id <= '{prev_hour_end}'
        """

        try:
            prev_batch_id = None
            for row in self.client.query(batch_query).result():
                if row.latest_batch_id:
                    prev_batch_id = row.latest_batch_id
                    break

            if not prev_batch_id:
                logger.debug(f"未找到上一小时 ({prev_hour:02d}:00-{prev_hour:02d}:05) 的批次")
                return None

            logger.debug(f"使用上一个batch: {prev_batch_id}")

            # 解析 batch_id 获取时间点 (格式: 20251224_080035)
            batch_time_str = None
            try:
                batch_time = datetime.strptime(prev_batch_id, '%Y%m%d_%H%M%S')
                batch_time_str = batch_time.strftime('%H:%M')
            except:
                pass

            # 查询上一个 batch 的汇总数据
            summary_query = f"""
            SELECT
                SUM(spend) as total_spend,
                SUM(media_user_revenue) as total_revenue,
                SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas
            FROM `{table_ref}`
            WHERE stat_date = '{prev_stat_date}' AND batch_id = '{prev_batch_id}'
            """

            result = {"total_spend": 0, "media_roas": 0, "optimizer_data": [], "batch_time": batch_time_str, "batch_id": prev_batch_id}

            for row in self.client.query(summary_query).result():
                result["total_spend"] = float(row.total_spend or 0)
                result["media_roas"] = float(row.media_roas or 0)

            # 查询上一个 batch 的分投手数据
            optimizer_query = f"""
            SELECT
                optimizer,
                SUM(spend) as spend
            FROM `{table_ref}`
            WHERE stat_date = '{prev_stat_date}' AND batch_id = '{prev_batch_id}'
              AND optimizer IS NOT NULL AND optimizer != ''
            GROUP BY optimizer
            """

            for row in self.client.query(optimizer_query).result():
                result["optimizer_data"].append({
                    "optimizer": row.optimizer,
                    "spend": float(row.spend or 0)
                })

            return result

        except Exception as e:
            logger.error(f"查询上一个batch失败: {e}")
            return None

    def query_yesterday_same_hour_data(self, table_id: str = None, dataset_id: str = None) -> Dict[str, Any]:
        """
        查询昨日同时段数据（用于同比校验）

        Returns:
            昨日同时段的汇总数据
        """
        from datetime import datetime, timedelta
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        now = datetime.now()
        yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        current_hour = now.hour

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 查找昨日对应小时的 batch_id (格式: 20251223_100033)
        batch_query = f"""
        SELECT batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{yesterday}'
          AND CAST(SUBSTR(batch_id, 10, 2) AS INT64) = {current_hour}
        ORDER BY batch_id DESC
        LIMIT 1
        """

        try:
            batch_ids = [row.batch_id for row in self.client.query(batch_query).result()]

            if not batch_ids:
                logger.debug(f"未找到昨日 {current_hour}:00 的数据")
                return None

            batch_id = batch_ids[0]
            logger.debug(f"使用昨日batch: {batch_id}")

            # 查询昨日同时段汇总数据
            summary_query = f"""
            SELECT
                SUM(spend) as total_spend,
                SUM(media_user_revenue) as total_revenue,
                SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas,
                COUNT(DISTINCT optimizer) as optimizer_count,
                COUNT(DISTINCT campaign_id) as campaign_count
            FROM `{table_ref}`
            WHERE stat_date = '{yesterday}' AND batch_id = '{batch_id}'
            """

            result = {
                "date": yesterday,
                "hour": current_hour,
                "batch_id": batch_id,
                "total_spend": 0,
                "media_roas": 0,
                "optimizer_count": 0,
                "campaign_count": 0
            }

            for row in self.client.query(summary_query).result():
                result["total_spend"] = float(row.total_spend or 0)
                result["media_roas"] = float(row.media_roas or 0)
                result["optimizer_count"] = int(row.optimizer_count or 0)
                result["campaign_count"] = int(row.campaign_count or 0)

            return result

        except Exception as e:
            logger.error(f"查询昨日数据失败: {e}")
            return None

    def get_previous_hour_snapshot(self, table_id: str = "hourly_snapshots", dataset_id: str = "quickbi_data") -> Optional[Dict[str, Any]]:
        """
        获取上一小时的快照数据（用于计算环比）

        Args:
            table_id: 快照表名
            dataset_id: 数据集名

        Returns:
            上一小时的快照数据，如果不存在则返回 None
        """
        from datetime import datetime

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 查询最近一条快照
        query = f"""
        SELECT
            snapshot_time,
            total_spend,
            d0_roas,
            optimizer_data,
            batch_id
        FROM `{table_ref}`
        ORDER BY snapshot_time DESC
        LIMIT 1
        """

        try:
            results = list(self.client.query(query).result())
            if not results:
                logger.debug("未找到历史快照")
                return None

            row = results[0]

            # 解析 optimizer_data (JSON 字符串)
            optimizer_count = 0
            if row.optimizer_data:
                try:
                    import json
                    optimizer_list = json.loads(row.optimizer_data)
                    optimizer_count = len(optimizer_list)
                except:
                    pass

            return {
                "snapshot_time": row.snapshot_time.isoformat() if row.snapshot_time else None,
                "total_spend": float(row.total_spend or 0),
                "media_roas": float(row.d0_roas or 0),
                "optimizer_count": optimizer_count,
                "batch_id": row.batch_id
            }

        except Exception as e:
            logger.error(f"获取上一小时快照失败: {e}")
            return None

    def save_hourly_snapshot(self, realtime_data: Dict[str, Any], table_id: str = "hourly_snapshots", dataset_id: str = "quickbi_data") -> bool:
        """
        保存当前小时的快照数据

        Args:
            realtime_data: 实时播报数据
            table_id: 快照表名
            dataset_id: 数据集名

        Returns:
            是否保存成功
        """
        from datetime import datetime

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 提取汇总数据
        summary = realtime_data.get("summary", {})

        # 序列化 optimizer_data 为 JSON
        import json
        optimizer_data = json.dumps(realtime_data.get("optimizer_spend", []))

        # 构建插入数据 (匹配快照表结构)
        row = {
            "snapshot_time": datetime.now().isoformat(),
            "stat_date": realtime_data.get("date"),
            "hour": datetime.now().hour,
            "total_spend": float(summary.get("total_spend", 0)),
            "total_revenue": float(summary.get("total_media_revenue", 0)),
            "d0_roas": float(summary.get("media_roas", 0)),
            "optimizer_data": optimizer_data,
            "batch_id": realtime_data.get("batch_id")
        }

        try:
            # 插入数据
            errors = self.client.insert_rows_json(table_ref, [row])
            if errors:
                logger.error(f"保存快照失败: {errors}")
                return False

            logger.info(f"快照保存成功: {row['snapshot_time']}")
            return True

        except Exception as e:
            logger.error(f"保存快照异常: {e}")
            return False

    # ============ 大盘 Benchmark 查询 (用于个人助理) ============

    def query_market_benchmark(self, date: str = None, table_id: str = None, dataset_id: str = None) -> Dict[str, Any]:
        """
        查询大盘 Benchmark 数据 - 分剧集、分国家的平均 ROAS

        用于个人助理模块，对比个人计划与大盘表现

        Args:
            date: 查询日期，默认为今天
            table_id: 表 ID
            dataset_id: 数据集 ID

        Returns:
            {
                "date": "2025-12-29",
                "overall_roas": 0.45,  # 全公司平均 ROAS
                "drama_country_benchmark": {
                    "剧集A": {"US": 0.50, "KR": 0.40, ...},
                    "剧集B": {"US": 0.45, ...}
                },
                "hourly_benchmark": [  # 最近3小时大盘数据
                    {"hour": "12:00", "roas": 0.45, "cpm": 12.5},
                    ...
                ]
            }
        """
        from datetime import datetime
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        batch_id = self._get_latest_batch_id(table_ref, date)

        result = {
            "date": date,
            "batch_id": batch_id,
            "overall_roas": 0,
            "overall_cpm": 0,
            "drama_country_benchmark": {},
            "hourly_benchmark": []
        }

        if not batch_id:
            return result

        batch_filter = f"AND batch_id = '{batch_id}'"

        # 1. 全公司平均 ROAS 和 CPM
        overall_query = f"""
        SELECT
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as overall_roas,
            SAFE_DIVIDE(SUM(spend), SUM(impressions)) * 1000 as overall_cpm
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
        """

        # 2. 分剧集+国家的平均 ROAS
        drama_country_query = f"""
        SELECT
            drama_name,
            country,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as avg_roas,
            SUM(spend) as total_spend
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
          AND drama_name IS NOT NULL
          AND country IS NOT NULL
        GROUP BY drama_name, country
        HAVING SUM(spend) > 50
        ORDER BY drama_name, total_spend DESC
        """

        try:
            # 执行全公司查询
            for row in self.client.query(overall_query).result():
                result["overall_roas"] = float(row.overall_roas or 0)
                result["overall_cpm"] = float(row.overall_cpm or 0)

            # 执行分剧集+国家查询
            for row in self.client.query(drama_country_query).result():
                drama = row.drama_name
                country = row.country
                if drama not in result["drama_country_benchmark"]:
                    result["drama_country_benchmark"][drama] = {}
                result["drama_country_benchmark"][drama][country] = {
                    "avg_roas": float(row.avg_roas or 0),
                    "total_spend": float(row.total_spend or 0)
                }

            return result

        except Exception as e:
            logger.error(f"查询大盘 Benchmark 失败: {e}")
            return result

    def get_drama_country_benchmark(self, drama_name: str, country: str, date: str = None, table_id: str = None, dataset_id: str = None) -> Dict[str, Any]:
        """
        获取指定剧集在指定国家的大盘平均 ROAS

        用于个人助理的智能预警，对比个人计划与大盘

        Args:
            drama_name: 剧集名称
            country: 国家代码
            date: 查询日期，默认为今天

        Returns:
            {
                "drama_name": "剧集A",
                "country": "US",
                "avg_roas": 0.45,
                "total_spend": 5000,
                "campaign_count": 10
            }
        """
        from datetime import datetime
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        batch_id = self._get_latest_batch_id(table_ref, date)

        result = {
            "drama_name": drama_name,
            "country": country,
            "avg_roas": 0,
            "total_spend": 0,
            "campaign_count": 0
        }

        if not batch_id:
            return result

        query = f"""
        SELECT
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as avg_roas,
            SUM(spend) as total_spend,
            COUNT(DISTINCT campaign_id) as campaign_count
        FROM `{table_ref}`
        WHERE stat_date = '{date}'
          AND batch_id = '{batch_id}'
          AND drama_name = '{drama_name}'
          AND country = '{country}'
        """

        try:
            for row in self.client.query(query).result():
                result["avg_roas"] = float(row.avg_roas or 0)
                result["total_spend"] = float(row.total_spend or 0)
                result["campaign_count"] = int(row.campaign_count or 0)
            return result
        except Exception as e:
            logger.error(f"查询剧集国家 Benchmark 失败: {e}")
            return result

    def get_drama_benchmark(self, drama_name: str, date: str = None, table_id: str = None, dataset_id: str = None) -> Dict[str, Any]:
        """
        获取指定剧集的大盘平均 ROAS（不区分国家）

        Args:
            drama_name: 剧集名称
            date: 查询日期，默认为今天

        Returns:
            {"drama_name": "剧集A", "avg_roas": 0.45, "total_spend": 5000, "campaign_count": 10}
        """
        from datetime import datetime
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        batch_id = self._get_latest_batch_id(table_ref, date)

        result = {"drama_name": drama_name, "avg_roas": 0, "total_spend": 0, "campaign_count": 0}

        if not batch_id:
            return result

        # 转义单引号防止 SQL 注入
        safe_drama_name = drama_name.replace("'", "\\'")

        query = f"""
        SELECT
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as avg_roas,
            SUM(spend) as total_spend,
            COUNT(DISTINCT campaign_id) as campaign_count
        FROM `{table_ref}`
        WHERE stat_date = '{date}'
          AND batch_id = '{batch_id}'
          AND drama_name = '{safe_drama_name}'
        """

        try:
            for row in self.client.query(query).result():
                result["avg_roas"] = float(row.avg_roas or 0)
                result["total_spend"] = float(row.total_spend or 0)
                result["campaign_count"] = int(row.campaign_count or 0)
            return result
        except Exception as e:
            logger.error(f"查询剧集 Benchmark 失败: {e}")
            return result

    def query_optimizer_hourly_pacing(self, optimizer_name: str, hours: int = 3, table_id: str = None, dataset_id: str = None) -> Dict[str, Any]:
        """
        查询投手最近 N 小时的流水账数据 (Hourly Pacing)

        用于个人专属助理的小时级流水账功能

        Args:
            optimizer_name: 投手名称
            hours: 查询小时数，默认 3 小时
            table_id: 表 ID
            dataset_id: 数据集 ID

        Returns:
            {
                "optimizer": "kino",
                "hourly_data": [
                    {"hour": "14:00", "spend": 100, "revenue": 45, "roas": 0.45, "cpm": 12.5},
                    {"hour": "13:00", "spend": 80, "revenue": 40, "roas": 0.50, "cpm": 11.0},
                    ...
                ],
                "market_hourly_data": [
                    {"hour": "14:00", "roas": 0.42, "cpm": 13.0},
                    ...
                ]
            }
        """
        from datetime import datetime, timedelta
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        today = datetime.now().strftime('%Y-%m-%d')
        current_hour = datetime.now().hour
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        result = {
            "optimizer": optimizer_name,
            "hourly_data": [],
            "market_hourly_data": []
        }

        # 查询最近 N 小时的 batch_id 列表
        batch_hours = []
        for i in range(hours):
            target_hour = (current_hour - i) % 24
            # 跨天处理
            if current_hour - i < 0:
                target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                target_date = today

            # 查找该小时的 batch
            hour_start = f"{target_date.replace('-', '')}_{target_hour:02d}0000"
            hour_end = f"{target_date.replace('-', '')}_{target_hour:02d}1500"

            batch_query = f"""
            SELECT MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date = '{target_date}'
              AND batch_id >= '{hour_start}'
              AND batch_id <= '{hour_end}'
            """

            try:
                for row in self.client.query(batch_query).result():
                    if row.batch_id:
                        batch_hours.append({
                            "hour": f"{target_hour:02d}:00",
                            "batch_id": row.batch_id,
                            "stat_date": target_date
                        })
            except Exception as e:
                logger.error(f"查询 batch_id 失败: {e}")

        if not batch_hours:
            return result

        # 查询每个小时的投手数据和大盘数据
        for batch_info in batch_hours:
            batch_id = batch_info["batch_id"]
            stat_date = batch_info["stat_date"]
            hour_label = batch_info["hour"]

            # 投手个人数据
            optimizer_query = f"""
            SELECT
                SUM(spend) as spend,
                SUM(media_user_revenue) as revenue,
                SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas,
                SAFE_DIVIDE(SUM(spend), SUM(impressions)) * 1000 as cpm
            FROM `{table_ref}`
            WHERE stat_date = '{stat_date}'
              AND batch_id = '{batch_id}'
              AND optimizer = '{optimizer_name}'
            """

            # 大盘数据
            market_query = f"""
            SELECT
                SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas,
                SAFE_DIVIDE(SUM(spend), SUM(impressions)) * 1000 as cpm
            FROM `{table_ref}`
            WHERE stat_date = '{stat_date}'
              AND batch_id = '{batch_id}'
            """

            try:
                # 投手数据
                for row in self.client.query(optimizer_query).result():
                    result["hourly_data"].append({
                        "hour": hour_label,
                        "spend": float(row.spend or 0),
                        "revenue": float(row.revenue or 0),
                        "roas": float(row.roas or 0),
                        "cpm": float(row.cpm or 0)
                    })

                # 大盘数据
                for row in self.client.query(market_query).result():
                    result["market_hourly_data"].append({
                        "hour": hour_label,
                        "roas": float(row.roas or 0),
                        "cpm": float(row.cpm or 0)
                    })

            except Exception as e:
                logger.error(f"查询小时数据失败: {e}")

        return result

    def query_optimizer_alerts_with_benchmark(self, optimizer_name: str, table_id: str = None, dataset_id: str = None, use_latest_batch: bool = False) -> Dict[str, Any]:
        """
        查询投手的止损/扩量预警数据，并附带大盘对比

        Args:
            optimizer_name: 投手名称
            use_latest_batch: 是否使用绝对最新 batch（默认 False，使用整点 batch）

        Returns:
            {
                "optimizer": "kino",
                "stop_loss_alerts": [{campaign, drama, country, spend, roas, benchmark_roas, conclusion}],
                "scale_up_alerts": [{campaign, drama, country, spend, roas, benchmark_roas, conclusion}]
            }
        """
        from datetime import datetime
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        today = datetime.now().strftime('%Y-%m-%d')
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        if use_latest_batch:
            batch_id = self._get_absolute_latest_batch_id(table_ref, today)
        else:
            batch_id = self._get_latest_batch_id(table_ref, today)

        result = {
            "optimizer": optimizer_name,
            "stop_loss_alerts": [],
            "scale_up_alerts": []
        }

        if not batch_id:
            return result

        # 查询该投手的止损预警 (Spend > $300 且 ROAS < 30%)
        # 按 campaign 整体聚合，不拆分国家（避免单独调整国家导致计划不稳定）
        stop_loss_query = f"""
        SELECT
            campaign_id, campaign_name, drama_name, channel,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' AND batch_id = '{batch_id}'
          AND optimizer = '{optimizer_name}'
        GROUP BY campaign_id, campaign_name, drama_name, channel
        HAVING spend > 300 AND (revenue = 0 OR SAFE_DIVIDE(revenue, spend) < 0.30)
        ORDER BY spend DESC
        LIMIT 5
        """

        # 查询该投手的扩量机会 (Spend > $300 且 ROAS > 50%)
        # 按 campaign 整体聚合，不拆分国家
        scale_up_query = f"""
        SELECT
            campaign_id, campaign_name, drama_name, channel,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' AND batch_id = '{batch_id}'
          AND optimizer = '{optimizer_name}'
        GROUP BY campaign_id, campaign_name, drama_name, channel
        HAVING spend > 300 AND SAFE_DIVIDE(revenue, spend) > 0.50
        ORDER BY roas DESC
        LIMIT 5
        """

        # 分渠道大盘 ROAS (过滤掉 campaign_name 为空的非投放数据)
        channel_benchmark_query = f"""
        SELECT
            channel,
            SUM(spend) as spend,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' AND batch_id = '{batch_id}'
          AND channel IS NOT NULL
          AND campaign_name IS NOT NULL
          AND campaign_name != ''
          AND campaign_name != '-'
        GROUP BY channel
        """

        # 分地区明细查询 (包含消耗和ROAS)
        country_detail_query = f"""
        SELECT
            campaign_id,
            country,
            SUM(spend) as spend,
            SUM(media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' AND batch_id = '{batch_id}'
          AND optimizer = '{optimizer_name}'
          AND country IS NOT NULL
        GROUP BY campaign_id, country
        ORDER BY campaign_id, spend DESC
        """

        try:
            # 先获取分渠道大盘 ROAS 和总消耗
            channel_benchmark = {}
            for row in self.client.query(channel_benchmark_query).result():
                channel_benchmark[row.channel] = {
                    "roas": float(row.roas or 0),
                    "total_spend": float(row.spend or 0)
                }

            # 获取分地区明细
            campaign_country_details = {}
            for row in self.client.query(country_detail_query).result():
                cid = row.campaign_id
                if cid not in campaign_country_details:
                    campaign_country_details[cid] = []
                campaign_country_details[cid].append({
                    "country": row.country,
                    "spend": float(row.spend or 0),
                    "roas": float(row.roas or 0)
                })

            # 止损预警
            for row in self.client.query(stop_loss_query).result():
                drama = row.drama_name or ""
                channel = row.channel or ""
                roas = float(row.roas or 0)
                spend = float(row.spend or 0)
                revenue = float(row.revenue or 0)
                cid = row.campaign_id
                campaign_name = row.campaign_name or ""

                # 渠道大盘 ROAS 和总消耗
                channel_data = channel_benchmark.get(channel, {})
                channel_roas = channel_data.get("roas", 0)
                channel_total_spend = channel_data.get("total_spend", 0)

                # 计算消耗占比
                spend_ratio_in_channel = spend / channel_total_spend if channel_total_spend > 0 else 0

                # 分地区明细 - 获取 Top 国家
                country_details = campaign_country_details.get(cid, [])
                country_details = sorted(country_details, key=lambda x: x["spend"], reverse=True)

                # Top 国家信息
                top_country = country_details[0] if country_details else None
                top_country_info = None
                if top_country:
                    top_country_info = {
                        "country": top_country["country"],
                        "spend": top_country["spend"],
                        "roas": top_country["roas"],
                        "spend_ratio": top_country["spend"] / spend if spend > 0 else 0
                    }

                # 计算所有国家的占比
                for d in country_details[:5]:
                    d["spend_ratio"] = d["spend"] / spend if spend > 0 else 0

                result["stop_loss_alerts"].append({
                    "campaign_id": cid,
                    "campaign_name": campaign_name,
                    "drama_name": drama,
                    "channel": channel,
                    "spend": spend,
                    "revenue": revenue,
                    "roas": roas,
                    "channel_roas": channel_roas,
                    "spend_ratio_in_channel": spend_ratio_in_channel,
                    "top_country": top_country_info,
                    "country_details": country_details[:5]
                })

            # 扩量机会
            for row in self.client.query(scale_up_query).result():
                drama = row.drama_name or ""
                channel = row.channel or ""
                roas = float(row.roas or 0)
                spend = float(row.spend or 0)
                revenue = float(row.revenue or 0)
                cid = row.campaign_id
                campaign_name = row.campaign_name or ""

                # 渠道大盘 ROAS 和总消耗
                channel_data = channel_benchmark.get(channel, {})
                channel_roas = channel_data.get("roas", 0)
                channel_total_spend = channel_data.get("total_spend", 0)

                # 计算消耗占比
                spend_ratio_in_channel = spend / channel_total_spend if channel_total_spend > 0 else 0

                # 分地区明细 - 获取 Top 国家
                country_details = campaign_country_details.get(cid, [])
                country_details = sorted(country_details, key=lambda x: x["spend"], reverse=True)

                # Top 国家信息
                top_country = country_details[0] if country_details else None
                top_country_info = None
                if top_country:
                    top_country_info = {
                        "country": top_country["country"],
                        "spend": top_country["spend"],
                        "roas": top_country["roas"],
                        "spend_ratio": top_country["spend"] / spend if spend > 0 else 0
                    }

                # 计算所有国家的占比
                for d in country_details[:5]:
                    d["spend_ratio"] = d["spend"] / spend if spend > 0 else 0

                result["scale_up_alerts"].append({
                    "campaign_id": cid,
                    "campaign_name": campaign_name,
                    "drama_name": drama,
                    "channel": channel,
                    "spend": spend,
                    "revenue": revenue,
                    "roas": roas,
                    "channel_roas": channel_roas,
                    "spend_ratio_in_channel": spend_ratio_in_channel,
                    "top_country": top_country_info,
                    "country_details": country_details[:5]
                })

        except Exception as e:
            logger.error(f"查询投手预警数据失败: {e}")

        return result

    def query_optimizer_zombie_alerts(self, optimizer_name: str, table_id: str = None, dataset_id: str = None, use_latest_batch: bool = False) -> Dict[str, Any]:
        """
        查询投手的重启提醒 (Zombie Alert)

        逻辑：
        - status = Stopped 的计划
        - 过去1小时有回款
        - 累计 ROAS >= 40%

        Args:
            optimizer_name: 投手名称
            use_latest_batch: 是否使用绝对最新 batch（默认 False，使用整点 batch）

        Returns:
            {
                "optimizer": "kino",
                "zombie_alerts": [{campaign, drama, country, spend, roas, last_hour_revenue}]
            }
        """
        from datetime import datetime
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        today = datetime.now().strftime('%Y-%m-%d')
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 获取当前和上一小时的 batch_id
        if use_latest_batch:
            batch_id = self._get_absolute_latest_batch_id(table_ref, today)
        else:
            batch_id = self._get_latest_batch_id(table_ref, today)

        result = {
            "optimizer": optimizer_name,
            "zombie_alerts": []
        }

        if not batch_id:
            return result

        # 查询已关停但仍有回款且 ROAS 达标的计划
        # 注意：需要对比当前 batch 和上一 batch 的 revenue 差值
        zombie_query = f"""
        WITH current_data AS (
            SELECT campaign_id, campaign_name, drama_name, country, status,
                   SUM(spend) as spend,
                   SUM(media_user_revenue) as revenue,
                   SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
            FROM `{table_ref}`
            WHERE stat_date = '{today}' AND batch_id = '{batch_id}'
              AND optimizer = '{optimizer_name}'
              AND status = 'Stopped'
            GROUP BY campaign_id, campaign_name, drama_name, country, status
            HAVING spend > 100 AND SAFE_DIVIDE(revenue, spend) >= 0.40
        )
        SELECT * FROM current_data
        ORDER BY roas DESC
        LIMIT 5
        """

        try:
            for row in self.client.query(zombie_query).result():
                result["zombie_alerts"].append({
                    "campaign_id": row.campaign_id,
                    "campaign_name": row.campaign_name,
                    "drama_name": row.drama_name or "",
                    "country": row.country or "",
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })
        except Exception as e:
            logger.error(f"查询重启提醒失败: {e}")

        return result

    # ============ 周报数据查询 ============

    def query_weekly_report_data(self, week_start: str = None, week_end: str = None,
                                  table_id: str = None, dataset_id: str = None,
                                  use_latest_batch: bool = True) -> Dict[str, Any]:
        """
        查询周报播报所需的全部数据 (W-1 周)

        Args:
            week_start: 周开始日期 (周一)，格式 YYYY-MM-DD，默认为上周一
            week_end: 周结束日期 (周日)，格式 YYYY-MM-DD，默认为上周日
            table_id: 表 ID
            dataset_id: 数据集 ID
            use_latest_batch: 是否使用绝对最新 batch（默认 True，周报默认取最新）

        Returns:
            包含周报所需全部数据的字典
        """
        from datetime import datetime, timedelta
        from config.data_source import get_data_source_config

        # 从配置获取默认数据源
        if table_id is None or dataset_id is None:
            config = get_data_source_config()
            table_id = table_id or config["table_id"]
            dataset_id = dataset_id or config["dataset_id"]

        # 默认查询上周数据 (W-1)
        if week_start is None or week_end is None:
            today = datetime.now()
            # 找到上周一: 本周一 - 7天
            days_since_monday = today.weekday()
            this_monday = today - timedelta(days=days_since_monday)
            last_monday = this_monday - timedelta(days=7)
            last_sunday = this_monday - timedelta(days=1)
            week_start = last_monday.strftime('%Y-%m-%d')
            week_end = last_sunday.strftime('%Y-%m-%d')

        logger.info(f"查询周报数据: {week_start} ~ {week_end}")

        # 计算上上周日期 (W-2) 用于环比
        week_start_dt = datetime.strptime(week_start, '%Y-%m-%d')
        week_end_dt = datetime.strptime(week_end, '%Y-%m-%d')
        actual_days = (week_end_dt - week_start_dt).days + 1  # 实际天数
        prev_week_start = (week_start_dt - timedelta(days=7)).strftime('%Y-%m-%d')
        prev_week_end = (week_start_dt - timedelta(days=1)).strftime('%Y-%m-%d')

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        result = {
            "week_start": week_start,
            "week_end": week_end,
            "summary": {},
            "prev_week_summary": {},
            "daily_stats": [],
            "optimizer_weekly": [],
            "prev_optimizer_weekly": [],
            "top_dramas": [],
            "potential_dramas": [],
            "declining_dramas": [],
            "prev_drama_roas": {},
            "top_countries": [],
            "prev_country_roas": {},
            "emerging_markets": [],
            # 新增字段
            "week_avg_cpm": 0,
            "prev_week_avg_cpm": 0,
            "new_campaigns_count": 0,
            "losing_dramas": [],  # 尾部亏损剧集
            "editor_stats": [],  # 剪辑师统计数据
        }

        # 1. 本周大盘汇总 (增加 CPM)
        summary_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{week_start}' AND '{week_end}'
            GROUP BY stat_date
        )
        SELECT
            SUM(t.spend) as week_total_spend,
            SUM(t.media_user_revenue) as week_total_revenue,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as week_avg_roas,
            SAFE_DIVIDE(SUM(t.spend), SUM(t.impressions)) * 1000 as week_avg_cpm,
            COUNT(DISTINCT t.campaign_id) as total_campaigns
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        """

        # 2. 上周大盘汇总 (环比，增加 CPM)
        prev_summary_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{prev_week_start}' AND '{prev_week_end}'
            GROUP BY stat_date
        )
        SELECT
            SUM(t.spend) as week_total_spend,
            SUM(t.media_user_revenue) as week_total_revenue,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as week_avg_roas,
            AVG(t.cpm) as week_avg_cpm
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        """

        # 3. 日趋势
        daily_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{week_start}' AND '{week_end}'
            GROUP BY stat_date
        )
        SELECT
            t.stat_date,
            SUM(t.spend) as spend,
            SUM(t.media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as roas
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        GROUP BY t.stat_date
        ORDER BY t.stat_date
        """

        # 4. 本周投手数据
        optimizer_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{week_start}' AND '{week_end}'
            GROUP BY stat_date
        )
        SELECT
            t.optimizer,
            SUM(t.spend) as spend,
            SUM(t.media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as roas,
            COUNT(DISTINCT t.campaign_id) as campaign_count
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        WHERE t.optimizer IS NOT NULL AND t.optimizer != ''
        GROUP BY t.optimizer
        ORDER BY spend DESC
        """

        # 5. 上周投手数据 (环比)
        prev_optimizer_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{prev_week_start}' AND '{prev_week_end}'
            GROUP BY stat_date
        )
        SELECT
            t.optimizer,
            SUM(t.spend) as spend,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as roas
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        WHERE t.optimizer IS NOT NULL AND t.optimizer != ''
        GROUP BY t.optimizer
        """

        # 6. 本周剧集数据
        drama_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{week_start}' AND '{week_end}'
            GROUP BY stat_date
        )
        SELECT
            t.drama_name,
            SUM(t.spend) as spend,
            SUM(t.media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as roas
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        WHERE t.drama_name IS NOT NULL AND t.drama_name != ''
        GROUP BY t.drama_name
        ORDER BY spend DESC
        """

        # 7. 上周剧集 ROAS (环比)
        prev_drama_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{prev_week_start}' AND '{prev_week_end}'
            GROUP BY stat_date
        )
        SELECT
            t.drama_name,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as roas
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        WHERE t.drama_name IS NOT NULL AND t.drama_name != ''
        GROUP BY t.drama_name
        """

        # 8. 剧集主投国家 (含消耗和 ROAS)
        drama_country_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{week_start}' AND '{week_end}'
            GROUP BY stat_date
        )
        SELECT
            t.drama_name,
            t.country,
            SUM(t.spend) as spend,
            SUM(t.media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as roas
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        WHERE t.drama_name IS NOT NULL AND t.country IS NOT NULL
        GROUP BY t.drama_name, t.country
        ORDER BY t.drama_name, spend DESC
        """

        # 9. 本周国家数据
        country_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{week_start}' AND '{week_end}'
            GROUP BY stat_date
        )
        SELECT
            t.country,
            SUM(t.spend) as spend,
            SUM(t.media_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as roas
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        WHERE t.country IS NOT NULL AND t.country != ''
        GROUP BY t.country
        ORDER BY spend DESC
        """

        # 10. 上周国家 ROAS (环比)
        prev_country_query = f"""
        WITH daily_batches AS (
            SELECT stat_date, MAX(batch_id) as batch_id
            FROM `{table_ref}`
            WHERE stat_date BETWEEN '{prev_week_start}' AND '{prev_week_end}'
            GROUP BY stat_date
        )
        SELECT
            t.country,
            SAFE_DIVIDE(SUM(t.media_user_revenue), SUM(t.spend)) as roas
        FROM `{table_ref}` t
        JOIN daily_batches b ON t.stat_date = b.stat_date AND t.batch_id = b.batch_id
        WHERE t.country IS NOT NULL AND t.country != ''
        GROUP BY t.country
        """

        try:
            # 执行查询
            # 1. 本周大盘
            for row in self.client.query(summary_query).result():
                result["summary"] = {
                    "week_total_spend": float(row.week_total_spend or 0),
                    "week_total_revenue": float(row.week_total_revenue or 0),
                    "week_avg_roas": float(row.week_avg_roas or 0),
                    "daily_avg_spend": float(row.week_total_spend or 0) / actual_days if actual_days > 0 else 0,
                    "week_avg_cpm": float(row.week_avg_cpm or 0),
                    "total_campaigns": int(row.total_campaigns or 0)
                }

            # 2. 上周大盘
            for row in self.client.query(prev_summary_query).result():
                result["prev_week_summary"] = {
                    "week_total_spend": float(row.week_total_spend or 0),
                    "week_total_revenue": float(row.week_total_revenue or 0),
                    "week_avg_roas": float(row.week_avg_roas or 0),
                    "week_avg_cpm": float(row.week_avg_cpm or 0)
                }

            # 3. 日趋势
            for row in self.client.query(daily_query).result():
                result["daily_stats"].append({
                    "date": str(row.stat_date),
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })

            # 4. 本周投手
            for row in self.client.query(optimizer_query).result():
                result["optimizer_weekly"].append({
                    "name": row.optimizer,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0),
                    "campaign_count": int(row.campaign_count or 0)
                })

            # 5. 上周投手 ROAS
            prev_opt_roas = {}
            for row in self.client.query(prev_optimizer_query).result():
                prev_opt_roas[row.optimizer] = float(row.roas or 0)

            # 计算投手 ROAS 环比
            for opt in result["optimizer_weekly"]:
                prev_roas = prev_opt_roas.get(opt["name"], 0)
                opt["prev_roas"] = prev_roas
                opt["roas_change"] = opt["roas"] - prev_roas

            # 6. 剧集数据
            all_dramas = []
            for row in self.client.query(drama_query).result():
                all_dramas.append({
                    "name": row.drama_name,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })

            # 7. 上周剧集 ROAS
            for row in self.client.query(prev_drama_query).result():
                result["prev_drama_roas"][row.drama_name] = float(row.roas or 0)

            # 8. 剧集主投国家 (含消耗和 ROAS)
            drama_countries = {}
            drama_country_details = {}  # 存储详细数据: {drama: [{country, spend, roas}, ...]}
            for row in self.client.query(drama_country_query).result():
                drama = row.drama_name
                if drama not in drama_countries:
                    drama_countries[drama] = []
                    drama_country_details[drama] = []
                if len(drama_countries[drama]) < 5:  # 扩展到 Top 5 国家
                    drama_countries[drama].append(row.country)
                    drama_country_details[drama].append({
                        "country": row.country,
                        "spend": float(row.spend or 0),
                        "revenue": float(row.revenue or 0),
                        "roas": float(row.roas or 0)
                    })

            # 分类剧集
            for drama in all_dramas:
                name = drama["name"]
                spend = drama["spend"]
                roas = drama["roas"]
                prev_roas = result["prev_drama_roas"].get(name, 0)
                roas_change = roas - prev_roas if prev_roas > 0 else 0

                drama["top_countries"] = drama_countries.get(name, [])
                drama["country_details"] = drama_country_details.get(name, [])  # 各国家详细数据
                drama["prev_roas"] = prev_roas
                drama["roas_change"] = roas_change

                # 头部剧集: 消耗 > $10k 且 ROAS > 40%
                if spend > 10000 and roas > 0.40:
                    result["top_dramas"].append(drama)
                # 潜力剧集: 消耗 $1k-$10k 且 ROAS > 50%
                elif 1000 <= spend <= 10000 and roas > 0.50:
                    result["potential_dramas"].append(drama)
                # 尾部亏损剧集: 消耗 > $1k 且 ROAS < 25%
                if spend > 1000 and roas < 0.25:
                    result["losing_dramas"].append(drama)
                # 衰退预警: ROAS 周环比下降 > 10%
                if prev_roas > 0 and roas_change < -0.10:
                    result["declining_dramas"].append(drama)

            # 尾部亏损剧集按消耗降序排列
            result["losing_dramas"].sort(key=lambda x: x["spend"], reverse=True)

            # 9. 国家数据
            all_countries = []
            for row in self.client.query(country_query).result():
                all_countries.append({
                    "name": row.country,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })

            # 10. 上周国家 ROAS
            for row in self.client.query(prev_country_query).result():
                result["prev_country_roas"][row.country] = float(row.roas or 0)

            # 主力市场 Top 5
            main_markets = ["US", "KR", "JP", "TW", "DE"]
            for country in all_countries[:10]:
                name = country["name"]
                prev_roas = result["prev_country_roas"].get(name, 0)
                country["prev_roas"] = prev_roas
                country["roas_change"] = country["roas"] - prev_roas if prev_roas > 0 else 0
                result["top_countries"].append(country)

            # 新兴机会: 非主投国家，ROAS > 50%
            for country in all_countries:
                name = country["name"]
                if name not in main_markets[:3] and country["roas"] > 0.50 and country["spend"] > 500:
                    result["emerging_markets"].append(country)

            result["emerging_markets"] = sorted(
                result["emerging_markets"], key=lambda x: x["roas"], reverse=True
            )[:5]

            # 11. 从 quickbi_overview 表获取真实的周度 revenue
            # 注意：overview 表固定在 quickbi_data 数据集中
            overview_table = f"{self.project_id}.quickbi_data.quickbi_overview"

            # 11.1 获取本周的 overview revenue (汇总每日)
            week_overview_query = f"""
            WITH daily_overview AS (
                SELECT stat_date, total_revenue,
                       ROW_NUMBER() OVER (PARTITION BY stat_date ORDER BY batch_id DESC) as rn
                FROM `{overview_table}`
                WHERE stat_date BETWEEN '{week_start}' AND '{week_end}'
            )
            SELECT SUM(total_revenue) as week_total_revenue
            FROM daily_overview
            WHERE rn = 1
            """
            try:
                for row in self.client.query(week_overview_query).result():
                    real_week_revenue = float(row.week_total_revenue or 0)
                    if real_week_revenue > 0:
                        result["summary"]["week_total_revenue"] = real_week_revenue
            except Exception as e:
                logger.warning(f"查询周报 overview 表失败，使用计算值: {e}")

            # 11.2 获取上周的 overview revenue (用于环比)
            prev_week_overview_query = f"""
            WITH daily_overview AS (
                SELECT stat_date, total_revenue,
                       ROW_NUMBER() OVER (PARTITION BY stat_date ORDER BY batch_id DESC) as rn
                FROM `{overview_table}`
                WHERE stat_date BETWEEN '{prev_week_start}' AND '{prev_week_end}'
            )
            SELECT SUM(total_revenue) as week_total_revenue
            FROM daily_overview
            WHERE rn = 1
            """
            try:
                for row in self.client.query(prev_week_overview_query).result():
                    prev_week_revenue = float(row.week_total_revenue or 0)
                    if prev_week_revenue > 0:
                        result["prev_week_summary"]["week_total_revenue"] = prev_week_revenue
            except Exception as e:
                logger.warning(f"查询周报 W-2 overview 表失败，使用计算值: {e}")

            # 12. 获取剪辑师统计数据 (从 BigQuery 查询)
            try:
                editor_query = f"""
                SELECT
                    editor_name as name,
                    SUM(material_count) as total_material_count,
                    SUM(spend) as total_spend,
                    SUM(revenue) as total_revenue,
                    SAFE_DIVIDE(SUM(revenue), SUM(spend)) as roas,
                    SUM(hot_count) as total_hot_count,
                    SAFE_DIVIDE(SUM(hot_count), SUM(material_count)) as hot_rate,
                    ARRAY_AGG(top_material ORDER BY top_material_spend DESC LIMIT 1)[OFFSET(0)] as top_material
                FROM `{self.project_id}.xmp_data.xmp_editor_stats`
                WHERE stat_date BETWEEN '{week_start}' AND '{week_end}'
                    AND channel = 'all'
                    AND editor_name IS NOT NULL
                    AND editor_name != ''
                GROUP BY editor_name
                HAVING SUM(spend) > 0
                ORDER BY total_spend DESC
                """
                editor_job = self.client.query(editor_query)
                editor_stats = []
                for row in editor_job:
                    editor_stats.append({
                        "name": row.name,
                        "material_count": int(row.total_material_count or 0),
                        "spend": float(row.total_spend or 0),
                        "revenue": float(row.total_revenue or 0),
                        "roas": float(row.roas or 0),
                        "hot_count": int(row.total_hot_count or 0),
                        "hot_rate": float(row.hot_rate or 0),
                        "top_material": row.top_material or ""
                    })
                result["editor_stats"] = editor_stats
                logger.info(f"获取剪辑师统计: {len(editor_stats)} 人")
            except Exception as e:
                logger.warning(f"获取剪辑师统计失败: {e}")

            return result

        except Exception as e:
            logger.error(f"查询周报数据失败: {e}")
            return result


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
        logger.error("请在 .env 文件中设置 BQ_PROJECT_ID 和 BQ_DATASET_ID")
        exit(1)

    # 从本地文件读取数据测试
    try:
        with open("api_responses.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        count = upload_xmp_to_bigquery(data, project_id, dataset_id)
        logger.info(f"测试上传完成: {count} 条记录")
    except FileNotFoundError:
        logger.warning("未找到 api_responses.json 文件，请先运行爬虫")
    except Exception as e:
        logger.error(f"测试上传失败: {e}")
