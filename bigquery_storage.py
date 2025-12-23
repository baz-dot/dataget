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
            bigquery.SchemaField("batch_id", "STRING"),  # 批次 ID，用于区分不同爬取批次
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("page_number", "INTEGER"),
        ]

        try:
            self.client.get_table(table_ref)
            print(f"表已存在: {table_ref}")
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            print(f"✓ 已创建表: {table_ref}")

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
            print(f"表已存在: {table_ref}")
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            print(f"✓ 已创建表: {table_ref}")

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
            print("没有数据需要上传")
            return 0

        # 获取表引用
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        # 插入数据
        errors = self.client.insert_rows_json(table_ref, rows)

        if errors:
            print(f"✗ BigQuery 插入错误: {errors}")
            return 0

        print(f"✓ 已上传 {len(rows)} 条 DataEye 记录到 BigQuery: {table_ref}")
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

    def _ensure_quickbi_table_exists(self, table_id: str = "quickbi_campaigns"):
        """确保 Quick BI 表存在，不存在则创建（单表模式，按 stat_date 分区）"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("stat_date", "DATE"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("language", "STRING"),
            bigquery.SchemaField("device", "STRING"),
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
            bigquery.SchemaField("lp_pv", "FLOAT"),
            bigquery.SchemaField("lp_cta_clicks", "FLOAT"),
            bigquery.SchemaField("new_users", "INTEGER"),
            bigquery.SchemaField("new_payers", "INTEGER"),
            bigquery.SchemaField("new_iap_purchases", "INTEGER"),
            bigquery.SchemaField("new_sub_purchases", "INTEGER"),
            bigquery.SchemaField("new_user_revenue", "FLOAT"),
            bigquery.SchemaField("new_ad_revenue", "FLOAT"),
            bigquery.SchemaField("new_sub_revenue", "FLOAT"),
            # 计算字段
            bigquery.SchemaField("cpi", "FLOAT"),
            bigquery.SchemaField("cac", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("cvr", "FLOAT"),
            bigquery.SchemaField("cpc", "FLOAT"),
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("d0_roas", "FLOAT"),
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
            print(f"✓ 已创建分区表: {table_ref} (按 stat_date 分区)")

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
            print("没有数据需要上传")
            return 0

        # 写入主表
        main_table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        errors = self.client.insert_rows_json(main_table_ref, rows)

        if errors:
            print(f"✗ BigQuery 插入错误: {errors}")
            return 0

        print(f"✓ 已上传 {len(rows)} 条 Quick BI 记录到: {main_table_ref}")
        if batch_id:
            print(f"  批次 ID: {batch_id}")

        return len(rows)

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
                'language': self._safe_str(record.get('language')),
                'device': self._safe_str(record.get('device')),
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
                'lp_pv': self._safe_float(record.get('lp_pv')),
                'lp_cta_clicks': self._safe_float(record.get('lp_clicks')),
                'new_users': self._safe_int(record.get('new_users')),
                'new_payers': self._safe_int(record.get('new_payers')),
                'new_iap_purchases': self._safe_int(record.get('new_iap_purchases')),
                'new_sub_purchases': self._safe_int(record.get('new_sub_purchases')),
                'new_user_revenue': self._safe_float(record.get('new_user_revenue')),
                'new_ad_revenue': self._safe_float(record.get('new_ad_revenue')),
                'new_sub_revenue': self._safe_float(record.get('new_sub_revenue')),
                # 计算字段
                'cpi': self._calculate_ratio(record.get('spend'), record.get('new_users')),
                'cac': self._calculate_ratio(record.get('spend'), record.get('new_users')),
                'ctr': self._calculate_ratio(record.get('clicks'), record.get('impressions')),
                'cvr': self._calculate_ratio(record.get('new_users'), record.get('clicks')),
                'cpc': self._calculate_ratio(record.get('spend'), record.get('clicks')),
                'cpm': self._calculate_cpm(record.get('spend'), record.get('impressions')),
                'd0_roas': self._calculate_roas(record.get('new_user_revenue'), record.get('spend')),
                'media_d0_roas': self._calculate_roas(record.get('new_user_revenue'), record.get('spend')),
                'new_user_paying_rate': self._calculate_ratio(record.get('new_payers'), record.get('new_users')),
                'arpu': self._calculate_ratio(record.get('new_user_revenue'), record.get('new_users')),
                # 批次追踪字段
                'batch_id': batch_id,
                'fetched_at': fetched_at,
            }
            rows.append(row)

        return rows

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
            print(f"查询失败: {e}")
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
            SUM(new_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(clicks), SUM(impressions)) as ctr,
            SAFE_DIVIDE(SUM(spend), SUM(clicks)) as cpc,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
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
            print(f"查询失败: {e}")
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
            SUM(new_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
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
            print(f"查询失败: {e}")
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
            print(f"查询失败: {e}")
            return []

    # ============ 日报播报数据查询 ============

    def _get_latest_batch_id(self, table_ref: str, stat_date: str) -> str:
        """
        获取指定日期的最新 batch_id

        Args:
            table_ref: 表引用
            stat_date: 统计日期 (YYYY-MM-DD)

        Returns:
            最新的 batch_id，如 20251221_230026
        """
        query = f"""
        SELECT MAX(batch_id) as latest_batch_id
        FROM `{table_ref}`
        WHERE stat_date = '{stat_date}'
        """
        try:
            for row in self.client.query(query).result():
                return row.latest_batch_id
        except Exception as e:
            print(f"查询最新 batch_id 失败: {e}")
        return None

    def query_daily_report_data(self, date: str = None, table_id: str = "quickbi_campaigns", dataset_id: str = "quickbi_data") -> Dict[str, Any]:
        """
        查询日报播报所需的全部数据 (T-1 日) - 单表模式

        目标：为管理层提供昨天的全盘复盘，辅助战略决策
        - 触发时间：每日 09:00
        - 取数范围：stat_date = Yesterday (T-1) 的最新批次数据

        Args:
            date: 查询日期，格式 YYYY-MM-DD，默认为昨天 (T-1)
            table_id: 表 ID，默认为 quickbi_campaigns
            dataset_id: 数据集 ID，默认为 quickbi_data

        Returns:
            包含日报所需全部数据的字典
        """
        from datetime import datetime, timedelta

        # 默认查询昨天的数据 (T-1)
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"查询日期 (T-1): {date}")

        # 计算前天日期 (T-2) 用于环比
        query_date = datetime.strptime(date, '%Y-%m-%d')
        day_before = (query_date - timedelta(days=1)).strftime('%Y-%m-%d')

        # 表引用
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 获取 T-1 和 T-2 的最新 batch_id
        batch_id = self._get_latest_batch_id(table_ref, date)
        batch_id_prev = self._get_latest_batch_id(table_ref, day_before)

        if batch_id:
            print(f"T-1 最新批次: {batch_id}")
        else:
            print(f"未找到 {date} 的数据")

        if batch_id_prev:
            print(f"T-2 最新批次: {batch_id_prev}")
        else:
            print(f"未找到 {day_before} 的数据，环比可能为空")

        result = {
            "date": date,
            "summary": {},
            "summary_prev": {},
            "optimizers": [],
            "dramas_top5": [],
            "countries_top5": [],
            "scale_up_dramas": [],
            "opportunity_markets": []
        }

        # 构建 batch_id 过滤条件
        batch_filter = f"AND batch_id = '{batch_id}'" if batch_id else ""
        batch_filter_prev = f"AND batch_id = '{batch_id_prev}'" if batch_id_prev else ""

        # 1. 大盘总览 (T-1)
        summary_query = f"""
        SELECT
            SUM(spend) as total_spend,
            SUM(new_user_revenue) as total_revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as global_roas
        FROM `{table_ref}`
        WHERE stat_date = '{date}' {batch_filter}
        """

        # 2. 大盘总览 (T-2) 用于环比
        summary_prev_query = f"""
        SELECT
            SUM(spend) as total_spend,
            SUM(new_user_revenue) as total_revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as global_roas
        FROM `{table_ref}`
        WHERE stat_date = '{day_before}' {batch_filter_prev}
        """

        # 3. 投手排行榜
        optimizer_query = f"""
        SELECT
            optimizer,
            SUM(spend) as spend,
            SUM(new_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas,
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
            SUM(new_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
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
            SUM(new_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
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
                SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
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
                SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
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

        try:
            # 执行查询
            # 1. 大盘总览
            for row in self.client.query(summary_query).result():
                result["summary"] = {
                    "total_spend": float(row.total_spend or 0),
                    "total_revenue": float(row.total_revenue or 0),
                    "global_roas": float(row.global_roas or 0)
                }

            # 2. 前一天数据
            for row in self.client.query(summary_prev_query).result():
                result["summary_prev"] = {
                    "total_spend": float(row.total_spend or 0),
                    "total_revenue": float(row.total_revenue or 0),
                    "global_roas": float(row.global_roas or 0)
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

            return result

        except Exception as e:
            print(f"查询日报数据失败: {e}")
            return result


    # ============ 实时播报数据查询 (小时级) ============

    def query_realtime_report_data(self, table_id: str = "quickbi_campaigns", dataset_id: str = "quickbi_data") -> Dict[str, Any]:
        """
        查询实时播报所需的全部数据 (当日累计)

        目标：为执行层提供"每小时"的监控，发现异动，即时调整
        - 触发时间：每日 9:00 - 24:00，每整点触发
        - 取数范围：当日累计数据 (stat_date = Today)

        Returns:
            包含实时播报所需全部数据的字典
        """
        from datetime import datetime

        today = datetime.now().strftime('%Y-%m-%d')
        current_hour = datetime.now().strftime('%H:%M')

        # 表引用
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 获取当日最新 batch_id
        batch_id = self._get_latest_batch_id(table_ref, today)

        result = {
            "date": today,
            "current_hour": current_hour,
            "batch_id": batch_id,
            "api_update_time": None,
            "data_delayed": False,
            "summary": {},
            "optimizer_spend": [],
            "stop_loss_campaigns": [],
            "scale_up_campaigns": [],
            "country_marginal_roas": []
        }

        if not batch_id:
            print(f"未找到 {today} 的数据")
            return result

        # 解析 batch_id 获取 API 更新时间 (格式: 20251222_103000)
        try:
            batch_time_str = batch_id.replace('_', '')
            api_update_time = datetime.strptime(batch_time_str, '%Y%m%d%H%M%S')
            result["api_update_time"] = api_update_time.strftime('%Y-%m-%d %H:%M:%S')

            # 检查数据延迟 (超过2小时)
            time_diff = datetime.now() - api_update_time
            if time_diff.total_seconds() > 7200:  # 2小时 = 7200秒
                result["data_delayed"] = True
        except Exception:
            pass

        batch_filter = f"AND batch_id = '{batch_id}'"

        # 1. 大盘总览 (当日累计)
        summary_query = f"""
        SELECT
            SUM(spend) as total_spend,
            SUM(new_user_revenue) as total_revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as d0_roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
        """

        # 2. 分投手消耗 (用于归因分析) - 简化查询，避免 STRUCT 问题
        optimizer_query = f"""
        SELECT
            optimizer,
            SUM(spend) as spend,
            SUM(new_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
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

        # 3. 止损预警 (Spend > $300 且 ROAS < 30%)
        stop_loss_query = f"""
        SELECT
            campaign_id,
            campaign_name,
            optimizer,
            drama_name,
            country,
            SUM(spend) as spend,
            SUM(new_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
        GROUP BY campaign_id, campaign_name, optimizer, drama_name, country
        HAVING spend > 300 AND (revenue = 0 OR SAFE_DIVIDE(revenue, spend) < 0.30)
        ORDER BY spend DESC
        LIMIT 10
        """

        # 4. 扩量机会 (Spend > $300 且 ROAS > 50%)
        scale_up_query = f"""
        SELECT
            campaign_id,
            campaign_name,
            optimizer,
            drama_name,
            country,
            SUM(spend) as spend,
            SUM(new_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
        GROUP BY campaign_id, campaign_name, optimizer, drama_name, country
        HAVING spend > 300 AND SAFE_DIVIDE(revenue, spend) > 0.50
        ORDER BY roas DESC
        LIMIT 10
        """

        # 5. 分国家边际 ROAS (用于地区观察)
        country_query = f"""
        SELECT
            country,
            SUM(spend) as spend,
            SUM(new_user_revenue) as revenue,
            SAFE_DIVIDE(SUM(new_user_revenue), SUM(spend)) as roas
        FROM `{table_ref}`
        WHERE stat_date = '{today}' {batch_filter}
          AND country IS NOT NULL
          AND country != ''
        GROUP BY country
        HAVING spend > 100
        ORDER BY roas DESC
        """

        try:
            # 执行查询
            # 1. 大盘总览
            for row in self.client.query(summary_query).result():
                result["summary"] = {
                    "total_spend": float(row.total_spend or 0),
                    "total_revenue": float(row.total_revenue or 0),
                    "d0_roas": float(row.d0_roas or 0)
                }

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

            # 3. 止损预警
            for row in self.client.query(stop_loss_query).result():
                result["stop_loss_campaigns"].append({
                    "campaign_id": row.campaign_id,
                    "campaign_name": row.campaign_name,
                    "optimizer": row.optimizer,
                    "drama_name": row.drama_name,
                    "country": row.country,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })

            # 4. 扩量机会
            for row in self.client.query(scale_up_query).result():
                result["scale_up_campaigns"].append({
                    "campaign_id": row.campaign_id,
                    "campaign_name": row.campaign_name,
                    "optimizer": row.optimizer,
                    "drama_name": row.drama_name,
                    "country": row.country,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })

            # 5. 分国家边际 ROAS
            for row in self.client.query(country_query).result():
                result["country_marginal_roas"].append({
                    "country": row.country,
                    "spend": float(row.spend or 0),
                    "revenue": float(row.revenue or 0),
                    "roas": float(row.roas or 0)
                })

            return result

        except Exception as e:
            print(f"查询实时播报数据失败: {e}")
            return result

    def save_hourly_snapshot(self, data: Dict[str, Any], table_id: str = "hourly_snapshots") -> bool:
        """
        保存小时级快照数据 (用于计算小时环比)

        Args:
            data: 实时播报数据
            table_id: 快照表 ID

        Returns:
            是否保存成功
        """
        from datetime import datetime

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        # 确保快照表存在
        self._ensure_hourly_snapshot_table_exists(table_id)

        snapshot = {
            "snapshot_time": datetime.now().isoformat(),
            "stat_date": data.get("date"),
            "hour": datetime.now().hour,
            "total_spend": data.get("summary", {}).get("total_spend", 0),
            "total_revenue": data.get("summary", {}).get("total_revenue", 0),
            "d0_roas": data.get("summary", {}).get("d0_roas", 0),
            "optimizer_data": json.dumps(data.get("optimizer_spend", []), ensure_ascii=False),
            "batch_id": data.get("batch_id")
        }

        try:
            errors = self.client.insert_rows_json(table_ref, [snapshot])
            if errors:
                print(f"保存快照失败: {errors}")
                return False
            print(f"[OK] 已保存小时快照: {snapshot['snapshot_time']}")
            return True
        except Exception as e:
            print(f"保存快照异常: {e}")
            return False

    def _ensure_hourly_snapshot_table_exists(self, table_id: str = "hourly_snapshots"):
        """确保小时快照表存在"""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("snapshot_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("stat_date", "DATE"),
            bigquery.SchemaField("hour", "INTEGER"),
            bigquery.SchemaField("total_spend", "FLOAT"),
            bigquery.SchemaField("total_revenue", "FLOAT"),
            bigquery.SchemaField("d0_roas", "FLOAT"),
            bigquery.SchemaField("optimizer_data", "STRING"),  # JSON 字符串
            bigquery.SchemaField("batch_id", "STRING"),
        ]

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="snapshot_time"
            )
            self.client.create_table(table)
            print(f"[OK] 已创建小时快照表: {table_ref}")

    def get_previous_hour_snapshot(self, table_id: str = "hourly_snapshots") -> Dict[str, Any]:
        """
        获取一个小时前的快照数据

        查找距离当前时间约1小时前的快照（45分钟到75分钟之间的最新一条）
        如果没有找到，则返回最近一条快照

        Returns:
            一个小时前的快照数据
        """
        from datetime import datetime, timedelta

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        now = datetime.now()

        # 一小时前的时间窗口：45分钟到75分钟之间
        one_hour_ago_start = now - timedelta(minutes=75)
        one_hour_ago_end = now - timedelta(minutes=45)

        # 优先查找一小时前时间窗口内的快照
        query = f"""
        SELECT *
        FROM `{table_ref}`
        WHERE snapshot_time >= '{one_hour_ago_start.strftime('%Y-%m-%d %H:%M:%S')}'
          AND snapshot_time <= '{one_hour_ago_end.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY snapshot_time DESC
        LIMIT 1
        """

        try:
            for row in self.client.query(query).result():
                print(f"[快照] 找到1小时前快照: {row.snapshot_time}")
                return {
                    "snapshot_time": str(row.snapshot_time),
                    "stat_date": str(row.stat_date),
                    "hour": row.hour,
                    "total_spend": float(row.total_spend or 0),
                    "total_revenue": float(row.total_revenue or 0),
                    "d0_roas": float(row.d0_roas or 0),
                    "optimizer_data": json.loads(row.optimizer_data) if row.optimizer_data else [],
                    "batch_id": row.batch_id
                }
        except Exception as e:
            print(f"查询1小时前快照失败: {e}")

        # 如果没有找到一小时前的快照，回退到查找最近一条（排除最近10分钟内的）
        ten_minutes_ago = now - timedelta(minutes=10)
        fallback_query = f"""
        SELECT *
        FROM `{table_ref}`
        WHERE snapshot_time < '{ten_minutes_ago.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY snapshot_time DESC
        LIMIT 1
        """

        try:
            for row in self.client.query(fallback_query).result():
                print(f"[快照] 未找到1小时前快照，使用最近快照: {row.snapshot_time}")
                return {
                    "snapshot_time": str(row.snapshot_time),
                    "stat_date": str(row.stat_date),
                    "hour": row.hour,
                    "total_spend": float(row.total_spend or 0),
                    "total_revenue": float(row.total_revenue or 0),
                    "d0_roas": float(row.d0_roas or 0),
                    "optimizer_data": json.loads(row.optimizer_data) if row.optimizer_data else [],
                    "batch_id": row.batch_id
                }
        except Exception as e:
            print(f"获取最近快照失败: {e}")

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
