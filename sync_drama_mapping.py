"""
自动同步 drama_mapping 表
从 QuickBI 和 Google Sheets 同步 drama_id → drama_name 到 BigQuery
支持：
- QuickBI → BigQuery drama_mapping 表（增量同步）
- Google Sheets → BigQuery quickbi_campaigns 表（回填空 drama_name）
"""
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(__file__))

try:
    from utils.logger import get_logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
else:
    logger = get_logger("dataget.sync_drama")

load_dotenv()


class DramaMappingSync:
    """Drama 映射同步器"""

    def __init__(self):
        self.project_id = os.getenv('BQ_PROJECT_ID', 'fleet-blend-469520-n7')
        self.client = bigquery.Client(project=self.project_id)
        self.quickbi_dataset = 'quickbi_data'
        self.xmp_dataset = 'xmp_data'

    def get_quickbi_dramas(self):
        """从 QuickBI 获取所有 drama_id 和 drama_name"""
        query = f"""
        SELECT DISTINCT
            drama_id,
            drama_name
        FROM `{self.project_id}.{self.quickbi_dataset}.quickbi_campaigns`
        WHERE drama_id IS NOT NULL
            AND drama_name IS NOT NULL
            AND drama_name != ''
        ORDER BY drama_id
        """

        logger.info("查询 QuickBI 中的 drama 映射...")
        results = self.client.query(query).result()

        dramas = {}
        for row in results:
            dramas[row.drama_id] = row.drama_name

        logger.info(f"从 QuickBI 获取到 {len(dramas)} 个 drama 映射")
        return dramas

    def get_existing_mappings(self):
        """获取 BigQuery 中已有的 drama 映射"""
        query = f"""
        SELECT
            drama_id,
            drama_name
        FROM `{self.project_id}.{self.xmp_dataset}.drama_mapping`
        ORDER BY drama_id
        """

        logger.info("查询 BigQuery 中已有的 drama 映射...")
        results = self.client.query(query).result()

        existing = {}
        for row in results:
            existing[row.drama_id] = row.drama_name

        logger.info(f"BigQuery 中已有 {len(existing)} 个 drama 映射")
        return existing

    def find_new_mappings(self, quickbi_dramas, existing_mappings):
        """找出需要新增的 drama 映射"""
        new_mappings = {}

        for drama_id, drama_name in quickbi_dramas.items():
            if drama_id not in existing_mappings:
                new_mappings[drama_id] = drama_name

        logger.info(f"发现 {len(new_mappings)} 个新的 drama 映射")
        return new_mappings

    def insert_new_mappings(self, new_mappings):
        """将新的 drama 映射插入到 BigQuery"""
        if not new_mappings:
            logger.info("没有新的映射需要插入")
            return 0

        table_ref = f"{self.project_id}.{self.xmp_dataset}.drama_mapping"
        success_count = 0
        fail_count = 0

        for drama_id, drama_name in new_mappings.items():
            insert_query = f"""
            INSERT INTO `{table_ref}` (drama_id, drama_name)
            VALUES (@drama_id, @drama_name)
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("drama_id", "STRING", drama_id),
                    bigquery.ScalarQueryParameter("drama_name", "STRING", drama_name)
                ]
            )

            try:
                self.client.query(insert_query, job_config=job_config).result()
                logger.info(f"✓ 插入成功: {drama_id} - {drama_name}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ 插入失败: {drama_id} - {drama_name}: {e}")
                fail_count += 1

        logger.info(f"插入完成: 成功 {success_count}, 失败 {fail_count}")
        return success_count

    def fetch_google_sheets_mapping(self):
        """从 Google Sheets 读取 drama_id → drama_name 映射"""
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            import google.auth
        except ImportError:
            logger.error("gspread 未安装，跳过 Google Sheets 同步")
            return {}

        sheets_url = os.getenv(
            'DRAMA_SHEETS_URL',
            'https://docs.google.com/spreadsheets/d/1nNz7RcHirkyp5gD-7p3stkV5vPks15YQMYbBZSyi21o/edit'
        )
        # EN sheet gid
        en_sheet_id = int(os.getenv('DRAMA_SHEETS_EN_GID', '852324189'))

        logger.info("从 Google Sheets 读取 drama 映射...")
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        # Cloud Run 环境用 ADC，本地用 service account 文件
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
        if not creds_path:
            creds_path = os.path.join(os.path.dirname(__file__), 'fleet-blend-469520-n7-1a29eac22376.json')

        if os.path.exists(creds_path):
            creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        else:
            logger.info("凭证文件不存在，使用 ADC 默认凭证")
            creds, _ = google.auth.default(scopes=scopes)

        gc = gspread.authorize(creds)

        sh = gc.open_by_url(sheets_url)
        ws = sh.get_worksheet_by_id(en_sheet_id)
        rows = ws.get_all_records()

        mapping = {}
        for r in rows:
            drama_id = str(r.get('id', '')).strip()
            title = str(r.get('title', '')).strip()
            if drama_id and title:
                mapping[drama_id] = title

        logger.info(f"从 Google Sheets 获取到 {len(mapping)} 个 drama 映射")

        # 同时更新本地 drama_mapping.json
        json_path = os.path.join(os.path.dirname(__file__), 'drama_mapping.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
        logger.info(f"已更新本地 {json_path}")

        return mapping

    def backfill_drama_names(self, sheets_mapping=None):
        """回填 BigQuery 中 drama_name 为空的记录"""
        if not sheets_mapping:
            # 尝试从本地 JSON 读取
            json_path = os.path.join(os.path.dirname(__file__), 'drama_mapping.json')
            if os.path.exists(json_path):
                with open(json_path, 'r', encoding='utf-8') as f:
                    sheets_mapping = json.load(f)
            else:
                logger.info("无映射数据，跳过回填")
                return 0

        # 查询 drama_name 为空但 drama_id 不为空的记录
        query = f"""
        SELECT DISTINCT drama_id
        FROM `{self.project_id}.{self.quickbi_dataset}.quickbi_campaigns`
        WHERE drama_id IS NOT NULL
          AND drama_id != ''
          AND (drama_name IS NULL OR drama_name = '' OR drama_name = '<nil>')
        """
        logger.info("查询 drama_name 为空的记录...")
        results = self.client.query(query).result()

        missing_ids = [row.drama_id for row in results]
        logger.info(f"发现 {len(missing_ids)} 个 drama_id 缺少 drama_name")

        if not missing_ids:
            return 0

        updated = 0
        for drama_id in missing_ids:
            drama_name = sheets_mapping.get(drama_id)
            if not drama_name:
                # sheets_mapping 的 value 可能是 dict 格式 {"name": ..., "programCode": ...}
                entry = sheets_mapping.get(drama_id)
                if isinstance(entry, dict):
                    drama_name = entry.get('name')
            if not drama_name:
                logger.info(f"  跳过 {drama_id}: 映射中无对应剧名")
                continue

            update_query = f"""
            UPDATE `{self.project_id}.{self.quickbi_dataset}.quickbi_campaigns`
            SET drama_name = @drama_name
            WHERE drama_id = @drama_id
              AND (drama_name IS NULL OR drama_name = '' OR drama_name = '<nil>')
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("drama_name", "STRING", drama_name),
                    bigquery.ScalarQueryParameter("drama_id", "STRING", drama_id),
                ]
            )
            try:
                job = self.client.query(update_query, job_config=job_config)
                job.result()
                logger.info(f"  ✓ 回填成功: {drama_id} → {drama_name} ({job.num_dml_affected_rows} 行)")
                updated += 1
            except Exception as e:
                logger.error(f"  ✗ 回填失败: {drama_id}: {e}")

        logger.info(f"回填完成: 更新 {updated} 个 drama_id")
        return updated

    def sync(self):
        """执行同步"""
        logger.info("=" * 80)
        logger.info(f"开始同步 drama_mapping - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)

        try:
            # 1. 获取 QuickBI 中的 drama 映射
            quickbi_dramas = self.get_quickbi_dramas()

            # 2. 获取 BigQuery 中已有的映射
            existing_mappings = self.get_existing_mappings()

            # 3. 找出新的映射
            new_mappings = self.find_new_mappings(quickbi_dramas, existing_mappings)

            # 4. 插入新的映射
            if new_mappings:
                logger.info(f"\n发现 {len(new_mappings)} 个新映射:")
                for drama_id, drama_name in sorted(new_mappings.items()):
                    logger.info(f"  {drama_id}: {drama_name}")

                success_count = self.insert_new_mappings(new_mappings)
                logger.info(f"\n同步完成: 成功插入 {success_count} 个新映射")
            else:
                logger.info("\n没有发现新的映射，无需同步")

            # 5. 从 Google Sheets 同步并回填空 drama_name
            logger.info("\n--- Google Sheets 同步 ---")
            sheets_mapping = self.fetch_google_sheets_mapping()
            if sheets_mapping:
                self.backfill_drama_names(sheets_mapping)

            logger.info("=" * 80)
            return True

        except Exception as e:
            logger.error(f"同步失败: {e}", exc_info=True)
            return False


def main():
    """主函数"""
    syncer = DramaMappingSync()
    success = syncer.sync()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

