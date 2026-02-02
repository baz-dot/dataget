"""
自动同步 drama_mapping 表
从 QuickBI 同步新的 drama_id 和 drama_name 到 BigQuery
"""
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

