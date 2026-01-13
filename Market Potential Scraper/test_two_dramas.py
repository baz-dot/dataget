"""
测试脚本 - 只测试2部剧，验证搜索是否正确
"""
import logging
from dataeye_scraper import DataEyeScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # 测试2部剧
    test_dramas = [
        "穿过荆棘拥抱你",
        "他不渡我"
    ]

    logger.info("="*60)
    logger.info("测试搜索功能 - 2部剧")
    logger.info("="*60)

    try:
        with DataEyeScraper(headless=False) as scraper:
            # 登录
            if not scraper.login():
                logger.error("登录失败")
                return

            # 搜索剧集
            results = scraper.scrape_multiple_dramas(test_dramas)

            # 输出结果摘要
            logger.info("\n" + "="*60)
            logger.info("测试结果摘要")
            logger.info("="*60)
            for result in results:
                drama_name = result.get('drama_name', 'Unknown')
                success = result.get('success', False)

                if success:
                    window_2y = result.get('window_2y', {})
                    window_30d = result.get('window_30d', {})

                    logger.info(f"\n剧名: {drama_name}")
                    logger.info(f"  - 2年素材数: {window_2y.get('creative_count', 0)}")
                    logger.info(f"  - 2年曝光量: {window_2y.get('total_exposure', 0):,}")
                    logger.info(f"  - 30天素材数: {window_30d.get('creative_count', 0)}")
                    logger.info(f"  - 30天曝光量: {window_30d.get('total_exposure', 0):,}")
                else:
                    logger.error(f"\n剧名: {drama_name} - 失败")

    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
