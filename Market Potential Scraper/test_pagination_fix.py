"""
测试翻页功能修复
只测试一部剧，验证翻页是否正常工作
"""
import os
import sys
import logging
from dotenv import load_dotenv
from dataeye_scraper import DataEyeScraper

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_single_drama():
    """测试单个短剧的翻页功能"""

    # 测试剧目
    test_drama = "天降萌宝老祖，孝子贤孙都跪下"

    logger.info("="*60)
    logger.info("测试翻页功能修复")
    logger.info("="*60)
    logger.info(f"测试剧目: {test_drama}")
    logger.info("="*60)

    try:
        with DataEyeScraper(headless=False) as scraper:
            # 登录
            logger.info("\n步骤 1: 登录...")
            if not scraper.login():
                logger.error("登录失败")
                return False

            logger.info("✓ 登录成功")

            # 搜索短剧
            logger.info(f"\n步骤 2: 搜索短剧并测试翻页...")
            result = scraper.search_drama(test_drama)

            # 检查结果
            if result.get('success'):
                data_2y = result.get('data_2y', {})
                materials = data_2y.get('content', {}).get('searchList', [])

                logger.info("\n" + "="*60)
                logger.info("测试结果")
                logger.info("="*60)
                logger.info(f"✓ 搜索成功")
                logger.info(f"✓ 获取记录数: {len(materials)} 条")
                logger.info(f"✓ 预期记录数: ~488 条（13页）")

                if len(materials) > 40:
                    logger.info(f"✓✓ 翻页成功！获取了多页数据")
                    return True
                else:
                    logger.warning(f"⚠ 只获取了第一页数据，翻页可能失败")
                    return False
            else:
                logger.error(f"✗ 搜索失败: {result.get('error')}")
                return False

    except Exception as e:
        logger.error(f"测试过程出错: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_single_drama()
    sys.exit(0 if success else 1)
