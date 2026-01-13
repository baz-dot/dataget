"""
DataEye 爬虫测试脚本 - 简化版
用于测试爬虫功能，支持手动登录
"""

import os
import sys
import json
import time
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 导入爬虫类
from dataeye_scraper import DataEyeScraper, logger


def test_single_drama():
    """测试单个短剧搜索"""

    # 测试剧名
    test_drama = "回家的诱惑"

    logger.info("="*60)
    logger.info("DataEye 爬虫测试 - 单剧搜索")
    logger.info("="*60)
    logger.info(f"测试剧集: {test_drama}")
    logger.info("")

    try:
        # 创建爬虫实例（非无头模式，可以看到浏览器）
        scraper = DataEyeScraper(headless=False)
        scraper.start()

        # 登录
        logger.info("步骤 1: 登录 DataEye 平台")
        logger.info("-" * 60)

        # 检查是否有 Cookie
        if os.path.exists('dataeye_cookies.json'):
            logger.info("发现 Cookie 文件，尝试自动登录...")
            if scraper.load_cookies():
                scraper.page.goto(scraper.target_url, timeout=60000)
                time.sleep(3)

                if scraper._check_login_status():
                    logger.info("✓ Cookie 登录成功！")
                else:
                    logger.warning("Cookie 已失效")
                    logger.info("请在浏览器中手动登录...")
                    logger.info("登录完成后，在终端按回车继续...")
                    input()
                    scraper.save_cookies()
            else:
                logger.info("请在浏览器中手动登录...")
                scraper.page.goto(scraper.target_url, timeout=60000)
                logger.info("登录完成后，在终端按回车继续...")
                input()
                scraper.save_cookies()
        else:
            logger.info("首次运行，请在浏览器中手动登录...")
            scraper.page.goto(scraper.target_url, timeout=60000)
            logger.info("登录完成后，在终端按回车继续...")
            input()
            scraper.save_cookies()

        logger.info("")
        logger.info("步骤 2: 搜索短剧数据")
        logger.info("-" * 60)

        # 搜索短剧
        result = scraper.search_drama(test_drama)

        # 保存结果
        output_file = f"test_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        logger.info("")
        logger.info("="*60)
        logger.info("测试完成！")
        logger.info(f"✓ 结果已保存到: {output_file}")
        logger.info(f"✓ 找到 {len(result.get('results', []))} 条数据")

        if result.get('screenshot'):
            logger.info(f"✓ 截图已保存: {result['screenshot']}")

        logger.info("="*60)

        # 关闭浏览器
        logger.info("\n按回车关闭浏览器...")
        input()
        scraper.close()

    except KeyboardInterrupt:
        logger.info("\n用户中断测试")
        if 'scraper' in locals():
            scraper.close()
    except Exception as e:
        logger.error(f"\n测试出错: {e}")
        import traceback
        traceback.print_exc()
        if 'scraper' in locals():
            scraper.close()


if __name__ == "__main__":
    test_single_drama()
