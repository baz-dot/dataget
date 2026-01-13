"""
DataEye 爬虫测试脚本 - 自动等待版本
不需要手动按回车，自动等待一段时间让用户登录
"""

import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 导入爬虫类
from dataeye_scraper import DataEyeScraper, logger


def test_with_auto_wait():
    """测试爬虫 - 自动等待版本"""

    # 测试剧名
    test_drama = "回家的诱惑"

    logger.info("="*60)
    logger.info("DataEye 爬虫测试 - 自动等待版本")
    logger.info("="*60)
    logger.info(f"测试剧集: {test_drama}")
    logger.info("")

    scraper = None
    try:
        # 创建爬虫实例
        scraper = DataEyeScraper(headless=False)
        scraper.start()

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
                    logger.warning("Cookie 已失效，需要手动登录")
                    logger.info("浏览器已打开，请在 60 秒内完成登录...")

                    # 等待 60 秒让用户登录
                    for i in range(60, 0, -10):
                        logger.info(f"剩余时间: {i} 秒...")
                        time.sleep(10)

                    # 检查是否登录成功
                    if scraper._check_login_status():
                        logger.info("✓ 手动登录成功！")
                        scraper.save_cookies()
                    else:
                        logger.error("✗ 登录超时或失败")
                        return
            else:
                logger.info("Cookie 加载失败，需要手动登录")
                scraper.page.goto(scraper.target_url, timeout=60000)
                logger.info("浏览器已打开，请在 60 秒内完成登录...")

                for i in range(60, 0, -10):
                    logger.info(f"剩余时间: {i} 秒...")
                    time.sleep(10)

                if scraper._check_login_status():
                    logger.info("✓ 手动登录成功！")
                    scraper.save_cookies()
                else:
                    logger.error("✗ 登录超时或失败")
                    return
        else:
            logger.info("首次运行，需要手动登录")
            scraper.page.goto(scraper.target_url, timeout=60000)
            logger.info("浏览器已打开，请在 60 秒内完成登录...")

            for i in range(60, 0, -10):
                logger.info(f"剩余时间: {i} 秒...")
                time.sleep(10)

            if scraper._check_login_status():
                logger.info("✓ 手动登录成功！")
                scraper.save_cookies()
            else:
                logger.error("✗ 登录超时或失败")
                return

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

        # 等待 10 秒后自动关闭
        logger.info("\n10 秒后自动关闭浏览器...")
        time.sleep(10)

    except KeyboardInterrupt:
        logger.info("\n用户中断测试")
    except Exception as e:
        logger.error(f"\n测试出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    test_with_auto_wait()
