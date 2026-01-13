"""
手动登录 DataEye 并保存 Cookie
用于首次登录或 Cookie 失效时
"""

import os
import time
from dotenv import load_dotenv
from dataeye_scraper import DataEyeScraper, logger

# 加载环境变量
load_dotenv()


def save_cookies_manually():
    """手动登录并保存 Cookie"""

    logger.info("="*60)
    logger.info("DataEye Cookie 保存工具")
    logger.info("="*60)
    logger.info("此工具将打开浏览器，请手动登录 DataEye 平台")
    logger.info("登录成功后，Cookie 将自动保存到 dataeye_cookies.json")
    logger.info("")

    try:
        # 创建爬虫实例（非无头模式）
        scraper = DataEyeScraper(headless=False)
        scraper.start()

        # 访问目标页面
        logger.info("正在打开 DataEye 平台...")
        scraper.page.goto(scraper.target_url, timeout=60000)
        time.sleep(3)

        # 检查是否已经登录
        if scraper._check_login_status():
            logger.info("✓ 检测到已登录状态")
            scraper.save_cookies()
            logger.info("✓ Cookie 已保存！")
            logger.info("")
            logger.info("现在可以关闭浏览器，按回车退出...")
            input()
            scraper.close()
            return True

        # 未登录，等待用户手动登录
        logger.info("")
        logger.info("请在浏览器中完成以下操作：")
        logger.info("  1. 输入账号密码")
        logger.info("  2. 完成登录")
        logger.info("  3. 确认页面加载完成")
        logger.info("")
        logger.info("登录完成后，在终端按回车继续...")
        input()

        # 再次检查登录状态
        if scraper._check_login_status():
            logger.info("✓ 登录成功！")
            scraper.save_cookies()
            logger.info("✓ Cookie 已保存到: dataeye_cookies.json")
            logger.info("")
            logger.info("现在可以运行爬虫程序了：")
            logger.info("  python dataeye_scraper.py")
            logger.info("  或")
            logger.info("  python test_pagination.py")
        else:
            logger.error("✗ 未检测到登录状态，请重试")

        logger.info("")
        logger.info("按回车关闭浏览器...")
        input()
        scraper.close()

    except KeyboardInterrupt:
        logger.info("\n用户中断")
        if 'scraper' in locals():
            scraper.close()
    except Exception as e:
        logger.error(f"\n出错: {e}")
        import traceback
        traceback.print_exc()
        if 'scraper' in locals():
            scraper.close()


if __name__ == "__main__":
    save_cookies_manually()
