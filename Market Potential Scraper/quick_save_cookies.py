"""
简化版 Cookie 保存工具 - 手动登录并保存 Cookie
"""
import time
from dataeye_scraper import DataEyeScraper, logger

logger.info("="*60)
logger.info("DataEye Cookie 保存工具")
logger.info("="*60)
logger.info("浏览器将打开，请手动登录 DataEye 平台")
logger.info("")

scraper = DataEyeScraper(headless=False)
scraper.start()

# 访问目标页面
logger.info("正在打开 DataEye 平台...")
scraper.page.goto(scraper.target_url, timeout=60000)
time.sleep(3)

# 检查是否已登录
if scraper._check_login_status():
    logger.info("✓ 检测到已登录状态")
    scraper.save_cookies()
    logger.info("✓ Cookie 已保存到: dataeye_cookies.json")
    logger.info("")
    logger.info("现在可以运行爬虫了！")
else:
    logger.info("请在浏览器中完成登录...")
    logger.info("程序将每 5 秒检查一次登录状态（最多等待 60 秒）")
    logger.info("")

    # 循环检查登录状态，最多等待 60 秒
    max_attempts = 12  # 12 次 x 5 秒 = 60 秒
    for attempt in range(1, max_attempts + 1):
        logger.info(f"检查登录状态... ({attempt}/{max_attempts})")
        time.sleep(5)

        if scraper._check_login_status():
            logger.info("✓ 检测到登录成功！")
            scraper.save_cookies()
            logger.info("✓ Cookie 已保存到: dataeye_cookies.json")
            logger.info("")
            logger.info("现在可以运行爬虫了：")
            logger.info("  python dataeye_scraper.py")
            logger.info("  或")
            logger.info("  python test_pagination.py")
            break
    else:
        logger.error("✗ 超时：未检测到登录状态")
        logger.error("请确保已完成登录，然后重新运行此脚本")

logger.info("")
logger.info("浏览器将在 5 秒后关闭...")
time.sleep(5)
scraper.close()
logger.info("完成！")
