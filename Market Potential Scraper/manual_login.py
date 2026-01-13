"""
手动登录脚本 - 打开浏览器让用户手动登录并保存 Cookie
"""
import logging
import time
from dataeye_scraper import DataEyeScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("="*60)
    logger.info("DataEye 手动登录工具")
    logger.info("="*60)
    logger.info("程序将打开浏览器，请手动登录 DataEye 平台")
    logger.info("登录成功后，程序会自动保存 Cookie（等待60秒）")
    logger.info("="*60)

    try:
        scraper = DataEyeScraper(headless=False)
        scraper.start()

        # 访问登录页面
        logger.info("正在打开 DataEye 平台...")
        scraper.page.goto('https://oversea-v2.dataeye.com/playlet/playlet-material', timeout=60000)

        # 等待用户手动登录（60秒）
        logger.info("请在浏览器中手动登录...")
        logger.info("等待 60 秒后自动保存 Cookie...")

        for i in range(60, 0, -10):
            logger.info(f"剩余 {i} 秒...")
            time.sleep(10)

        # 保存 Cookie
        logger.info("正在保存 Cookie...")
        scraper.save_cookies()
        logger.info("✓ Cookie 已保存到 dataeye_cookies.json")
        logger.info("✓ 现在可以运行主程序了")

        # 关闭浏览器
        scraper.close()

    except KeyboardInterrupt:
        logger.info("\n用户中断程序")
    except Exception as e:
        logger.error(f"\n程序出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
