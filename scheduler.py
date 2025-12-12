"""
定时任务调度器
支持 XMP 数据采集、QuickBI API 调用等定时任务
"""

import os
import sys
import time
import schedule
from datetime import datetime
from dotenv import load_dotenv

# 设置控制台编码
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')


def run_xmp_scraper():
    """运行 XMP 数据采集任务"""
    print(f"\n{'='*50}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始 XMP 数据采集...")
    print('='*50)

    try:
        from scraper import XMPScraper

        load_dotenv()
        username = os.getenv('XMP_USERNAME')
        password = os.getenv('XMP_PASSWORD')
        gcs_bucket = os.getenv('GCS_BUCKET_NAME')

        if not username or not password:
            print("错误: 缺少 XMP 登录凭证")
            return

        scraper = XMPScraper(
            username=username,
            password=password,
            headless=True  # 定时任务使用无头模式
        )

        scraper.run(
            save_format='csv',
            upload_to_gcs=bool(gcs_bucket),
            gcs_bucket=gcs_bucket
        )

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] XMP 数据采集完成")

    except Exception as e:
        print(f"✗ XMP 数据采集失败: {e}")


def run_quickbi_sync():
    """运行 QuickBI API 同步任务（待实现）"""
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] QuickBI 同步任务（待实现）")
    # TODO: 实现 QuickBI API 调用


def run_adx_scraper():
    """运行 ADX 数据采集任务（待实现）"""
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ADX 数据采集任务（待实现）")
    # TODO: 实现 ADX 数据采集


def setup_schedules():
    """设置定时任务"""
    # XMP 素材数据：每6小时执行一次
    schedule.every(6).hours.do(run_xmp_scraper)

    # QuickBI API：每1小时执行一次
    schedule.every(1).hours.do(run_quickbi_sync)

    # ADX 素材数据：每6小时执行一次
    schedule.every(6).hours.do(run_adx_scraper)

    print("定时任务已设置:")
    print("  - XMP 素材数据采集: 每 6 小时")
    print("  - QuickBI API 同步: 每 1 小时")
    print("  - ADX 素材数据采集: 每 6 小时")


def main():
    """主函数"""
    load_dotenv()

    print("="*50)
    print("数据采集定时调度器")
    print("="*50)

    # 设置定时任务
    setup_schedules()

    # 立即执行一次 XMP 采集
    print("\n首次运行 XMP 数据采集...")
    run_xmp_scraper()

    # 开始调度循环
    print("\n开始定时调度循环...")
    print("按 Ctrl+C 停止\n")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    except KeyboardInterrupt:
        print("\n调度器已停止")


if __name__ == "__main__":
    main()
