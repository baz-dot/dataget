"""
测试 DataEye 爬虫的翻页功能
"""
import os
from dotenv import load_dotenv
from dataeye_scraper import DataEyeScraper

# 加载环境变量
load_dotenv()

def test_pagination():
    """测试翻页功能"""
    print("=" * 60)
    print("测试 DataEye 海外版翻页功能")
    print("=" * 60)

    # 从环境变量获取登录信息
    username = os.getenv('DATAEYE_USERNAME')
    password = os.getenv('DATAEYE_PASSWORD')

    if not username or not password:
        print("错误: 请在 .env 文件中配置 DATAEYE_USERNAME 和 DATAEYE_PASSWORD")
        return

    # 创建爬虫实例
    scraper = DataEyeScraper(
        username=username,
        password=password,
        headless=False,  # 使用有头模式，方便观察
        version='overseas',  # 海外版
        date_filter=None,  # 暂时不使用日期筛选，避免筛选问题
        max_records=150  # 限制获取 150 条记录（需要翻页 4-5 次，测试连续翻页）
    )

    try:
        # 启动浏览器
        scraper.start()

        # 登录
        print("\n步骤 1: 登录...")
        if not scraper.login():
            print("登录失败")
            return

        print("✓ 登录成功")

        # 提取数据（会自动翻页）
        print("\n步骤 2: 提取数据并测试翻页...")
        records = scraper.extract_data()

        if records:
            print(f"\n✓ 成功获取 {len(records)} 条记录")
            print(f"✓ 发现 {len(scraper.video_urls)} 个视频 URL")

            # 显示前 3 条记录的基本信息
            print("\n前 3 条记录预览:")
            for i, record in enumerate(records[:3], 1):
                print(f"\n记录 {i}:")
                print(f"  热度值: {record.get('heatNum', 'N/A')}")
                print(f"  首次发现: {record.get('firstSeen', 'N/A')}")
                print(f"  最后发现: {record.get('lastSeen', 'N/A')}")
        else:
            print("\n✗ 未能获取数据")

    except Exception as e:
        print(f"\n✗ 测试过程出错: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # 关闭浏览器
        print("\n步骤 3: 清理...")
        scraper.stop()
        print("✓ 测试完成")

if __name__ == '__main__':
    test_pagination()
