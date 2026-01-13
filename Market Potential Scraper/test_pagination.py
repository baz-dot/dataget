"""
测试翻页功能 - 重新采集数据不完整的剧目
"""
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 导入爬虫类
from dataeye_scraper import DataEyeScraper, logger


def test_pagination():
    """测试翻页功能，重新采集数据不完整的剧目"""

    # 需要重新采集的剧目（数据不完整的）
    incomplete_dramas = [
        "天降萌宝老祖，孝子贤孙都跪下",  # 488条，只采集了40条
        "带崽嫁入豪门",                  # 395条，只采集了40条
        "断手医圣",                      # 116条，只采集了40条
        "离婚！本小姐爱得起放得下",      # 75条，只采集了40条
        "我是元婴期！四个姐姐瞧不起我",  # 64条，只采集了40条
    ]

    logger.info("="*60)
    logger.info("测试翻页功能 - 重新采集数据不完整的剧目")
    logger.info("="*60)
    logger.info(f"待采集剧目: {len(incomplete_dramas)} 部")
    for i, drama in enumerate(incomplete_dramas, 1):
        logger.info(f"  {i}. {drama}")
    logger.info("")

    try:
        # 使用 with 语句自动管理浏览器生命周期
        with DataEyeScraper(headless=False) as scraper:
            # 登录
            logger.info("步骤 1: 登录 DataEye 平台")
            logger.info("-" * 60)

            if not scraper.login():
                logger.error("登录失败，退出测试")
                return

            logger.info("")
            logger.info("步骤 2: 开始采集数据")
            logger.info("-" * 60)

            # 采集每部剧
            results = []
            for i, drama_name in enumerate(incomplete_dramas, 1):
                logger.info(f"\n[{i}/{len(incomplete_dramas)}] 正在采集: {drama_name}")
                logger.info("=" * 60)

                result = scraper.search_drama(drama_name)
                results.append(result)

                if result.get('success'):
                    window_2y = result.get('window_2y', {})
                    window_30d = result.get('window_30d', {})
                    logger.info(f"✓ {drama_name} 采集完成")
                    logger.info(f"  - 近2年: {window_2y.get('creative_count', 0)} 条素材")
                    logger.info(f"  - 近30天: {window_30d.get('creative_count', 0)} 条素材")
                else:
                    logger.error(f"✗ {drama_name} 采集失败: {result.get('error')}")

                # 等待一下，避免请求过快
                if i < len(incomplete_dramas):
                    logger.info("\n等待 3 秒后继续...")
                    time.sleep(3)

            logger.info("")
            logger.info("="*60)
            logger.info("采集完成！")
            logger.info(f"✓ 成功: {sum(1 for r in results if r.get('success'))} 部")
            logger.info(f"✗ 失败: {sum(1 for r in results if not r.get('success'))} 部")
            logger.info("="*60)

    except KeyboardInterrupt:
        logger.info("\n用户中断测试")
    except Exception as e:
        logger.error(f"\n测试出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_pagination()
