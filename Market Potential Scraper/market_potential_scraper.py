"""
市场潜力分析工具 - 主程序
评估海外市场短剧表现,生成投放优先级报告
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime
from typing import List, Dict

from dataeye_scraper import DataEyeScraper
from excel_generator import ExcelReportGenerator

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('market_potential_scraper.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


def load_dramas_from_file(file_path: str) -> List[str]:
    """
    从文件加载剧名列表

    支持格式:
    - .txt: 每行一个剧名
    - .json: {"dramas": [{"name": "剧名1"}, ...]}

    Args:
        file_path: 文件路径

    Returns:
        剧名列表
    """
    logger.info(f"从文件加载剧名: {file_path}")

    if not os.path.exists(file_path):
        logger.error(f"文件不存在: {file_path}")
        return []

    try:
        if file_path.endswith('.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                dramas = [d['name'] for d in data.get('dramas', [])]
                logger.info(f"✓ 从 JSON 加载 {len(dramas)} 部剧")
                return dramas

        elif file_path.endswith('.txt'):
            with open(file_path, 'r', encoding='utf-8') as f:
                dramas = [line.strip() for line in f if line.strip()]
                logger.info(f"✓ 从 TXT 加载 {len(dramas)} 部剧")
                return dramas

        else:
            logger.error(f"不支持的文件格式: {file_path}")
            return []

    except Exception as e:
        logger.error(f"加载文件失败: {e}")
        return []


def parse_drama_list(drama_string: str) -> List[str]:
    """
    解析逗号分隔的剧名字符串

    Args:
        drama_string: 逗号分隔的剧名,如 "剧1,剧2,剧3"

    Returns:
        剧名列表
    """
    dramas = [d.strip() for d in drama_string.split(',') if d.strip()]
    logger.info(f"解析剧名列表: {len(dramas)} 部剧")
    return dramas


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='市场潜力分析工具 - 评估海外市场短剧表现',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 使用配置文件
  python market_potential_scraper.py --input dramas.json --output report.xlsx

  # 直接指定剧名
  python market_potential_scraper.py --dramas "回家的诱惑,霸道总裁爱上我,重生之豪门千金"

  # 自定义时间窗口
  python market_potential_scraper.py --input dramas.json --recent-days 30 --lifecycle-days 730

  # 启用调试模式
  python market_potential_scraper.py --input dramas.json --debug
        """
    )

    parser.add_argument(
        '--input', '-i',
        type=str,
        help='输入文件路径 (.json 或 .txt 格式)'
    )

    parser.add_argument(
        '--dramas', '-d',
        type=str,
        help='逗号分隔的剧名列表,如 "剧1,剧2,剧3"'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        help='输出 Excel 文件路径 (默认: 9部剧市场潜力分析报告_时间戳.xlsx)'
    )

    parser.add_argument(
        '--recent-days',
        type=int,
        default=30,
        help='近期数据时间窗口 (天数,默认: 30)'
    )

    parser.add_argument(
        '--lifecycle-days',
        type=int,
        default=730,
        help='生命周期数据时间窗口 (天数,默认: 730 即2年)'
    )

    parser.add_argument(
        '--headless',
        action='store_true',
        help='使用无头模式运行浏览器'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用调试模式'
    )

    args = parser.parse_args()

    # 设置日志级别
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("调试模式已启用")

    # 获取剧名列表
    dramas = []
    if args.input:
        dramas = load_dramas_from_file(args.input)
    elif args.dramas:
        dramas = parse_drama_list(args.dramas)
    else:
        # 默认使用 dramas.json
        default_file = 'dramas.json'
        if os.path.exists(default_file):
            logger.info(f"未指定输入,使用默认配置文件: {default_file}")
            dramas = load_dramas_from_file(default_file)
        else:
            logger.error("错误: 请使用 --input 或 --dramas 参数指定剧名")
            parser.print_help()
            sys.exit(1)

    if not dramas:
        logger.error("错误: 未找到任何剧名")
        sys.exit(1)

    logger.info("="*60)
    logger.info("市场潜力分析工具")
    logger.info("="*60)
    logger.info(f"待分析剧目: {len(dramas)} 部")
    for i, drama in enumerate(dramas, 1):
        logger.info(f"  {i}. {drama}")
    logger.info("="*60)

    try:
        # 步骤 1: 启动爬虫并采集数据
        logger.info("\n步骤 1/3: 启动 DataEye 爬虫...")
        with DataEyeScraper(headless=args.headless) as scraper:
            # 登录
            logger.info("正在登录 DataEye 平台...")
            if not scraper.login():
                logger.error("登录失败,退出程序")
                sys.exit(1)

            # 搜索短剧数据
            logger.info(f"开始搜索 {len(dramas)} 部短剧的数据...")
            results = scraper.scrape_multiple_dramas(dramas)

            # 保存原始数据
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            raw_data_file = f"raw_data_{timestamp}.json"
            with open(raw_data_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"✓ 原始数据已保存: {raw_data_file}")

        logger.info("✓ 步骤 1/3 完成: 数据采集完成")

    except KeyboardInterrupt:
        logger.info("\n用户中断程序")
        sys.exit(0)
    except Exception as e:
        logger.error(f"\n数据采集失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 步骤 2: 生成 Excel 报告
    try:
        logger.info("\n步骤 2/3: 生成 Excel 报告...")
        generator = ExcelReportGenerator(output_path=args.output)
        report_path = generator.generate_report(results)
        logger.info("✓ 步骤 2/3 完成: Excel 报告生成完成")

    except Exception as e:
        logger.error(f"\n生成报告失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 步骤 3: 输出汇总信息
    logger.info("\n步骤 3/3: 输出汇总信息...")
    logger.info("="*60)
    logger.info("分析完成!")
    logger.info("="*60)
    logger.info(f"✓ 分析剧目数: {len(dramas)}")
    logger.info(f"✓ 原始数据文件: {raw_data_file}")
    logger.info(f"✓ Excel 报告: {report_path}")
    logger.info("="*60)


if __name__ == "__main__":
    main()
