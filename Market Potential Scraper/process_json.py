#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSON 文件处理工具 - 直接处理 DataEye API 的 JSON 文件
"""
import json
import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from manual_data_processor import ManualDataProcessor
from excel_generator import ExcelReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_single_json(json_path: str, drama_name: str = None):
    """处理单个 JSON 文件"""
    logger.info(f"正在处理: {json_path}")

    try:
        # 读取 JSON 文件
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 如果没有指定剧名，从文件名提取
        if drama_name is None:
            drama_name = Path(json_path).stem

        # 提取素材列表
        creatives = []
        if 'content' in data and 'searchList' in data['content']:
            creatives = data['content']['searchList']

        logger.info(f"  ✓ 找到 {len(creatives)} 个素材")

        return drama_name, creatives

    except Exception as e:
        logger.error(f"处理文件失败: {e}")
        return None, []


def process_multiple_jsons(json_files: list):
    """处理多个 JSON 文件并生成报告"""
    logger.info("=" * 60)
    logger.info("开始批量处理 JSON 文件")
    logger.info("=" * 60)

    processor = ManualDataProcessor()
    all_drama_results = {}

    # 处理每个文件
    for json_file in json_files:
        drama_name, creatives = process_single_json(json_file)

        if drama_name and creatives:
            # 聚合数据
            aggregated = processor.aggregate_drama_data(creatives)
            all_drama_results[drama_name] = aggregated
            logger.info(f"  ✓ {drama_name}: 数据聚合完成")

    if not all_drama_results:
        logger.warning("没有成功处理任何数据")
        return

    # 转换数据格式为 List[Dict]
    results_list = []
    for drama_name, aggregated_data in all_drama_results.items():
        results_list.append({
            "success": True,
            "drama_name": drama_name,
            "aggregated": aggregated_data
        })

    # 生成 Excel 报告
    output_file = f"市场潜力分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    logger.info(f"\n正在生成报告: {output_file}")

    generator = ExcelReportGenerator(output_file)
    generator.generate_report(results_list)

    logger.info(f"✓ 报告已生成: {output_file}")
    logger.info("=" * 60)


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("JSON 文件处理工具")
    logger.info("=" * 60)

    # 检查命令行参数
    if len(sys.argv) < 2:
        logger.info("使用方法:")
        logger.info("  1. 处理单个文件:")
        logger.info("     python process_json.py 文件路径.json")
        logger.info("")
        logger.info("  2. 处理多个文件:")
        logger.info("     python process_json.py 文件1.json 文件2.json 文件3.json")
        logger.info("")
        logger.info("  3. 处理整个目录:")
        logger.info("     python process_json.py captured_data/")
        logger.info("")
        return

    # 收集所有 JSON 文件
    json_files = []

    for arg in sys.argv[1:]:
        path = Path(arg)

        if path.is_file() and path.suffix == '.json':
            json_files.append(str(path))
        elif path.is_dir():
            # 目录：查找所有 JSON 文件
            json_files.extend([str(f) for f in path.glob('*.json')])

    if not json_files:
        logger.error("未找到任何 JSON 文件")
        return

    logger.info(f"找到 {len(json_files)} 个 JSON 文件")

    # 处理文件
    process_multiple_jsons(json_files)


if __name__ == "__main__":
    main()
