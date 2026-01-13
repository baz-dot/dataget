"""
测试 Excel 报告生成器
使用模拟数据测试报告生成功能
"""

import json
import logging
from excel_generator import ExcelReportGenerator

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_mock_data():
    """创建模拟数据用于测试"""
    mock_results = [
        {
            "success": True,
            "drama_name": "回家的诱惑",
            "total_records": 150,
            "aggregated": {
                "window_30d": {
                    "total_exposure": 5000000,
                    "total_downloads": 50000,
                    "creative_count": 25,
                    "countries": ["US", "UK", "CA"],
                    "platforms": ["Facebook", "Google Ads"],
                    "publishers": ["ReelShort", "DramaBox"]
                },
                "window_2y": {
                    "total_exposure": 80000000,
                    "total_downloads": 800000,
                    "creative_count": 120,
                    "countries": ["US", "UK", "CA", "AU", "DE"],
                    "platforms": ["Facebook", "Google Ads", "TikTok"],
                    "publishers": ["ReelShort", "DramaBox", "FlexTV"],
                    "first_seen": "2023-06-15",
                    "last_seen": "2025-12-20",
                    "lifecycle_days": 918,
                    "active_days_count": 450
                }
            }
        },
        {
            "success": True,
            "drama_name": "霸道总裁爱上我",
            "total_records": 80,
            "aggregated": {
                "window_30d": {
                    "total_exposure": 15000000,
                    "total_downloads": 150000,
                    "creative_count": 45,
                    "countries": ["US", "UK"],
                    "platforms": ["Facebook"],
                    "publishers": ["ReelShort"]
                },
                "window_2y": {
                    "total_exposure": 120000000,
                    "total_downloads": 1200000,
                    "creative_count": 200,
                    "countries": ["US", "UK", "CA"],
                    "platforms": ["Facebook", "Google Ads"],
                    "publishers": ["ReelShort", "DramaBox"],
                    "first_seen": "2024-01-10",
                    "last_seen": "2025-12-22",
                    "lifecycle_days": 712,
                    "active_days_count": 380
                }
            }
        },
        {
            "success": True,
            "drama_name": "重生之豪门千金",
            "total_records": 5,
            "aggregated": {
                "window_30d": {
                    "total_exposure": 500000,
                    "total_downloads": 5000,
                    "creative_count": 3,
                    "countries": ["US"],
                    "platforms": ["Facebook"],
                    "publishers": []
                },
                "window_2y": {
                    "total_exposure": 800000,
                    "total_downloads": 8000,
                    "creative_count": 5,
                    "countries": ["US"],
                    "platforms": ["Facebook"],
                    "publishers": [],
                    "first_seen": "2025-11-01",
                    "last_seen": "2025-11-10",
                    "lifecycle_days": 9,
                    "active_days_count": 5
                }
            }
        }
    ]

    return mock_results


def main():
    """测试主函数"""
    logger.info("="*60)
    logger.info("测试 Excel 报告生成器")
    logger.info("="*60)

    # 创建模拟数据
    logger.info("创建模拟数据...")
    mock_data = create_mock_data()
    logger.info(f"✓ 创建了 {len(mock_data)} 部剧的模拟数据")

    # 生成报告
    try:
        logger.info("\n开始生成 Excel 报告...")
        generator = ExcelReportGenerator(output_path="测试报告.xlsx")
        report_path = generator.generate_report(mock_data)

        logger.info("\n" + "="*60)
        logger.info("✓ 测试成功!")
        logger.info(f"✓ 报告已生成: {report_path}")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
