"""
测试 ChatGPT Advisor 智能分析功能
"""

import os
import sys

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载 .env 文件
from dotenv import load_dotenv
load_dotenv()

from lark.chatgpt_advisor import ChatGPTAdvisor, create_chatgpt_advisor


def get_test_daily_data():
    """获取测试用的日报数据"""
    return {
        "date": "2024-12-22",
        "summary": {
            "total_spend": 15000,
            "total_revenue": 6000,
            "global_roas": 0.40
        },
        "summary_prev": {
            "total_spend": 14000,
            "total_revenue": 5600,
            "global_roas": 0.40
        },
        "optimizers": [
            {"name": "张三", "spend": 5000, "roas": 0.45},
            {"name": "李四", "spend": 4000, "roas": 0.38},
            {"name": "王五", "spend": 3500, "roas": 0.42},
            {"name": "赵六", "spend": 2500, "roas": 0.28},
        ],
        "dramas": [
            {"name": "霸道总裁爱上我", "spend": 4000, "roas": 0.52},
            {"name": "重生之都市修仙", "spend": 3500, "roas": 0.35},
            {"name": "甜蜜婚约", "spend": 3000, "roas": 0.48},
            {"name": "逆袭人生", "spend": 2500, "roas": 0.25},
            {"name": "豪门千金", "spend": 2000, "roas": 0.41},
        ],
        "countries": [
            {"name": "US", "spend": 6000, "roas": 0.42},
            {"name": "TW", "spend": 4000, "roas": 0.45},
            {"name": "MY", "spend": 2500, "roas": 0.38},
            {"name": "TH", "spend": 1500, "roas": 0.55},
            {"name": "PH", "spend": 1000, "roas": 0.32},
        ]
    }


def test_daily_analysis():
    """测试日报分析"""
    print("=" * 50)
    print("测试日报智能分析")
    print("=" * 50)

    advisor = create_chatgpt_advisor()
    if not advisor:
        print("ChatGPT Advisor 初始化失败，请检查 API Key")
        return

    data = get_test_daily_data()
    print(f"\n输入数据概览:")
    print(f"- 日期: {data['date']}")
    print(f"- 总消耗: ${data['summary']['total_spend']:,}")
    print(f"- ROAS: {data['summary']['global_roas']:.1%}")

    print("\n正在调用 ChatGPT 分析...")
    result = advisor.analyze_daily_data(data)

    print("\n分析结果:")
    print("-" * 40)
    print(f"趋势分析: {result.get('trend_analysis', 'N/A')}")
    print(f"\n异常点:")
    for a in result.get('anomalies', []):
        print(f"  - {a}")
    print(f"\n机会:")
    for o in result.get('opportunities', []):
        print(f"  - {o}")
    print(f"\n风险:")
    for r in result.get('risks', []):
        print(f"  - {r}")
    print(f"\n核心洞察: {result.get('key_insights', 'N/A')}")


if __name__ == "__main__":
    test_daily_analysis()
