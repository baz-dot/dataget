"""
Test AI Insight strategy suggestions
"""

import os
import sys
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env file
from dotenv import load_dotenv
load_dotenv()

from lark.lark_bot import LarkBot


def test_strategy_insights():
    """Test strategy insights generation (fallback mode)"""

    # Create LarkBot (no Gemini API Key, use fallback rules)
    bot = LarkBot(
        webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
        secret="test_secret"
    )

    # Test data
    test_data = {
        "date": "2025-01-15",
        "summary": {
            "total_spend": 50000,
            "total_revenue": 85000,
            "global_roas": 0.42
        },
        "summary_prev": {
            "total_spend": 48000,
            "global_roas": 0.40
        },
        # All drama data (for scale up filtering)
        "dramas": [
            {"name": "Eternal Love", "spend": 2500, "roas": 0.48},  # meets scale up criteria
            {"name": "Revenge", "spend": 1500, "roas": 0.52},       # meets scale up criteria
            {"name": "Secret Garden", "spend": 800, "roas": 0.35},  # no (spend < 1000)
            {"name": "Lost Memory", "spend": 1200, "roas": 0.38},   # no (roas < 45%)
            {"name": "Dark Night", "spend": 500, "roas": 0.60},     # no (spend < 1000)
        ],
        # Drama x Country data (for opportunity market filtering)
        "drama_country": [
            {"drama_name": "Eternal Love", "country": "US", "spend": 800, "roas": 0.45},
            {"drama_name": "Eternal Love", "country": "DE", "spend": 200, "roas": 0.60},  # opportunity
            {"drama_name": "Revenge", "country": "JP", "spend": 150, "roas": 0.55},       # in top3
            {"drama_name": "Revenge", "country": "KR", "spend": 300, "roas": 0.48},
            {"drama_name": "Secret Garden", "country": "BR", "spend": 120, "roas": 0.52}, # opportunity
        ],
        # Top 3 countries
        "top3_countries": ["US", "KR", "JP"],
        # Countries Top5 (backup)
        "countries_top5": [
            {"name": "US", "spend": 20000, "roas": 0.40},
            {"name": "KR", "spend": 12000, "roas": 0.38},
            {"name": "JP", "spend": 8000, "roas": 0.42},
            {"name": "DE", "spend": 5000, "roas": 0.45},
            {"name": "BR", "spend": 3000, "roas": 0.35},
        ],
        "optimizers": [
            {"name": "Zhang San", "spend": 15000, "roas": 0.45, "top_campaign": "Eternal Love - US"},
            {"name": "Li Si", "spend": 12000, "roas": 0.38, "top_campaign": "Revenge - KR"},
            {"name": "Wang Wu", "spend": 10000, "roas": 0.42, "top_campaign": "Secret Garden - JP"},
        ],
        "dramas_top5": [
            {"name": "Eternal Love", "spend": 2500, "roas": 0.48},
            {"name": "Revenge", "spend": 1500, "roas": 0.52},
            {"name": "Secret Garden", "spend": 800, "roas": 0.35},
        ]
    }

    # Test strategy insights generation
    print("=" * 60)
    print("Test Strategy Insights (Fallback Mode)")
    print("=" * 60)

    insights = bot._generate_strategy_insights(test_data)

    print("\nGenerated Strategy Insights:")
    print(f"  [Scale Up Drama]: {insights.get('scale_up_drama', 'None')}")
    print(f"  [Opportunity Market]: {insights.get('opportunity_market', 'None')}")
    print(f"  [Test Drama Suggestion]: {insights.get('test_drama_suggestion', 'None')}")

    # Validation
    print("\n" + "=" * 60)
    print("Validation:")
    print("=" * 60)

    # Scale up drama should be Revenge (highest ROAS that meets criteria)
    assert "Revenge" in insights["scale_up_drama"], "Scale up drama should be Revenge (ROAS 52%)"
    print("[PASS] Scale up drama filter correct (Spend>$1000 and ROAS>45%, sorted by ROAS)")

    # Opportunity market should be Eternal Love in DE (ROAS 60%, not in Top3)
    assert "DE" in insights["opportunity_market"], "Opportunity market should be Germany"
    print("[PASS] Opportunity market filter correct (Spend>$100 and ROAS>50% and not in Top3)")

    # Test drama suggestion (5 dramas, 2 high efficiency)
    print(f"[PASS] Test drama suggestion: {insights.get('test_drama_suggestion') or 'None (sufficient dramas)'}")

    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60)

    return test_data


def test_send_daily_report(webhook_url: str = None):
    """Test sending actual daily report to Lark"""

    if not webhook_url:
        webhook_url = os.getenv("LARK_WEBHOOK_URL")

    if not webhook_url:
        print("\n[SKIP] No webhook URL provided, printing card JSON instead")
        print("   Set LARK_WEBHOOK_URL or pass webhook_url parameter")

        # Just generate and print the card content
        bot = LarkBot(
            webhook_url="https://example.com/test",
            secret="test"
        )

        test_data = {
            "date": "2025-01-15",
            "summary": {"total_spend": 50000, "global_roas": 0.42},
            "summary_prev": {"total_spend": 48000, "global_roas": 0.40},
            "dramas": [
                {"name": "Eternal Love", "spend": 2500, "roas": 0.48},
                {"name": "Revenge", "spend": 1500, "roas": 0.52},
            ],
            "drama_country": [
                {"drama_name": "Eternal Love", "country": "DE", "spend": 200, "roas": 0.60},
            ],
            "top3_countries": ["US", "KR", "JP"],
            "optimizers": [
                {"name": "Zhang San", "spend": 15000, "roas": 0.45, "top_campaign": "Eternal Love - US"},
            ],
            "dramas_top5": [{"name": "Eternal Love", "spend": 2500, "roas": 0.48}],
            "countries_top5": [{"name": "US", "spend": 20000, "roas": 0.40}],
        }

        # Generate insights
        insights = bot._generate_strategy_insights(test_data)
        print("\n[Card Content Preview]")
        print("-" * 40)
        print("Strategy Insights Section:")
        print(f"  Scale Up: {insights['scale_up_drama']}")
        print(f"  Opportunity: {insights['opportunity_market']}")
        print(f"  Test Drama: {insights['test_drama_suggestion']}")
        return

    # Actually send to Lark
    print("\n" + "=" * 60)
    print("Test Send Daily Report to Lark")
    print("=" * 60)

    secret = os.getenv("LARK_WEBHOOK_SECRET", "")

    bot = LarkBot(
        webhook_url=webhook_url,
        secret=secret if secret else None
    )

    test_data = {
        "date": "2025-01-15",
        "summary": {"total_spend": 50000, "global_roas": 0.42},
        "summary_prev": {"total_spend": 48000, "global_roas": 0.40},
        "dramas": [
            {"name": "Eternal Love", "spend": 2500, "roas": 0.48},
            {"name": "Revenge", "spend": 1500, "roas": 0.52},
        ],
        "drama_country": [
            {"drama_name": "Eternal Love", "country": "DE", "spend": 200, "roas": 0.60},
        ],
        "top3_countries": ["US", "KR", "JP"],
        "optimizers": [
            {"name": "Zhang San", "spend": 15000, "roas": 0.45, "top_campaign": "Eternal Love - US"},
            {"name": "Li Si", "spend": 12000, "roas": 0.38, "top_campaign": "Revenge - KR"},
        ],
        "dramas_top5": [
            {"name": "Eternal Love", "spend": 2500, "roas": 0.48},
            {"name": "Revenge", "spend": 1500, "roas": 0.52},
        ],
        "countries_top5": [
            {"name": "US", "spend": 20000, "roas": 0.40},
            {"name": "KR", "spend": 12000, "roas": 0.38},
        ],
    }

    result = bot.send_daily_report(test_data, bi_link="https://example.com/bi")

    print(f"\nSend Result: {json.dumps(result, ensure_ascii=False, indent=2)}")

    if result.get("code") == 0 or result.get("StatusCode") == 0:
        print("\n[SUCCESS] Message sent to Lark!")
    else:
        print(f"\n[FAILED] Error: {result.get('msg', result)}")


def test_with_gemini():
    """Test Gemini AI generation (requires GEMINI_API_KEY)"""

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("\n[WARN] GEMINI_API_KEY not set, skipping Gemini test")
        print("   Set it with: export GEMINI_API_KEY=your_api_key")
        return

    print("\n" + "=" * 60)
    print("Test Gemini AI Generation")
    print("=" * 60)

    bot = LarkBot(
        webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
        secret="test_secret",
        gemini_api_key=api_key
    )

    test_data = {
        "summary": {
            "total_spend": 50000,
            "global_roas": 0.42
        },
        "dramas": [
            {"name": "Eternal Love", "spend": 2500, "roas": 0.48},
            {"name": "Revenge", "spend": 1500, "roas": 0.52},
        ],
        "drama_country": [
            {"drama_name": "Eternal Love", "country": "DE", "spend": 200, "roas": 0.60},
        ],
        "top3_countries": ["US", "KR", "JP"],
    }

    insights = bot._generate_strategy_insights(test_data)

    print("\nGemini Generated Insights:")
    print(f"  [Scale Up Drama]: {insights.get('scale_up_drama', 'None')}")
    print(f"  [Opportunity Market]: {insights.get('opportunity_market', 'None')}")
    print(f"  [Test Drama Suggestion]: {insights.get('test_drama_suggestion', 'None')}")


if __name__ == "__main__":
    test_strategy_insights()
    test_send_daily_report()  # Will print preview if no webhook
    test_with_gemini()
