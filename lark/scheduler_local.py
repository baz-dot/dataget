"""
本地定时任务调度器
- 日报: 每日 09:00 执行
- 实时播报: 每日 09:00-24:00 每整点执行
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import schedule
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from lark.lark_bot import LarkBot, Daily_Job
from bigquery_storage import BigQueryUploader

# 配置
WEBHOOK_URL = os.getenv('LARK_WEBHOOK_URL')
SECRET = os.getenv('LARK_SECRET') or None
BI_LINK = os.getenv('DAILY_REPORT_BI_LINK', 'https://bi.aliyun.com/product/vigloo.htm?menuId=f438317d-6f93-4561-8fb2-e85bf2e9aea8')
PROJECT_ID = os.getenv('BQ_PROJECT_ID')
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
CHATGPT_API_KEY = os.getenv("OPENAI_API_KEY") or GEMINI_API_KEY

# BigQuery 实例
bq = BigQueryUploader(PROJECT_ID, "quickbi_data")


def run_daily_report():
    """执行日报任务 - 每日 09:00"""
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 执行日报任务")
    print('='*60)

    try:
        # 查询昨天的数据 (T-1)
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"查询 {yesterday} 的日报数据...")

        report_data = bq.query_daily_report_data(date=yesterday)

        summary = report_data.get('summary', {})
        print(f"  总消耗: ${summary.get('total_spend', 0):,.2f}")
        print(f"  ROAS: {summary.get('global_roas', 0):.2%}")

        # 发送日报
        result = Daily_Job(
            webhook_url=WEBHOOK_URL,
            secret=SECRET,
            data=report_data,
            bi_link=BI_LINK
        )

        if result.get('StatusCode') == 0 or result.get('code') == 0:
            print("[OK] 日报发送成功!")
        else:
            print(f"[FAIL] 日报发送失败: {result}")

    except Exception as e:
        print(f"[ERROR] 日报任务异常: {e}")


def run_realtime_report():
    """执行实时播报任务 - 每小时整点 (09:00-24:00)"""
    current_hour = datetime.now().hour

    # 只在 9:00-24:00 之间执行
    if current_hour < 9:
        print(f"[{datetime.now().strftime('%H:%M')}] 当前时间不在播报时段 (09:00-24:00)，跳过")
        return

    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 执行实时播报任务")
    print('='*60)

    try:
        # 查询当日实时数据
        print("查询当日实时数据...")
        realtime_data = bq.query_realtime_report_data()

        summary = realtime_data.get('summary', {})
        total_spend = summary.get('total_spend', 0)
        d0_roas = summary.get('d0_roas', 0)
        print(f"  总消耗: ${total_spend:,.2f}")
        print(f"  D0 ROAS: {d0_roas:.2%}")

        # ========== 数据校验 ==========
        bot = LarkBot(
            webhook_url=WEBHOOK_URL,
            secret=SECRET,
            gemini_api_key=GEMINI_API_KEY,
            chatgpt_api_key=CHATGPT_API_KEY
        )

        # 1. 检查消耗是否为0（可能API异常）
        if total_spend == 0:
            print("[校验失败] 总消耗为0，可能API异常")
            bot.send_alert(
                alert_type="数据异常",
                message="实时播报检测到总消耗为0，可能API Token过期或数据源异常",
                level="error"
            )
            return

        # 2. 检查数据时效性（batch时间不应落后当前时间超过70分钟）
        api_update_time = realtime_data.get('api_update_time', '')
        if api_update_time:
            try:
                batch_datetime = datetime.strptime(api_update_time, '%Y-%m-%d %H:%M:%S')
                time_diff_minutes = (datetime.now() - batch_datetime).total_seconds() / 60

                if time_diff_minutes > 70:
                    print(f"[校验失败] 数据延迟{time_diff_minutes:.0f}分钟: API更新时间={api_update_time}")
                    bot.send_alert(
                        alert_type="数据延迟",
                        message=f"QuickBI数据延迟{time_diff_minutes:.0f}分钟，API更新时间: {api_update_time}",
                        level="warning"
                    )
                    return
            except Exception as e:
                print(f"[校验警告] 无法解析API更新时间: {api_update_time}, {e}")

        # 3. 检查数据是否延迟超过2小时
        if realtime_data.get('data_delayed'):
            print(f"[校验失败] 数据延迟超过2小时，API更新时间: {realtime_data.get('api_update_time')}")
            bot.send_alert(
                alert_type="数据延迟",
                message=f"数据延迟超过2小时，API更新时间: {realtime_data.get('api_update_time')}",
                level="error"
            )
            return

        # 获取上一个batch数据（用于计算环比）
        prev_data = bq.get_previous_batch_data()
        if prev_data:
            prev_spend = prev_data.get('total_spend', 0)
            prev_roas = prev_data.get('d0_roas', 0)
            print(f"  上批次消耗: ${prev_spend:,.2f}")
            print(f"  上批次时间: {prev_data.get('batch_time', '未知')}")

            # 4. 检查数据是否有变化（当前消耗应该大于上批次）
            if total_spend <= prev_spend:
                print(f"[校验警告] 数据无变化或异常: 当前(${total_spend:,.2f}) <= 上批次(${prev_spend:,.2f})")
                print("  等待5分钟后重试...")
                import time
                time.sleep(300)  # 等待5分钟

                # 重新查询数据
                realtime_data = bq.query_realtime_report_data()
                summary = realtime_data.get('summary', {})
                new_spend = summary.get('total_spend', 0)

                if new_spend <= prev_spend:
                    print(f"[校验失败] 重试后数据仍无变化: ${new_spend:,.2f}")
                    bot.send_alert(
                        alert_type="数据异常",
                        message=f"数据无变化，当前消耗(${new_spend:,.2f}) <= 上批次(${prev_spend:,.2f})，请检查数据源",
                        level="warning"
                    )
                    return
                else:
                    print(f"[重试成功] 数据已更新: ${new_spend:,.2f}")
                    total_spend = new_spend
                    d0_roas = summary.get('d0_roas', 0)

            # 5. 检查单小时消耗增量是否异常（不应超过$50k）
            hourly_delta = total_spend - prev_spend
            if hourly_delta > 50000:
                print(f"[校验警告] 单小时消耗增量异常: ${hourly_delta:,.2f} > $50,000")
                bot.send_alert(
                    alert_type="数据异常",
                    message=f"单小时消耗增量异常高: ${hourly_delta:,.2f}，请核实数据",
                    level="warning"
                )

            # 6. 检查ROAS是否突变（变化超过50%）
            if prev_roas > 0:
                roas_change = abs(d0_roas - prev_roas) / prev_roas
                if roas_change > 0.5:
                    print(f"[校验警告] ROAS突变: {prev_roas:.1%} -> {d0_roas:.1%} (变化{roas_change:.0%})")
                    bot.send_alert(
                        alert_type="数据异常",
                        message=f"ROAS突变: {prev_roas:.1%} -> {d0_roas:.1%} (变化{roas_change:.0%})，请关注",
                        level="warning"
                    )

        # 7. 数据源头完整性校验
        # 7.1 检查必要字段是否存在
        required_fields = ['summary', 'optimizer_spend', 'batch_id']
        missing_fields = [f for f in required_fields if not realtime_data.get(f)]
        if missing_fields:
            print(f"[校验失败] 数据缺失关键字段: {missing_fields}")
            bot.send_alert(
                alert_type="数据异常",
                message=f"Quick BI返回数据不完整，缺失字段: {missing_fields}",
                level="error"
            )
            return

        # 7.2 检查投手数量是否正常
        optimizer_count = len(realtime_data.get('optimizer_spend', []))
        if optimizer_count == 0:
            print("[校验警告] 投手数据为空")
            bot.send_alert(
                alert_type="数据异常",
                message="投手数据为空，请检查数据源",
                level="error"
            )
            return
        elif optimizer_count < 3:
            print(f"[校验警告] 投手数量偏少: {optimizer_count}人")
            bot.send_alert(
                alert_type="数据异常",
                message=f"投手数量偏少: {optimizer_count}人，请确认是否正常",
                level="warning"
            )

        # 8. 同比校验 - 与昨日同时段对比
        yesterday_data = bq.query_yesterday_same_hour_data()
        if yesterday_data:
            yesterday_spend = yesterday_data.get('total_spend', 0)
            yesterday_optimizer_count = yesterday_data.get('optimizer_count', 0)
            yesterday_campaign_count = yesterday_data.get('campaign_count', 0)

            # 8.1 消耗同比波动检查 (波动超过50%告警)
            if yesterday_spend > 0:
                spend_change = (total_spend - yesterday_spend) / yesterday_spend
                if abs(spend_change) > 0.5:
                    direction = "上涨" if spend_change > 0 else "下跌"
                    print(f"[校验警告] 消耗同比{direction}{abs(spend_change):.0%}: 今日${total_spend:,.0f} vs 昨日${yesterday_spend:,.0f}")
                    bot.send_alert(
                        alert_type="同比异常",
                        message=f"消耗同比{direction}{abs(spend_change):.0%}，今日${total_spend:,.0f} vs 昨日同时段${yesterday_spend:,.0f}",
                        level="warning"
                    )

            # 8.2 投手数量同比检查
            if yesterday_optimizer_count > 0 and optimizer_count < yesterday_optimizer_count * 0.5:
                print(f"[校验警告] 投手数量同比减少: 今日{optimizer_count}人 vs 昨日{yesterday_optimizer_count}人")
                bot.send_alert(
                    alert_type="数据异常",
                    message=f"投手数量同比减少50%以上: 今日{optimizer_count}人 vs 昨日{yesterday_optimizer_count}人",
                    level="warning"
                )

        # 发送实时播报
        result = bot.send_realtime_report(data=realtime_data, prev_data=prev_data)

        if result.get('StatusCode') == 0 or result.get('code') == 0:
            print("[OK] 实时播报发送成功!")
        else:
            print(f"[FAIL] 实时播报发送失败: {result}")

    except Exception as e:
        print(f"[ERROR] 实时播报任务异常: {e}")


def run_all_now():
    """立即执行所有任务（测试用）"""
    print("\n" + "="*60)
    print("立即执行所有任务")
    print("="*60)

    run_daily_report()
    run_realtime_report()


def main():
    """主函数 - 启动定时调度"""
    print("="*60)
    print("飞书播报本地定时任务")
    print("="*60)
    print(f"Webhook: {WEBHOOK_URL[:50]}...")
    print(f"Project: {PROJECT_ID}")
    print()
    print("定时任务配置:")
    print("  - 日报: 每日 09:10")
    print("  - 实时播报: 每日 09:10-23:10 每小时10分")
    print()

    # 设置定时任务
    schedule.every().day.at("09:10").do(run_daily_report)

    # 每小时10分执行实时播报 (等待QuickBI数据同步)
    for hour in range(9, 24):
        schedule.every().day.at(f"{hour:02d}:10").do(run_realtime_report)

    print("定时任务已启动，按 Ctrl+C 停止")
    print("="*60)

    # 运行调度器
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='飞书播报本地定时任务')
    parser.add_argument('--now', action='store_true', help='立即执行一次所有任务')
    parser.add_argument('--daily', action='store_true', help='立即执行日报')
    parser.add_argument('--realtime', action='store_true', help='立即执行实时播报')
    args = parser.parse_args()

    if args.now:
        run_all_now()
    elif args.daily:
        run_daily_report()
    elif args.realtime:
        run_realtime_report()
    else:
        main()
