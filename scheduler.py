"""
主调度脚本 (The Brain Scheduler)
每小时运行一次，整合数据采集、规则引擎、消息推送
支持：
- 每日 09:00 日报播报 (Daily Report)
- 每日 9:00-24:00 整点实时播报 (Real-time Report)
"""

import os
import sys
import time
import schedule
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 导入各模块
from rule_engine import RuleEngine, RuleConfig
from lark.lark_bot import LarkBot, OPTIMIZER_USER_MAP
from bigquery_storage import BigQueryUploader

# 日报配置
DAILY_REPORT_BI_LINK = os.getenv('DAILY_REPORT_BI_LINK', 'https://bi.aliyun.com/product/vigloo.htm?menuId=f438317d-6f93-4561-8fb2-e85bf2e9aea8&accounttraceid=ee0ec5d2837043b595c3c6a6df78b4b3lglk')


class BrainScheduler:
    """核心调度器"""

    def __init__(self):
        """初始化调度器"""
        # GCP 配置
        self.project_id = os.getenv('BQ_PROJECT_ID')
        self.dataset_id = os.getenv('BQ_DATASET_ID')

        # Lark 配置
        self.lark_webhook = os.getenv('LARK_WEBHOOK_URL')
        self.lark_secret = os.getenv('LARK_SECRET')

        # 验证配置
        self._validate_config()

        # 初始化组件
        self.rule_engine = RuleEngine(
            self.project_id,
            self.dataset_id,
            self._get_rule_config()
        )
        self.lark_bot = LarkBot(self.lark_webhook, self.lark_secret) if self.lark_webhook else None
        self.bq_uploader = BigQueryUploader(self.project_id, self.dataset_id)
        # Quick BI 数据专用 uploader（实时播报、快照都用这个）
        self.quickbi_uploader = BigQueryUploader(self.project_id, "quickbi_data")

        print(f"[Scheduler] 初始化完成")
        print(f"  - Project: {self.project_id}")
        print(f"  - Dataset: {self.dataset_id}")
        print(f"  - QuickBI Dataset: quickbi_data")
        print(f"  - Lark Bot: {'已配置' if self.lark_bot else '未配置'}")

    def _validate_config(self):
        """验证必要配置"""
        if not self.project_id or not self.dataset_id:
            print("错误: 请在 .env 文件中设置 BQ_PROJECT_ID 和 BQ_DATASET_ID")
            sys.exit(1)

    def _get_rule_config(self) -> RuleConfig:
        """从环境变量获取规则配置"""
        return RuleConfig(
            stop_loss_min_spend=float(os.getenv('RULE_STOP_LOSS_MIN_SPEND', '30')),
            stop_loss_max_roas=float(os.getenv('RULE_STOP_LOSS_MAX_ROAS', '0.10')),
            scale_up_min_roas=float(os.getenv('RULE_SCALE_UP_MIN_ROAS', '0.40')),
            scale_up_min_spend=float(os.getenv('RULE_SCALE_UP_MIN_SPEND', '50')),
            scale_up_target_cpi=float(os.getenv('RULE_SCALE_UP_TARGET_CPI', '2.0')),
            creative_refresh_ctr_drop=float(os.getenv('RULE_CREATIVE_CTR_DROP', '0.20')),
            creative_refresh_min_ctr=float(os.getenv('RULE_CREATIVE_MIN_CTR', '0.01')),
        )

    def run_analysis(self, date: str = None) -> dict:
        """
        运行一次完整分析

        Args:
            date: 分析日期，默认今天

        Returns:
            分析结果摘要
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始分析...")
        print(f"{'='*60}")

        result = {
            "date": date,
            "start_time": datetime.now().isoformat(),
            "signals": [],
            "summary": {},
            "lark_sent": False,
            "errors": []
        }

        try:
            # 1. 运行规则引擎
            print("\n[Step 1] 运行规则引擎...")
            signals = self.rule_engine.run(date)
            result["signals"] = [s.to_dict() for s in signals]
            result["summary"] = self.rule_engine.get_summary()

            print(f"  生成信号: {result['summary']['total']} 个")
            print(f"    - 止损: {result['summary']['stop_loss']}")
            print(f"    - 扩量: {result['summary']['scale_up']}")
            print(f"    - 素材优化: {result['summary']['creative_refresh']}")

            # 2. 发送 Lark 通知
            if self.lark_bot and signals:
                print("\n[Step 2] 发送 Lark 通知...")
                self._send_lark_notifications(signals)
                result["lark_sent"] = True
                print("  通知发送完成")
            elif not signals:
                print("\n[Step 2] 无信号，跳过通知")

        except Exception as e:
            error_msg = f"分析过程出错: {str(e)}"
            print(f"\n[Error] {error_msg}")
            result["errors"].append(error_msg)

            # 发送错误告警
            if self.lark_bot:
                self.lark_bot.send_alert(
                    alert_type="系统错误",
                    message=error_msg,
                    level="error"
                )

        result["end_time"] = datetime.now().isoformat()
        print(f"\n[完成] 分析结束")
        return result

    def _send_lark_notifications(self, signals):
        """发送 Lark 通知"""
        # 转换为字典格式
        signal_dicts = [s.to_dict() for s in signals]

        # 按优化师分组发送
        self.lark_bot.send_strategy_batch(signal_dicts, group_by_optimizer=True)

        # 如果有紧急止损信号，额外发送全员通知
        critical_signals = [s for s in signals if s.priority.value == 1]
        if critical_signals:
            total_spend = sum(s.metrics.get('spend', 0) for s in critical_signals)
            self.lark_bot.send_alert(
                alert_type="紧急止损",
                message=f"发现 {len(critical_signals)} 个需要立即关停的计划，涉及消耗 ${total_spend:.2f}",
                level="error",
                at_user_ids=list(OPTIMIZER_USER_MAP.values())[:5]  # @前5个优化师
            )

    def send_daily_report(self, date: str = None) -> dict:
        """
        发送日报播报 (每日 09:00 触发)

        Args:
            date: 报告日期，默认为昨天 (T-1)

        Returns:
            发送结果
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始生成日报...")
        print(f"{'='*60}")

        result = {
            "date": date,
            "success": False,
            "error": None
        }

        try:
            # 1. 查询日报数据
            print(f"[Step 1] 查询 {date} 的日报数据...")
            report_data = self.bq_uploader.query_daily_report_data(date)

            if not report_data.get("summary"):
                print(f"  警告: {date} 无数据")
                result["error"] = "无数据"
                return result

            print(f"  总消耗: ${report_data['summary'].get('total_spend', 0):,.2f}")
            print(f"  综合 ROAS: {report_data['summary'].get('global_roas', 0):.1%}")
            print(f"  投手数: {len(report_data.get('optimizers', []))}")

            # 2. 发送日报
            if self.lark_bot:
                print(f"\n[Step 2] 发送日报到飞书...")
                send_result = self.lark_bot.send_daily_report(
                    report_data,
                    bi_link=DAILY_REPORT_BI_LINK
                )
                print(f"  发送结果: {send_result}")
                result["success"] = send_result.get("code") == 0 or send_result.get("StatusCode") == 0
            else:
                print(f"\n[Step 2] Lark Bot 未配置，跳过发送")

        except Exception as e:
            error_msg = f"日报生成失败: {str(e)}"
            print(f"\n[Error] {error_msg}")
            result["error"] = error_msg

            # 发送错误告警
            if self.lark_bot:
                self.lark_bot.send_alert(
                    alert_type="日报生成失败",
                    message=error_msg,
                    level="error"
                )

        print(f"\n[完成] 日报发送结束")
        return result

    def send_realtime_report(self) -> dict:
        """
        发送实时播报 (每日 9:00-24:00 整点触发)

        目标：为执行层提供"每小时"的监控，发现异动，即时调整
        播报群：vigloo投放剪辑群 + 个人推送

        Returns:
            发送结果
        """
        current_hour = datetime.now().hour

        # 检查是否在播报时间范围内 (9:00 - 24:00)
        if current_hour < 9:
            print(f"[Realtime] 当前时间 {current_hour}:00，不在播报时间范围 (9:00-24:00)")
            return {"success": False, "reason": "outside_broadcast_hours"}

        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始生成实时播报...")
        print(f"{'='*60}")

        result = {
            "hour": current_hour,
            "success": False,
            "error": None
        }

        try:
            # 1. 查询当前实时数据
            print(f"[Step 1] 查询当日实时数据...")
            realtime_data = self.quickbi_uploader.query_realtime_report_data()

            # 检查数据同步状态
            if not realtime_data.get("summary") or not realtime_data.get("batch_id"):
                print(f"  警告: 当日无数据，可能数据同步失败")
                result["error"] = "无数据"

                # 发送数据同步失败告警
                if self.lark_bot:
                    self.lark_bot.send_alert(
                        alert_type="数据同步异常",
                        message=f"实时播报查询不到当日数据，请检查 Quick BI 数据同步任务是否正常运行。",
                        level="error"
                    )
                return result

            # 检查数据延迟
            if realtime_data.get("data_delayed"):
                print(f"  警告: 数据延迟超过2小时")
                if self.lark_bot:
                    self.lark_bot.send_alert(
                        alert_type="数据延迟警告",
                        message=f"Quick BI 数据更新时间超过2小时，最后更新: {realtime_data.get('api_update_time', '未知')}",
                        level="warning"
                    )

            print(f"  当前总消耗: ${realtime_data['summary'].get('total_spend', 0):,.2f}")
            print(f"  当前 D0 ROAS: {realtime_data['summary'].get('d0_roas', 0):.1%}")
            print(f"  数据延迟: {'是' if realtime_data.get('data_delayed') else '否'}")

            # 2. 获取上次快照 (用于计算环比)
            print(f"\n[Step 2] 获取上次快照...")
            prev_snapshot = self.quickbi_uploader.get_previous_hour_snapshot()
            if prev_snapshot:
                print(f"  上次快照时间: {prev_snapshot.get('snapshot_time')}")
                print(f"  上次消耗: ${prev_snapshot.get('total_spend', 0):,.2f}")
            else:
                print(f"  无历史快照，首次播报")

            # 3. 发送实时播报 (带环比)
            if self.lark_bot:
                print(f"\n[Step 3] 发送实时播报到飞书...")
                send_result = self.lark_bot.send_realtime_report(realtime_data, prev_snapshot)
                print(f"  发送结果: {send_result}")
                result["success"] = send_result.get("code") == 0 or send_result.get("StatusCode") == 0

                # 4. 播报成功后保存当前快照
                if result["success"]:
                    print(f"\n[Step 4] 保存当前快照...")
                    self.quickbi_uploader.save_hourly_snapshot(realtime_data)
            else:
                print(f"\n[Step 3] Lark Bot 未配置，跳过发送")

        except Exception as e:
            error_msg = f"实时播报生成失败: {str(e)}"
            print(f"\n[Error] {error_msg}")
            result["error"] = error_msg

            # 发送错误告警
            if self.lark_bot:
                self.lark_bot.send_alert(
                    alert_type="实时播报失败",
                    message=error_msg,
                    level="error"
                )

        print(f"\n[完成] 实时播报发送结束")
        return result

    def start_scheduler(self, interval_minutes: int = 60):
        """
        启动定时调度

        Args:
            interval_minutes: 执行间隔（分钟），默认60分钟
        """
        print(f"\n[Scheduler] 启动定时任务，间隔 {interval_minutes} 分钟")

        # 立即执行一次策略分析
        self.run_analysis()

        # 设置定时任务
        schedule.every(interval_minutes).minutes.do(self.run_analysis)

        # 设置每日 09:00 日报播报
        schedule.every().day.at("09:00").do(self.send_daily_report)
        print(f"[Scheduler] 已设置每日 09:00 日报播报")

        # 设置每日 9:00-24:00 整点实时播报 (每小时整点触发)
        for hour in range(9, 24):
            schedule.every().day.at(f"{hour:02d}:30").do(self.send_realtime_report)
        print(f"[Scheduler] 已设置每日 9:30-23:30 实时播报 (每小时整点后30分触发)")

        # 持续运行
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次

    def run_once(self, date: str = None):
        """单次运行（用于测试或手动触发）"""
        return self.run_analysis(date)


# ============ Cloud Run 入口 ============

def cloud_run_handler():
    """Cloud Run Job 入口"""
    print("[Cloud Run] 启动分析任务...")
    scheduler = BrainScheduler()
    result = scheduler.run_once()

    # 输出结果摘要
    print(f"\n[Cloud Run] 任务完成")
    print(f"  信号总数: {result['summary'].get('total', 0)}")
    print(f"  错误数: {len(result['errors'])}")

    return result


# ============ 命令行入口 ============

def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description='广告策略分析调度器')
    parser.add_argument('--mode', choices=['once', 'schedule', 'cloud'],
                        default='once', help='运行模式')
    parser.add_argument('--date', type=str, default=None,
                        help='分析日期 (YYYY-MM-DD)')
    parser.add_argument('--interval', type=int, default=60,
                        help='调度间隔（分钟）')

    args = parser.parse_args()

    scheduler = BrainScheduler()

    if args.mode == 'once':
        # 单次运行
        result = scheduler.run_once(args.date)
        print(f"\n结果摘要: {result['summary']}")

    elif args.mode == 'schedule':
        # 定时运行
        scheduler.start_scheduler(args.interval)

    elif args.mode == 'cloud':
        # Cloud Run 模式
        cloud_run_handler()


if __name__ == "__main__":
    # 检测运行环境
    if os.getenv('K_SERVICE') or os.getenv('CLOUD_RUN_JOB') or os.getenv('CLOUD_RUN'):
        # Cloud Run 环境
        cloud_run_handler()
    else:
        # 本地环境
        main()
