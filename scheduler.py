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
from lark.personal_assistant import PersonalAssistant
from bigquery_storage import BigQueryUploader
from config.data_source import get_data_source_config

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

        # 从配置获取数据源
        data_source_config = get_data_source_config()
        self.data_source_dataset = data_source_config["dataset_id"]
        self.data_source_table = data_source_config["table_id"]

        # 初始化组件
        self.rule_engine = RuleEngine(
            self.project_id,
            self.data_source_dataset,
            self._get_rule_config()
        )
        self.lark_bot = LarkBot(self.lark_webhook, self.lark_secret) if self.lark_webhook else None
        self.bq_uploader = BigQueryUploader(self.project_id, self.data_source_dataset)
        self.quickbi_uploader = BigQueryUploader(self.project_id, self.data_source_dataset)

        print(f"[Scheduler] 初始化完成")
        print(f"  - Project: {self.project_id}")
        print(f"  - Dataset: {self.data_source_dataset}")
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
            # 1. 运行规则引擎 (已屏蔽)
            # print("\n[Step 1] 运行规则引擎...")
            # signals = self.rule_engine.run(date)
            # result["signals"] = [s.to_dict() for s in signals]
            # result["summary"] = self.rule_engine.get_summary()

            # print(f"  生成信号: {result['summary']['total']} 个")
            # print(f"    - 止损: {result['summary']['stop_loss']}")
            # print(f"    - 扩量: {result['summary']['scale_up']}")
            # print(f"    - 素材优化: {result['summary']['creative_refresh']}")

            # # 2. 发送 Lark 通知
            # if self.lark_bot and signals:
            #     print("\n[Step 2] 发送 Lark 通知...")
            #     self._send_lark_notifications(signals)
            #     result["lark_sent"] = True
            #     print("  通知发送完成")
            # elif not signals:
            #     print("\n[Step 2] 无信号，跳过通知")

            print("\n[Step 1] 规则引擎已屏蔽，跳过策略信号汇总")

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

    def send_daily_report(self, date: str = None, use_latest_batch: bool = False) -> dict:
        """
        发送日报播报 (每日 09:00 触发)

        Args:
            date: 报告日期，默认为昨天 (T-1)
            use_latest_batch: 是否强制使用最新 batch（而非整点 batch）

        Returns:
            发送结果
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始生成日报...")
        if use_latest_batch:
            print(f"  (使用最新 batch)")
        print(f"{'='*60}")

        result = {
            "date": date,
            "success": False,
            "error": None
        }

        try:
            # 1. 查询日报数据
            print(f"[Step 1] 查询 {date} 的日报数据...")
            report_data = self.bq_uploader.query_daily_report_data(date, use_latest_batch=use_latest_batch)

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

    def send_realtime_report(self, use_latest_batch: bool = False) -> dict:
        """
        发送实时播报 (08:00-24:00)

        目标：为执行层提供实时监控，发现异动，即时调整
        播报群：vigloo投放剪辑群 + 个人推送

        注意：凌晨 0-8 点跳过播报，因为 XMP API 在此时段返回的是昨天的数据

        Args:
            use_latest_batch: 是否使用绝对最新 batch（默认 False，使用整点 batch）

        Returns:
            发送结果
        """
        current_hour = datetime.now().hour

        # 凌晨 0-8 点跳过实时播报（XMP 数据在此时段为 T-1 日数据）
        if current_hour < 8:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 凌晨 {current_hour}:00，跳过实时播报（XMP 当日数据尚未更新）")
            return {
                "hour": current_hour,
                "success": True,
                "error": None,
                "skipped": True,
                "reason": "凌晨时段跳过"
            }

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
            print(f"[Step 1] 查询当日实时数据... (use_latest_batch={use_latest_batch})")
            realtime_data = self.quickbi_uploader.query_realtime_report_data(use_latest_batch=use_latest_batch)

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
                print(f"  警告: 数据延迟超过1小时")
                if self.lark_bot:
                    self.lark_bot.send_alert(
                        alert_type="数据延迟警告",
                        message=f"Quick BI 数据更新时间超过1小时，最后更新: {realtime_data.get('api_update_time', '未知')}",
                        level="warning"
                    )

            print(f"  当前总消耗: ${realtime_data['summary'].get('total_spend', 0):,.2f}")
            print(f"  当前 Media ROAS: {realtime_data['summary'].get('media_roas', 0):.1%}")
            print(f"  数据延迟: {'是' if realtime_data.get('data_delayed') else '否'}")

            # 2. 发送实时播报
            if self.lark_bot:
                print(f"\n[Step 2] 发送实时播报到飞书...")
                send_result = self.lark_bot.send_realtime_report(realtime_data, None)
                print(f"  发送结果: {send_result}")
                result["success"] = send_result.get("code") == 0 or send_result.get("StatusCode") == 0
            else:
                print(f"\n[Step 2] Lark Bot 未配置，跳过发送")

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

    def send_personal_reports(self) -> dict:
        """
        发送个人播报 (每小时15分触发，在实时播报之后)

        Returns:
            发送结果
        """
        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始发送个人播报...")
        print(f"{'='*60}")

        result = {"success": False, "results": {}}

        try:
            assistant = PersonalAssistant(self.project_id)
            results = assistant.send_to_all_optimizers()

            for optimizer, res in results.items():
                if res.get("error"):
                    print(f"  [{optimizer}] 错误: {res['error']}")
                else:
                    status_list = []
                    for key, val in res.items():
                        status = val.get('code', val.get('StatusCode', -1))
                        status_list.append(f"{key}:{'OK' if status == 0 else 'FAIL'}")
                    print(f"  [{optimizer}] {', '.join(status_list)}")

            result["results"] = results
            result["success"] = True
            print("[OK] 个人播报发送完成!")

        except Exception as e:
            error_msg = f"个人播报发送失败: {str(e)}"
            print(f"\n[Error] {error_msg}")
            result["error"] = error_msg

        return result

    def send_weekly_report(self) -> dict:
        """
        发送周报播报 (每周一 09:30 触发)

        Returns:
            发送结果
        """
        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始生成周报...")
        print(f"{'='*60}")

        result = {"success": False, "error": None}

        try:
            # 1. 查询周报数据
            print(f"[Step 1] 查询上周数据...")
            report_data = self.bq_uploader.query_weekly_report_data()

            week_start = report_data.get('week_start', '')
            week_end = report_data.get('week_end', '')
            summary = report_data.get('summary', {})

            print(f"  周期: {week_start} ~ {week_end}")
            print(f"  周总消耗: ${summary.get('week_total_spend', 0):,.2f}")
            print(f"  周均 ROAS: {summary.get('week_avg_roas', 0):.1%}")
            print(f"  投手数: {len(report_data.get('optimizer_weekly', []))}")

            # 2. 发送周报
            if self.lark_bot:
                print(f"\n[Step 2] 发送周报到飞书...")
                send_result = self.lark_bot.send_weekly_report(data=report_data)
                print(f"  发送结果: {send_result}")
                result["success"] = send_result.get("code") == 0 or send_result.get("StatusCode") == 0
            else:
                print(f"\n[Step 2] Lark Bot 未配置，跳过发送")

            # 3. 写入飞书文档
            print(f"\n[Step 3] 写入周报到飞书文档...")
            try:
                from lark.lark_doc_client import create_doc_client
                doc_client = create_doc_client()
                WIKI_TOKEN = "DEDKwkkMliSFyVku2LHlWWC5gvq"
                node_info = doc_client.get_wiki_node_info(WIKI_TOKEN)
                if node_info.get('code') == 0:
                    doc_token = node_info['data']['node']['obj_token']
                    doc_result = doc_client.write_standard_weekly_report(doc_token, report_data)
                    print(f"  文档写入: {'成功' if doc_result.get('code') == 0 else '失败'}")
                else:
                    print(f"  获取文档失败: {node_info.get('msg')}")
            except Exception as doc_e:
                print(f"  文档写入异常: {doc_e}")

        except Exception as e:
            error_msg = f"周报生成失败: {str(e)}"
            print(f"\n[Error] {error_msg}")
            result["error"] = error_msg

        print(f"\n[完成] 周报发送结束")
        return result

    def start_scheduler(self, interval_minutes: int = 60, skip_first_daily: bool = False, use_latest_batch: bool = False):
        """
        启动定时调度

        Args:
            interval_minutes: 执行间隔（分钟），默认60分钟
            skip_first_daily: 跳过第一次日报和周报
            use_latest_batch: 使用最新 batch
        """
        print(f"\n[Scheduler] 启动定时任务，间隔 {interval_minutes} 分钟")
        if skip_first_daily:
            print(f"[Scheduler] 跳过第一次日报和周报")

        # 立即执行一次策略分析（如果不跳过）
        if not skip_first_daily:
            self.run_analysis()

        # 立即执行一次实时播报
        print(f"\n[Scheduler] 立即执行第一次实时播报...")
        self.send_realtime_report(use_latest_batch=use_latest_batch)

        # 设置定时任务
        schedule.every(interval_minutes).minutes.do(self.run_analysis)

        # 设置每日 09:00 日报播报
        schedule.every().day.at("09:00").do(self.send_daily_report)
        print(f"[Scheduler] 已设置每日 09:00 日报播报")

        # 设置每日 8:00-24:00 整点实时播报 (每小时10分触发，等待QuickBI数据同步)
        for hour in range(8, 24):
            schedule.every().day.at(f"{hour:02d}:10").do(self.send_realtime_report)
        print(f"[Scheduler] 已设置每日 8:10-23:10 实时播报 (每小时10分触发)")

        # 设置每日 9:15-23:15 个人播报 (每小时15分触发，在实时播报之后)
        for hour in range(9, 24):
            schedule.every().day.at(f"{hour:02d}:15").do(self.send_personal_reports)
        print(f"[Scheduler] 已设置每日 9:15-23:15 个人播报 (每小时15分触发)")

        # 设置每周一 09:30 周报
        schedule.every().monday.at("09:30").do(self.send_weekly_report)
        print(f"[Scheduler] 已设置每周一 09:30 周报")

        # 设置每日 02:00 XMP 投手/剪辑师统计同步 (统计前一天数据)
        schedule.every().day.at("02:00").do(self.sync_xmp_stats)
        print(f"[Scheduler] 已设置每日 02:00 XMP 统计同步")

        # 持续运行
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次

    def start_realtime_scheduler(self, interval_seconds: int = 30):
        """
        启动实时播报调度器（高频模式）

        Args:
            interval_seconds: 执行间隔（秒），默认30秒
        """
        print(f"\n[Scheduler] 启动实时播报调度器，间隔 {interval_seconds} 秒")

        # 立即执行一次实时播报
        print(f"[Scheduler] 立即执行第一次实时播报...")
        self.send_realtime_report()

        # 设置定时任务
        schedule.every(interval_seconds).seconds.do(self.send_realtime_report)
        print(f"[Scheduler] 已设置每 {interval_seconds} 秒发送实时播报")

        # 持续运行
        print(f"[Scheduler] 调度器运行中，按 Ctrl+C 停止...")
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)  # 每秒检查一次
        except KeyboardInterrupt:
            print(f"\n[Scheduler] 收到停止信号，正在退出...")
            sys.exit(0)

    def run_once(self, date: str = None):
        """单次运行（用于测试或手动触发）"""
        return self.run_analysis(date)

    def sync_xmp_stats(self, date: str = None) -> dict:
        """
        同步 XMP 投手/剪辑师统计数据到 BigQuery

        Args:
            date: 统计日期，默认今天

        Returns:
            同步结果
        """
        import asyncio

        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 同步 XMP 统计数据...")
        print(f"{'='*60}")

        result = {
            "date": date,
            "success": False,
            "optimizer_count": 0,
            "editor_count": 0,
            "error": None
        }

        try:
            from xmp.xmp_scheduler import run_with_stats

            # 运行异步函数
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                data = loop.run_until_complete(run_with_stats(date, upload_bq=True))
            finally:
                loop.close()

            result["optimizer_count"] = len(data.get("optimizer_stats", []))
            result["editor_count"] = len(data.get("editor_stats", []))
            result["success"] = True

            print(f"[XMP] 同步完成: {result['optimizer_count']} 投手, {result['editor_count']} 剪辑师")

        except Exception as e:
            error_msg = f"XMP 统计同步失败: {str(e)}"
            print(f"[Error] {error_msg}")
            result["error"] = error_msg

            if self.lark_bot:
                self.lark_bot.send_alert(
                    alert_type="XMP 统计同步失败",
                    message=error_msg,
                    level="error"
                )

        return result


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
    parser.add_argument('--mode', choices=['once', 'schedule', 'cloud', 'realtime', 'personal', 'daily', 'weekly', 'xmp-stats'],
                        default='once', help='运行模式: once=单次运行, schedule=定时调度, cloud=Cloud Run, realtime=实时播报, personal=个人播报, daily=日报, weekly=周报, xmp-stats=XMP统计同步')
    parser.add_argument('--date', type=str, default=None,
                        help='分析日期 (YYYY-MM-DD)')
    parser.add_argument('--interval', type=int, default=60,
                        help='调度间隔（分钟，用于schedule模式）')
    parser.add_argument('--seconds', type=int, default=30,
                        help='实时播报间隔（秒，用于realtime模式）')
    parser.add_argument('--latest', '-l', action='store_true',
                        help='使用最新 batch（而非整点 batch）')
    parser.add_argument('--webhook', type=str, default=None,
                        help='指定飞书 Webhook URL（覆盖环境变量）')
    parser.add_argument('--skip-first-daily', action='store_true',
                        help='跳过第一次日报和周报（仅发实时播报）')

    args = parser.parse_args()

    # 如果指定了 webhook，临时覆盖环境变量
    if args.webhook:
        os.environ['LARK_WEBHOOK_URL'] = args.webhook
        print(f"[Config] 使用指定的 Webhook: {args.webhook[:50]}...")

    scheduler = BrainScheduler()

    if args.mode == 'once':
        # 单次运行
        result = scheduler.run_once(args.date)
        print(f"\n结果摘要: {result['summary']}")

    elif args.mode == 'schedule':
        # 定时运行
        scheduler.start_scheduler(args.interval, skip_first_daily=args.skip_first_daily, use_latest_batch=args.latest)

    elif args.mode == 'realtime':
        # 实时播报调度（如果指定 --latest，则单次运行）
        if args.latest:
            scheduler.send_realtime_report(use_latest_batch=True)
        else:
            scheduler.start_realtime_scheduler(args.seconds)

    elif args.mode == 'personal':
        # 个人播报
        scheduler.send_personal_reports()

    elif args.mode == 'daily':
        # 日报播报
        scheduler.send_daily_report(args.date, use_latest_batch=args.latest)

    elif args.mode == 'weekly':
        # 周报播报
        scheduler.send_weekly_report()

    elif args.mode == 'xmp-stats':
        # XMP 投手/剪辑师统计同步
        result = scheduler.sync_xmp_stats(args.date)
        print(f"\n结果: 投手 {result['optimizer_count']} 人, 剪辑师 {result['editor_count']} 人")

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
