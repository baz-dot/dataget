"""
KST 统一调度 单元测试 (不访问网络 / BigQuery / 飞书, 不依赖 pytest)

用法:
    python tests/test_kst_schedule_unit.py
"""

import json
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('BQ_PROJECT_ID', 'test-project')

import schedule

import scheduler as sched_mod
from scheduler import BrainScheduler, kst_to_local, is_realtime_quiet_hour
from bigquery_storage import BigQueryUploader
from lark import lark_bot as lark_mod
from lark.lark_bot import LarkBot

H = lambda n: timedelta(hours=n)


class KstToLocalTest(unittest.TestCase):

    def test_beijing_process_is_one_hour_behind(self):
        self.assertEqual(kst_to_local('10:00', H(8)), (0, '09:00'))
        self.assertEqual(kst_to_local('23:10', H(8)), (0, '22:10'))
        self.assertEqual(kst_to_local('10:30', H(8)), (0, '09:30'))

    def test_crossing_midnight_shifts_to_previous_day(self):
        self.assertEqual(kst_to_local('00:10', H(8)), (-1, '23:10'))

    def test_kst_process_is_identity(self):
        self.assertEqual(kst_to_local('00:10', H(9)), (0, '00:10'))
        self.assertEqual(kst_to_local('10:00', H(9)), (0, '10:00'))

    def test_utc_process(self):
        self.assertEqual(kst_to_local('10:00', H(0)), (0, '01:00'))
        self.assertEqual(kst_to_local('00:10', H(0)), (-1, '15:10'))


class QuietHourTest(unittest.TestCase):

    def test_quiet_from_1_to_9_kst(self):
        self.assertEqual([h for h in range(24) if is_realtime_quiet_hour(h)], list(range(1, 10)))


def _registered(local_offset):
    """按给定进程时区注册全部任务, 返回 {(函数名, 星期, 'HH:MM' 或 None)}"""
    schedule.clear()
    s = BrainScheduler.__new__(BrainScheduler)
    with patch.object(sched_mod, 'local_utc_offset', return_value=local_offset):
        s._register_jobs(interval_minutes=60)
    out = set()
    for job in schedule.get_jobs():
        at = job.at_time.strftime('%H:%M') if job.at_time else None
        out.add((job.job_func.func.__name__, job.start_day, at))
    schedule.clear()
    return out


class RegistrationTest(unittest.TestCase):
    """改用 KST 时刻表后, 实际触发时刻必须与改动前逐条一致"""

    LEGACY_BEIJING = (
        {('run_analysis', None, None)}
        | {('send_daily_report', None, '09:00')}
        | {('send_realtime_report', None, f'{h:02d}:10') for h in range(9, 23)}
        | {('send_closing_report', None, '23:10')}
        | {('send_personal_reports', None, f'{h:02d}:15') for h in range(9, 23)}
        | {('send_weekly_report', 'monday', '09:30')}
        | {('sync_drama_mapping', 'monday', '02:30')}
        | {('sync_xmp_stats', None, '02:00')}
    )

    def test_beijing_process_fires_at_exactly_the_legacy_times(self):
        self.assertEqual(_registered(H(8)), self.LEGACY_BEIJING)

    def test_kst_process_reads_as_the_kst_timetable(self):
        jobs = _registered(H(9))
        self.assertIn(('send_daily_report', None, '10:00'), jobs)
        self.assertIn(('send_closing_report', None, '00:10'), jobs)
        self.assertIn(('send_realtime_report', None, '23:10'), jobs)
        self.assertIn(('send_weekly_report', 'monday', '10:30'), jobs)
        # 素材线任务是进程本地时间, 不随 KST 时刻表换算
        self.assertIn(('sync_xmp_stats', None, '02:00'), jobs)
        self.assertIn(('sync_drama_mapping', 'monday', '02:30'), jobs)

    def test_weekly_report_weekday_follows_when_conversion_crosses_midnight(self):
        # 进程在 UTC-5: KST 周一 10:30 = 本地周日 20:30
        self.assertIn(('send_weekly_report', 'sunday', '20:30'), _registered(H(-5)))


def _bare_scheduler(kst_time):
    s = BrainScheduler.__new__(BrainScheduler)
    s._kst_now = lambda: kst_time
    s._sleep = lambda seconds: None
    s.lark_bot = None
    s.data_source_label = 'QuickBI-dbt'
    s.bq_uploader = MagicMock()
    s.bq_uploader.get_upstream_batched_time.return_value = None
    s.quickbi_uploader = MagicMock()
    s._wait_for_realtime_data = MagicMock(return_value={
        'summary': {'total_spend': 100.0, 'mmp_roas': 0.5}, 'batch_id': '20261005_000312'})
    return s


class RealtimeGateTest(unittest.TestCase):

    def test_closing_report_at_kst_0010_is_not_skipped(self):
        s = _bare_scheduler(datetime(2026, 10, 5, 0, 10))
        result = s.send_realtime_report(use_latest_batch=True, report_date='2026-10-04')
        self.assertFalse(result.get('skipped'), result)
        s._wait_for_realtime_data.assert_called_once()

    def test_quiet_hours_are_skipped(self):
        for hour in (1, 5, 9):
            s = _bare_scheduler(datetime(2026, 10, 5, hour, 10))
            result = s.send_realtime_report()
            self.assertTrue(result.get('skipped'), f'KST {hour:02d}:10')
            s._wait_for_realtime_data.assert_not_called()

    def test_daytime_reports_run(self):
        for hour in (10, 23):
            s = _bare_scheduler(datetime(2026, 10, 5, hour, 10))
            self.assertFalse(s.send_realtime_report().get('skipped'), f'KST {hour:02d}:10')


class ClosingReportWaitTest(unittest.TestCase):
    """收官播报等 00:00 班的批次落库, 等不到再用已有批次"""

    def _scheduler(self, batch_ids, clock):
        s = BrainScheduler.__new__(BrainScheduler)
        s._kst_now = clock
        s.sleeps = []
        s._sleep = s.sleeps.append
        s.quickbi_uploader = MagicMock()
        s.quickbi_uploader.get_latest_batch_id.side_effect = batch_ids
        s.send_realtime_report = MagicMock(return_value={'success': True})
        return s

    def test_waits_until_midnight_batch_arrives(self):
        fixed = datetime(2026, 10, 5, 0, 10)
        s = self._scheduler(['20261004_230350', '20261004_230350', '20261005_000312'], lambda: fixed)
        s.send_closing_report()
        self.assertEqual(len(s.sleeps), 2)
        s.quickbi_uploader.get_latest_batch_id.assert_called_with('2026-10-04')
        s.send_realtime_report.assert_called_once_with(use_latest_batch=True, report_date='2026-10-04')

    def test_no_wait_when_batch_already_there(self):
        fixed = datetime(2026, 10, 5, 0, 10)
        s = self._scheduler(['20261005_000312'], lambda: fixed)
        s.send_closing_report()
        self.assertEqual(s.sleeps, [])
        s.send_realtime_report.assert_called_once()

    def test_gives_up_after_max_wait_and_still_reports(self):
        ticks = iter(datetime(2026, 10, 5, 0, 10) + timedelta(minutes=m) for m in range(100))
        s = self._scheduler(['20261004_230350'] * 100, lambda: next(ticks))
        s.send_closing_report()
        self.assertLessEqual(len(s.sleeps), 10)
        s.send_realtime_report.assert_called_once_with(use_latest_batch=True, report_date='2026-10-04')


class BatchLookupTest(unittest.TestCase):

    def _uploader(self, rows):
        u = BigQueryUploader.__new__(BigQueryUploader)
        u.project_id, u.dataset_id = 'p', 'd'
        u.client = MagicMock()
        u.client.query.return_value.result.return_value = rows
        return u

    def test_batch_upstream_time(self):
        u = self._uploader([MagicMock(batched_time='20261004 23:35:02')])
        self.assertEqual(u.get_batch_upstream_time('20261005_000312'), '20261004 23:35:02')
        self.assertIn("batch_id = '20261005_000312'", u.client.query.call_args[0][0])

    def test_batch_upstream_time_missing(self):
        self.assertIsNone(self._uploader([]).get_batch_upstream_time('20261005_000312'))
        self.assertIsNone(self._uploader([]).get_batch_upstream_time(None))


class LarkDisplayTest(unittest.TestCase):

    def test_data_asof_label(self):
        self.assertEqual(lark_mod.format_data_asof('20261004 23:35:02'), 'KST 10-04 23:35')
        self.assertIsNone(lark_mod.format_data_asof(None))
        self.assertIsNone(lark_mod.format_data_asof(''))
        self.assertIsNone(lark_mod.format_data_asof('not-a-time'))

    def _capture(self, call):
        bot = LarkBot('https://example.invalid/hook')
        sent = []
        bot._send = lambda payload: sent.append(payload) or {'code': 0}
        call(bot)
        self.assertEqual(len(sent), 1)
        return sent[0]

    def test_realtime_card_title_and_asof_line(self):
        data = {
            'date': '2026-10-04', 'current_hour': '00:10', 'batch_id': '20261005_000312',
            'summary': {'total_spend': 93000.0, 'total_revenue': 45000.0, 'mmp_roas': 0.49},
            'upstream_batched_time': '20261004 23:35:02',
        }
        payload = self._capture(lambda bot: bot.send_realtime_report(data, None))
        self.assertEqual(payload['card']['header']['title']['content'], '⏰ 实时战报 [KST 00:10]')
        body = json.dumps(payload, ensure_ascii=False)
        self.assertIn('数据截至', body)
        self.assertIn('KST 10-04 23:35', body)

    def test_realtime_card_without_upstream_time_has_no_asof_line(self):
        data = {'date': '2026-10-04', 'current_hour': '10:10',
                'summary': {'total_spend': 1.0, 'mmp_roas': 0.5}}
        payload = self._capture(lambda bot: bot.send_realtime_report(data, None))
        self.assertNotIn('数据截至', json.dumps(payload, ensure_ascii=False))

    def test_alert_time_is_labelled_kst(self):
        fixed = datetime(2026, 10, 5, 0, 10, 35)
        with patch.object(lark_mod, 'kst_now', return_value=fixed):
            payload = self._capture(lambda bot: bot.send_alert('数据延迟警告', 'msg', level='warning'))
        self.assertIn('2026-10-05 00:10:35 KST', json.dumps(payload, ensure_ascii=False))


if __name__ == '__main__':
    unittest.main(verbosity=2)
