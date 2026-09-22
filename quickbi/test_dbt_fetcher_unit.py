"""
dbt_fetcher 单元测试 (不访问网络, 不依赖 pytest)

用法:
    python quickbi/test_dbt_fetcher_unit.py
"""

import os
import sys
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quickbi import dbt_fetcher
from quickbi.dbt_fetcher import DbtFetcher, is_retryable_error


class RetryableErrorTest(unittest.TestCase):

    def test_existing_retryable_errors_still_match(self):
        for msg in [
            "Error: ServiceUnavailable code: 503, The request has failed",
            "Read timeout on endpoint",
            "Datasource.Sql.ExecuteFailed: query failed",
        ]:
            self.assertTrue(is_retryable_error(msg), msg)

    def test_cross_border_connection_errors_are_retryable(self):
        for msg in [
            "{'message': \"('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))\"}",
            "{'message': \"('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))\"}",
        ]:
            self.assertTrue(is_retryable_error(msg), msg)

    def test_permanent_errors_are_not_retryable(self):
        for msg in [
            "Error: InvalidAccessKeyId.NotFound code: 404",
            "Error: Forbidden code: 403, no permission",
            "Error: InvalidParameter code: 400, api_id not found",
        ]:
            self.assertFalse(is_retryable_error(msg), msg)


class ConcurrentFetchTest(unittest.TestCase):
    """按投手并发拉取: 结果顺序稳定、每个投手恰好拉一次、线程各用各的 client"""

    def _make_fetcher(self):
        with patch.object(dbt_fetcher, 'Client') as client_cls:
            client_cls.side_effect = lambda *a, **k: object()
            fetcher = DbtFetcher()
        return fetcher

    def test_rows_cover_every_optimizer_in_stable_order(self):
        fetcher = self._make_fetcher()
        optimizers = [f'opt{i:02d}' for i in range(12)]
        dim = [{'optimizer': o, 'channel': 'meta', 'drama_id': 'd1'} for o in optimizers]

        def fake_call(api_id, date_from, date_to, extra=None, max_retries=3):
            if api_id == dbt_fetcher.AGG_API:
                return [{'total_spend': str(len(optimizers) * 10.0), 'row_count': 1}]
            if api_id == dbt_fetcher.DIMCOUNT_API:
                return dim
            opt = extra['optimizer']
            return [{'optimizer': opt, 'spend': '10', 'kst_date': date_from,
                     'campaign_id': f'c-{opt}', 'country_code': 'US'}]

        with patch.object(fetcher, '_call', side_effect=fake_call):
            rows = fetcher.fetch_campaign_day('20260921')

        self.assertEqual([r['optimizer'] for r in rows], optimizers)

    def test_each_thread_gets_its_own_client(self):
        fetcher = self._make_fetcher()
        seen = {}  # 持有对象引用, 避免线程结束后对象回收导致 id 复用

        def record():
            seen[threading.get_ident()] = (fetcher._get_client(), fetcher._get_client())

        with patch.object(dbt_fetcher, 'Client', side_effect=lambda *a, **k: object()):
            threads = [threading.Thread(target=record) for _ in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(len(seen), 4)
        self.assertEqual(len({id(first) for first, _ in seen.values()}), 4,
                         "不同线程必须拿到不同的 client")
        for first, second in seen.values():
            self.assertIs(first, second, "同一线程内 client 必须复用")

    def test_worker_count_from_env(self):
        with patch.dict(os.environ, {'DBT_FETCH_WORKERS': '6'}):
            self.assertEqual(dbt_fetcher.fetch_workers(), 6)
        with patch.dict(os.environ, {'DBT_FETCH_WORKERS': 'abc'}):
            self.assertEqual(dbt_fetcher.fetch_workers(), 4)
        with patch.dict(os.environ, {'DBT_FETCH_WORKERS': '0'}):
            self.assertEqual(dbt_fetcher.fetch_workers(), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
