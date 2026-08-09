"""
Quick BI dbt 数据服务采集器
数据源: Campaign_Ad_dataset_dbt / Overview_dataset_dbt (vigloo_dm, 与看板 [DBT] Marketer Performance 同源)

核心机制:
1. 明细 API 单次返回上限 1 万行, dbt 数据集一天 6-9 万输出行, 必须切片拉取
   切片降级链: 投手 -> 投手+渠道 -> 投手+渠道+剧目
2. 过滤值必须用底层原始值: 渠道显示值 Meta 对应过滤值 meta (小写)
3. 拉取完成后用聚合 API 校验 spend 总额, 差异 >0.5% 视为不完整, 拒绝落库
4. country_code 显示值为英文全名混 ISO 码, 统一归一化为 ISO-2 码
5. 日期口径: kst_date (韩国时区 UTC+9), batch_id 也用 KST 时间生成

用法:
    python quickbi/dbt_fetcher.py            # 拉取 KST 今天并落库
    python quickbi/dbt_fetcher.py 20260807   # 拉取指定日期并落库
    FETCH_YESTERDAY=true python quickbi/dbt_fetcher.py  # 拉取 KST 昨天 (日报 T-1)
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alibabacloud_quickbi_public20220101.client import Client
from alibabacloud_quickbi_public20220101 import models
from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util.models import RuntimeOptions
from dotenv import load_dotenv

from config.country_mapping import normalize_country

try:
    from utils.logger import get_logger
    logger = get_logger("dataget.dbt_fetcher")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("dbt_fetcher")

load_dotenv()

# KST 时区 (UTC+9), 与 kst_date 口径一致
KST_TZ = timezone(timedelta(hours=9))

# dbt 数据服务 API (Key data 空间)
DETAIL_API = os.getenv('DBT_DETAIL_API_ID', '4f9f9ced4bf1')      # Get_Campaign_Data_dbt_ax_mkt
AGG_API = os.getenv('DBT_AGG_API_ID', 'dbe6d5561d89')            # Get_Campaign_Agg_dbt_ax_mkt
DIMCOUNT_API = os.getenv('DBT_DIMCOUNT_API_ID', '43f9a682c64c')  # Get_Campaign_DimCount_dbt_ax_mkt
OVERVIEW_API = os.getenv('DBT_OVERVIEW_API_ID', 'bde60da44aa2')  # Get_overview_data_dbt

# 单切片行数达到该值视为可能被 1 万行上限截断, 需要降级细分
TRUNCATION_THRESHOLD = 9990
# Agg 校验允许的 spend 相对差异
VERIFY_TOLERANCE = 0.005

# 明细 API 返回的数值字段 (比率类字段 CPI/d24h_roas/media_roas 等不落库, 下游按聚合重算)
METRIC_FIELDS = [
    'spend', 'impression', 'clicks',
    'new_users_mkt', 'd24h_payers', 'd24h_revenue', 'd24h_ad_revenue',
    'mmp_total_revenue', 'mmp_sub_revenue', 'mmp_ad_revenue',
    'mmp_renewal_revenue', 'mmp_renewals', 'mmp_sub_purchase',
]
DIMENSION_FIELDS = [
    'kst_date', 'channel', 'country_code', 'optimizer', 'campaign_status',
    'account_id', 'drama_id', 'drama_name', 'campaign_id', 'campaign_name',
]


class DbtFetcher:
    """dbt 数据服务采集器"""

    def __init__(self):
        self.client = Client(Config(
            access_key_id=os.getenv('ALIYUN_ACCESS_KEY_ID'),
            access_key_secret=os.getenv('ALIYUN_ACCESS_KEY_SECRET'),
            endpoint='quickbi-public.cn-hangzhou.aliyuncs.com',
        ))
        self.runtime = RuntimeOptions(read_timeout=180000, connect_timeout=30000)
        self.api_calls = 0

    # ---------- 底层调用 ----------

    def _call(self, api_id: str, date_from: str, date_to: str, extra: Optional[Dict] = None,
              max_retries: int = 3) -> List[Dict]:
        """调用数据服务 API, 带重试"""
        conditions = {'kst_date_from': date_from, 'kst_date_to': date_to}
        if extra:
            conditions.update(extra)
        request = models.QueryDataServiceRequest(api_id=api_id, conditions=json.dumps(conditions))

        retry_delays = [10, 30, 60]
        for attempt in range(max_retries):
            try:
                self.api_calls += 1
                response = self.client.query_data_service_with_options(request, self.runtime)
                if response.body.result:
                    return response.body.result.values or []
                return []
            except Exception as e:
                msg = str(e)
                retryable = ('503' in msg or 'ServiceUnavailable' in msg
                             or 'timeout' in msg.lower() or 'Datasource.Sql.ExecuteFailed' in msg)
                if retryable and attempt < max_retries - 1:
                    delay = retry_delays[attempt]
                    logger.warning(f"API {api_id} 暂时不可用, {delay}s 后重试 ({attempt + 1}/{max_retries}): {msg[:150]}")
                    time.sleep(delay)
                    continue
                raise

    # ---------- campaign 明细 ----------

    def fetch_campaign_day(self, date: str) -> List[Dict]:
        """
        拉取一天的完整 campaign 明细 (切片 + 校验)

        Args:
            date: YYYYMMDD (KST)

        Returns:
            归一化后的行列表; 校验不通过抛 RuntimeError
        """
        self.api_calls = 0

        # 1. 基准: 真实 spend 总额
        agg = self._call(AGG_API, date, date)
        if not agg:
            logger.warning(f"[{date}] Agg API 无数据, 该日无投放")
            return []
        true_spend = float(agg[0].get('total_spend') or 0)
        logger.info(f"[{date}] 基准: 底层 {agg[0].get('row_count')} 行, spend=${true_spend:,.2f}")

        # 2. 切片规划
        dim = self._call(DIMCOUNT_API, date, date)
        optimizers = sorted({d['optimizer'] for d in dim if d.get('optimizer')})
        # 渠道过滤值必须用小写原始值 (显示值 Meta <-> 过滤值 meta)
        channels = sorted({(d.get('channel') or '').lower() for d in dim if d.get('channel')})
        empty_opt_channels = sorted({(d['channel'] or '').lower() for d in dim
                                     if not d.get('optimizer') and d.get('channel')})

        # 3. 按投手切片, 撞上限自动降级
        rows: List[Dict] = []
        for opt in optimizers:
            rows.extend(self._pull_sliced(date, {'optimizer': opt}, ['channel', 'drama_id'], dim))

        # 4. 无投手行 (自然量/未映射 campaign 的延迟归因收入, spend 通常为 0)
        for ch in empty_opt_channels:
            part = self._call(DETAIL_API, date, date, {'channel': ch})
            if len(part) >= TRUNCATION_THRESHOLD:
                dramas = sorted({d.get('drama_id') or '' for d in dim
                                 if not d.get('optimizer') and (d.get('channel') or '').lower() == ch})
                part = []
                for did in dramas:
                    if did:
                        part.extend(self._call(DETAIL_API, date, date, {'channel': ch, 'drama_id': did}))
            rows.extend(r for r in part if not r.get('optimizer'))

        # 5. Agg 校验: 不通过拒绝落库
        got_spend = sum(float(r.get('spend') or 0) for r in rows)
        diff = abs(got_spend - true_spend) / true_spend if true_spend else 0
        if diff > VERIFY_TOLERANCE:
            raise RuntimeError(
                f"[{date}] 完整性校验失败: 拉取 spend=${got_spend:,.2f} vs 基准 ${true_spend:,.2f} "
                f"差异 {diff:.2%} (阈值 {VERIFY_TOLERANCE:.1%}), 拒绝落库")
        logger.info(f"[{date}] 校验通过: {len(rows)} 行, spend 差异 {diff:.3%}, API 调用 {self.api_calls} 次")

        return self._normalize_rows(rows)

    def _pull_sliced(self, date: str, base: Dict, fallback_keys: List[str], dim: List[Dict]) -> List[Dict]:
        """拉取一个切片; 行数贴上限时按 fallback_keys 逐级细分"""
        part = self._call(DETAIL_API, date, date, base)
        if len(part) < TRUNCATION_THRESHOLD or not fallback_keys:
            if len(part) >= TRUNCATION_THRESHOLD:
                logger.error(f"[{date}] 切片仍贴上限且无法再降级: {base}")
            return part

        key = fallback_keys[0]
        if key == 'channel':
            values = sorted({(d.get('channel') or '').lower() for d in dim
                             if (d.get('optimizer') or '') == base.get('optimizer', '') and d.get('channel')})
        else:  # drama_id
            values = sorted({d.get('drama_id') or '' for d in dim
                             if (d.get('optimizer') or '') == base.get('optimizer', '')
                             and (not base.get('channel')
                                  or (d.get('channel') or '').lower() == base['channel'])})
        out: List[Dict] = []
        for v in values:
            if not v:
                continue
            sub = dict(base)
            sub[key] = v
            out.extend(self._pull_sliced(date, sub, fallback_keys[1:], dim))
        return out

    def _normalize_rows(self, rows: List[Dict]) -> List[Dict]:
        """归一化: 数值转型 / country ISO 码 / 日期格式化 / 过滤全零行"""
        out = []
        for r in rows:
            metrics = {}
            all_zero = True
            for f in METRIC_FIELDS:
                v = r.get(f)
                try:
                    fv = float(v) if v not in (None, '', '-') else 0.0
                except (TypeError, ValueError):
                    fv = 0.0
                metrics[f] = fv
                if fv:
                    all_zero = False
            if all_zero:
                continue  # 全零行不落库 (对任何聚合无贡献, 省 2/3 存储)

            kst_raw = str(r.get('kst_date') or '')
            kst_date = f"{kst_raw[:4]}-{kst_raw[4:6]}-{kst_raw[6:8]}" if len(kst_raw) == 8 else kst_raw
            country_raw = (r.get('country_code') or '').strip()

            row = {
                'kst_date': kst_date,
                'channel': r.get('channel') or '',
                'country_code': normalize_country(country_raw),
                'country_raw': country_raw,
                'optimizer': r.get('optimizer') or '',
                'campaign_status': r.get('campaign_status') or '',
                'account_id': r.get('account_id') or '',
                'drama_id': r.get('drama_id') or '',
                'drama_name': r.get('drama_name') or '',
                'campaign_id': r.get('campaign_id') or '',
                'campaign_name': r.get('campaign_name') or '',
            }
            row.update(metrics)
            out.append(row)
        return out

    # ---------- overview 大盘 ----------

    def fetch_overview(self, date: str) -> Optional[Dict]:
        """
        拉取一天的大盘数据 (全平台收入含自然量老用户)

        Returns:
            {'kst_date', 'total_revenue', 'spend', 'batched_time'} 或 None
        """
        vals = self._call(OVERVIEW_API, date, date)
        if not vals:
            return None
        v = vals[0]
        # 多个批次分组时逐组求和, batched_time 取最大 (表级批次戳, 通常只有一组)
        total_revenue = sum(float(x.get('total_revenue') or 0) for x in vals)
        spend = sum(float(x.get('spend') or 0) for x in vals)
        batched_time = max((x.get('batched_time') or '' for x in vals), default='')
        return {
            'kst_date': f"{date[:4]}-{date[4:6]}-{date[6:8]}",
            'total_revenue': total_revenue,
            'spend': spend,
            'batched_time': batched_time,  # 格式: 'YYYYMMDD HH:MM:SS' (KST), 上游 ETL 批次戳
        }

    def get_upstream_freshness(self) -> Optional[datetime]:
        """上游 dbt 数据新鲜度: 返回最新 batched_time (KST aware datetime), 用于停更检测"""
        today = datetime.now(KST_TZ).strftime('%Y%m%d')
        ov = self.fetch_overview(today) or self.fetch_overview(
            (datetime.now(KST_TZ) - timedelta(days=1)).strftime('%Y%m%d'))
        if not ov or not ov['batched_time']:
            return None
        try:
            return datetime.strptime(ov['batched_time'], '%Y%m%d %H:%M:%S').replace(tzinfo=KST_TZ)
        except ValueError:
            return None


def main():
    """采集入口: 拉取指定日期 (默认 KST 今天) 并落库 BigQuery"""
    if len(sys.argv) > 1:
        date = sys.argv[1]
    elif os.getenv('FETCH_YESTERDAY', '').lower() == 'true':
        date = (datetime.now(KST_TZ) - timedelta(days=1)).strftime('%Y%m%d')
        logger.info(f"[T-1 模式] 采集 KST 昨天: {date}")
    else:
        date = datetime.now(KST_TZ).strftime('%Y%m%d')

    fetcher = DbtFetcher()
    rows = fetcher.fetch_campaign_day(date)
    if not rows:
        logger.warning(f"[{date}] 无数据, 跳过落库")
        return

    overview = fetcher.fetch_overview(date)

    # batch_id 用 KST 时间, 与 kst_date 同一时钟
    batch_id = datetime.now(KST_TZ).strftime('%Y%m%d_%H%M%S')
    logger.info(f"批次 ID: {batch_id} (KST)")

    bq_project = os.getenv('BQ_PROJECT_ID')
    bq_dataset = os.getenv('BQ_DATASET_ID', 'quickbi_data')
    if not bq_project:
        logger.error("未配置 BQ_PROJECT_ID, 跳过落库")
        return

    from bigquery_storage import BigQueryUploader
    uploader = BigQueryUploader(bq_project, bq_dataset)
    count = uploader.upload_dbt_campaigns(rows, batch_id=batch_id)
    logger.info(f"dbt_campaigns 落库 {count} 行")
    if overview:
        uploader.upload_dbt_overview(overview, batch_id=batch_id)
        logger.info(f"dbt_overview 落库: revenue=${overview['total_revenue']:,.2f} "
                    f"spend=${overview['spend']:,.2f} batched_time={overview['batched_time']}")


if __name__ == '__main__':
    main()
