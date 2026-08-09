# dbt 数据服务对接文档

> 广告投放数据(短剧出海)取数接口对接指南。数据与看板 **[DBT] Marketer Performance** 同源同口径。
> 参考实现: [quickbi/dbt_fetcher.py](../quickbi/dbt_fetcher.py) (生产采集器,含全部逻辑)
> 维护: 数据采集组 | 更新: 2026-08-09

---

## 1. 概述

数据来自阿里云 QuickBI **数据服务 API**(dbt 数据集,Key data 空间),底层数据集:

| 数据集 | 用途 |
|---|---|
| `Campaign_Ad_dataset_dbt` (vigloo_dm) | campaign 明细(投放 + 归因) |
| `Overview_dataset_dbt` (vigloo_dm) | 大盘收入(含自然量老用户) |

**铁律: 取到的数字必须与看板一致。** 看板地址: https://bi.aliyun.com/product/vigloo.htm?menuId=eaf02d95-131d-4ccc-8b72-496021131956

---

## 2. 前置条件

1. **阿里云 RAM 子账号 AccessKey**(AccessKey ID + Secret)
2. RAM 授权 `AliyunQuickBIFullAccess`,且账号需加入 QuickBI **组织成员** 并有 Key data 空间权限
3. Python SDK:

```bash
pip install alibabacloud-quickbi-public20220101 alibabacloud-tea-openapi alibabacloud-tea-util
```

Endpoint 固定: `quickbi-public.cn-hangzhou.aliyuncs.com`

---

## 3. API 清单

共 4 个数据服务 API,均接受相同的日期参数:

| api_id | API 名称 | 用途 | 单次上限 |
|---|---|---|---|
| `4f9f9ced4bf1` | Get_Campaign_Data_dbt_ax_mkt | **明细**: 逐 campaign×国家 行级数据 | **1 万行(会静默截断)** |
| `dbe6d5561d89` | Get_Campaign_Agg_dbt_ax_mkt | **聚合**: 全天 total_spend + row_count,用作校验基准 | 1 行 |
| `43f9a682c64c` | Get_Campaign_DimCount_dbt_ax_mkt | **维度清单**: 当天出现的 optimizer×channel×drama_id 组合,用于切片规划 | 数百行 |
| `bde60da44aa2` | Get_overview_data_dbt | **大盘**: total_revenue(SUM)+spend(SUM)+batched_time | 1~数行 |

### 请求参数(conditions,JSON 字符串)

| 参数 | 必填 | 格式 | 说明 |
|---|---|---|---|
| `kst_date_from` | 是 | `YYYYMMDD` | 开始日期(KST) |
| `kst_date_to` | 是 | `YYYYMMDD` | 结束日期(KST) |
| `optimizer` | 否 | 字符串 | 投手过滤(明细 API 切片用) |
| `channel` | 否 | 字符串 | 渠道过滤,**必须用小写原始值**(见 5.2) |
| `drama_id` | 否 | 字符串 | 剧目过滤 |

### 调用示例

```python
import json
from alibabacloud_quickbi_public20220101.client import Client
from alibabacloud_quickbi_public20220101 import models
from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util.models import RuntimeOptions

client = Client(Config(
    access_key_id='<ALIYUN_ACCESS_KEY_ID>',
    access_key_secret='<ALIYUN_ACCESS_KEY_SECRET>',
    endpoint='quickbi-public.cn-hangzhou.aliyuncs.com',
))
runtime = RuntimeOptions(read_timeout=180000, connect_timeout=30000)

request = models.QueryDataServiceRequest(
    api_id='4f9f9ced4bf1',
    conditions=json.dumps({
        'kst_date_from': '20260808',
        'kst_date_to': '20260808',
        'optimizer': 'felix',        # 可选切片条件
    }),
)
response = client.query_data_service_with_options(request, runtime)
rows = response.body.result.values or []   # List[Dict]
```

---

## 4. 明细 API 字段字典

### 维度字段

| 字段 | 说明 | 注意 |
|---|---|---|
| `kst_date` | 统计日期(KST 口径) | 返回 `YYYYMMDD`,建议落库转 `YYYY-MM-DD` |
| `optimizer` | 投手 | 自然量/未映射行为空 |
| `channel` | 渠道 | 显示值 `Meta/google/tiktok/snapchat/organic/other` |
| `country_code` | 国家 | **英文全名与 ISO 码混杂**(`Japan`/`United States`/`TW`/`UNKN`),必须归一化,见 5.3 |
| `campaign_id` / `campaign_name` | 广告系列 | |
| `campaign_status` | 状态 | `Active` / `Stopped` |
| `drama_id` / `drama_name` | 剧集 | |
| `account_id` | 广告账户 | |

### 指标字段(均为可累加原始值)

| 字段 | 说明 |
|---|---|
| `spend` | 消耗($) |
| `impression` | 曝光(单数,不是 impressions) |
| `clicks` | 点击 |
| `new_users_mkt` | 买量新增(不含自然量) |
| `d24h_payers` | 24h 窗口付费人数 |
| `d24h_revenue` | 24h 窗口总收入 |
| `d24h_ad_revenue` | 24h 窗口广告收入 |
| `mmp_total_revenue` | MMP 归因总收入 ← **主口径** |
| `mmp_sub_revenue` | MMP 订阅收入 |
| `mmp_ad_revenue` | MMP 广告收入 |
| `mmp_renewal_revenue` / `mmp_renewals` | MMP 续订收入 / 续订数 |
| `mmp_sub_purchase` | MMP 订阅购买数 |

> 明细 API 同时会返回 CPI/d24h_roas 等**比率字段,不要使用**——聚合后必须按下方公式重算,直接对比率求和/平均是错的。

### Overview API 字段

| 字段 | 说明 |
|---|---|
| `total_revenue` | 大盘总收入(含自然量+老用户,口径大于 campaign 明细之和) |
| `spend` | 大盘消耗(与明细 Agg 交叉对账差异 ~0.02%) |
| `batched_time` | 上游 dbt ETL 批次戳,`YYYYMMDD HH:MM:SS`(KST)。长时间不前进 = 上游停更,可做新鲜度监控 |

---

## 5. 关键规则(踩过的坑)

### 5.1 明细 API 1 万行静默截断 → 必须切片

单次返回 ≥ **9990 行**即视为可能被截断(实际上限 1 万,不报错、直接丢数据)。一天全量约 10 万+ 底层行,必须切片拉取:

```
按 optimizer 逐个拉
  └─ 某投手 ≥9990 行 → 追加 channel 条件细分
        └─ 仍 ≥9990 行 → 再追加 drama_id 细分
无投手行(自然量)按 channel 拉,不够再按 drama_id 细分
```

### 5.2 过滤值必须用底层原始值

数据集显示值和底层存储值不一致:显示 `Meta`,过滤条件必须传小写 **`meta`**,传 `Meta` 会静默返回 0 行。切片前先用 DimCount API 拿当天真实维度值,渠道统一 `.lower()` 后再当过滤条件。

### 5.3 country_code 必须归一化

返回值是英文全名与 ISO-2 码的混合(`Japan`、`United States`、`TW`、`UNKN`)。参考 [config/country_mapping.py](../config/country_mapping.py)(242 项全名→ISO2 映射):全名→转码,2 字码→原样,未知→`UNKN`。建议同时保留原始值(`country_raw`)便于排查。

### 5.4 拉完必须做完整性校验

用 Agg API 的 `total_spend` 作基准,切片拉取的 spend 求和与之对比,**相对差异 > 0.5% 拒绝落库**。生产实测正常差异 0.000%~0.2%。

### 5.5 重试策略

对 `503` / `ServiceUnavailable` / `timeout` / `Datasource.Sql.ExecuteFailed` 做指数退避重试(10s / 30s / 60s,最多 3 次)。

### 5.6 数据新鲜度与成熟度

- 上游 dbt 约 **每小时一批**(看 `batched_time`)
- MMP 归因收入**日切后仍持续回传**:当天数据次日仍会上涨 ~0.4%,spend 也有小幅修正。要出 T-1 定稿报表,建议在使用前(而非日切后立刻)重拉一次
- 全零行(所有指标为 0)可过滤不存,约省 2/3 存储,不影响任何聚合

---

## 6. 时区口径(重要)

**所有日期参数与 `kst_date` 均为 KST(UTC+9)口径,日切点 = 北京时间 23:00。**

```python
from datetime import datetime, timedelta
kst_now = datetime.utcnow() + timedelta(hours=9)   # "今天"一律这样算
today   = kst_now.strftime('%Y%m%d')
```

禁止用机器本地时间(北京 UTC+8)算"今天/昨天",否则每天 23:00~24:00 之间会差一天。

---

## 7. 指标计算公式(与看板计算字段逐字一致)

聚合到任意维度(投手/剧目/国家/全量)后,先对原始指标 SUM,再套公式:

| 指标 | 公式 | 看板对应列 |
|---|---|---|
| **MMP ROAS**(主口径) | `SUM(mmp_total_revenue) / SUM(spend)` | MMP ROAS |
| D24h ROAS | `SUM(d24h_revenue) / SUM(spend)` | D24h ROAS |
| CPI | `SUM(spend) / SUM(new_users_mkt)` | CPI |
| CTR | `SUM(clicks) / SUM(impression)` | CTR |
| CVR | `SUM(new_users_mkt) / SUM(clicks)` | CVR |
| CPM | `SUM(spend) / SUM(impression) × 1000` | CPM |
| 付费率 | `SUM(d24h_payers) / SUM(new_users_mkt)` | New Users_MKT Pay Rate |
| 大盘收入 | overview API `total_revenue`(不要用明细求和代替) | 大盘 Revenue |

除法一律做零保护(`SAFE_DIVIDE` 或判空)。

**业务阈值参考**(mmp_roas 口径):止损预警 Spend>$300 且 <30%;扩量机会 Spend>$300 且 >50%;评级 绿≥40% / 黄 30-40% / 红<30%。

---

## 8. 对接验收方法

1. 选一个已完结日期(T-2 以前,数据已成熟),拉全天明细
2. 按 optimizer 聚合,套第 7 节公式算 Spend / MMP ROAS
3. 与看板同日期 Cross table 逐投手比对:**spend 应精确到个位一致,ROAS 精确到小数点后两位一致**
4. 对不上时先查:是否漏切片(对比 Agg 基准)、是否取了旧批次、看板是否命中查询缓存(改一下日期筛选强制刷新)

---

## 附: 环境变量约定(本项目)

```bash
ALIYUN_ACCESS_KEY_ID=xxx
ALIYUN_ACCESS_KEY_SECRET=xxx
# api_id 有默认值,一般无需配置
# DBT_DETAIL_API_ID=4f9f9ced4bf1
# DBT_AGG_API_ID=dbe6d5561d89
# DBT_DIMCOUNT_API_ID=43f9a682c64c
# DBT_OVERVIEW_API_ID=bde60da44aa2
```
