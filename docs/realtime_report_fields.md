# 实时战报字段说明文档

## 概述

实时战报 (`send_realtime_report`) 是为执行层提供的实时监控报告。

**触发方式：**
- 原有：每日 9:00 - 24:00 每整点10分触发
- 新增：支持自定义间隔（如30秒、30分钟）的高频播报模式

**数据源：** BigQuery `quickbi_data.quickbi_campaigns` 表

---

## 📊 数据查询逻辑说明

### 核心概念：batch_id

所有实时播报数据都基于 **batch_id** 查询，batch_id 是数据快照的唯一标识。

**batch_id 格式：** `YYYYMMDD_HHMMSS`
- 例如：`20251225_140033` 表示 2025-12-25 14:00:33 的数据快照
- 由 QuickBI 数据采集器在每次同步时生成

**查询过滤：** 所有查询都使用 `batch_filter = "AND batch_id = '{batch_id}'"`

**代码位置：** `bigquery_storage.py:query_realtime_report_data()`

---

### 数据获取流程

实时播报需要获取 **3 个时间点** 的数据：

| 数据类型 | 用途 | 查询逻辑 | 代码位置 |
|---------|------|---------|---------|
| **当前 batch** | 当前累计数据 | 获取今天最新的 batch | `bigquery_storage.py:1172` |
| **昨天同整点 batch** | 日环比对比 | 查找昨天当前整点（±10分钟）的 batch | `bigquery_storage.py:1320-1341` |
| **上一整点 batch** | 小时环比对比 | 查找今天上一整点（±10分钟）的 batch | `bigquery_storage.py:1362-1393` |

---

### 1. 当前 batch 查询

**目标：** 获取今天最新的数据快照

```python
# 获取当日最新 batch_id
batch_id = self._get_latest_batch_id(table_ref, today)
# 例如：batch_id = "20251225_120000"
```

**SQL 查询示例：**
```sql
SELECT SUM(spend) as total_spend,
       SUM(media_user_revenue) as total_media_revenue,
       SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as media_roas
FROM `project.dataset.quickbi_campaigns`
WHERE stat_date = '2025-12-25'
  AND batch_id = '20251225_120000'
```

**说明：**
- 查询结果是 **截止到当前整点的累计数据**
- 例如：batch_id = `20251225_120000`，则返回截止到 12:00 的累计消耗、收入、ROAS

---

### 2. 昨天同整点 batch 查询（日环比）

**目标：** 查找昨天相同整点的数据，用于计算日环比

**查询逻辑：** （`bigquery_storage.py:1320-1341`）

```python
# 例如：现在是 12:30，查找昨天 12:00-12:10 范围内的 batch
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
current_hour_int = datetime.now().hour  # 12

yesterday_date_str = yesterday.replace('-', '')  # "20251224"
yesterday_hour_start = f"{yesterday_date_str}_{current_hour_int:02d}0000"  # "20251224_120000"
yesterday_hour_end = f"{yesterday_date_str}_{current_hour_int:02d}1000"    # "20251224_121000"
```

**SQL 查询示例：**
```sql
-- 查找昨天 12:00-12:10 范围内最新的 batch
SELECT batch_id
FROM `project.dataset.quickbi_campaigns`
WHERE stat_date = '2025-12-24'
  AND batch_id >= '20251224_120000'
  AND batch_id <= '20251224_121000'
ORDER BY batch_id DESC
LIMIT 1
```

**说明：**
- 查找昨天 **当前整点后 0-10 分钟** 范围内的最新 batch
- 例如：现在 12:30，查找昨天 12:00-12:10 的 batch
- 确保对比的是 **相同时间段** 的数据

---

### 3. 上一整点 batch 查询（小时环比）

**目标：** 查找今天上一整点的数据，用于计算小时环比

**查询逻辑：** （`bigquery_storage.py:1362-1393`）

```python
# 例如：现在是 12:30，查找今天 11:00-11:10 范围内的 batch
current_hour = datetime.now().hour  # 12
prev_hour = (current_hour - 1) % 24  # 11

prev_hour_start = f"{today.replace('-', '')}_{prev_hour:02d}0000"  # "20251225_110000"
prev_hour_end = f"{today.replace('-', '')}_{prev_hour:02d}1000"    # "20251225_111000"
```

**SQL 查询示例：**
```sql
-- 查找今天 11:00-11:10 范围内最新的 batch
SELECT batch_id
FROM `project.dataset.quickbi_campaigns`
WHERE stat_date = '2025-12-25'
  AND batch_id >= '20251225_110000'
  AND batch_id <= '20251225_111000'
ORDER BY batch_id DESC
LIMIT 1
```

**说明：**
- 查找今天 **上一整点后 0-10 分钟** 范围内的最新 batch
- 支持跨天（如果当前是 00:xx，则查找昨天 23:00 的 batch）

---

### 核心字段数据源和计算方式

根据你的截图 [实时战报 14:50]，以下是各字段的数据来源：

| 显示字段 | 数据来源 | SQL 查询 | 代码位置 |
|---------|---------|---------|---------|
| **截止当前总耗** | `summary.total_spend` | `SUM(spend)` | `bigquery_storage.py:1227` |
| **日环比** | `yesterday_summary.total_spend` | `(今天 - 昨天) / 昨天 × 100%` | 计算得出 |
| **截止当前收入** | `summary.total_media_revenue` | `SUM(media_user_revenue)` | `bigquery_storage.py:1229` |
| **当前 Media ROAS** | `summary.media_roas` | `SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))` | `bigquery_storage.py:1230` |
| **ROAS 日环比** | `yesterday_summary.media_roas` | `今天 ROAS - 昨天 ROAS` (绝对值) | 计算得出 |
| **新增消耗** | `prev_hour_summary.total_spend` | `当前累计 - 上小时累计` | 计算得出 |
| **小时环比** | 同上 | `新增消耗 / 上小时累计 × 100%` | 计算得出 |
| **过去1小时 ROAS 趋势** | `prev_hour_summary.media_roas` | `当前 ROAS - 上小时 ROAS` (绝对值) | 计算得出 |

**重要说明：**
- ✅ **收入字段使用 `media_user_revenue`**（媒体归因收入）
- ✅ **所有 ROAS 均为 Media ROAS**（= media_user_revenue / spend）
- ✅ **ROAS 环比/趋势使用绝对值差异**，不是百分比变化

---

### 关键设计要点

✅ **时间点一致性**
- 所有对比都基于 **整点数据**（±10分钟容差）
- 避免分钟级波动影响判断

✅ **累计 vs 增量**
- "截止当前" 的指标都是 **累计值**（从 00:00 到当前整点）
- "新增消耗" 是 **增量值**（当前整点 - 上一整点）

✅ **ROAS 环比的特殊性**
- ROAS 的环比/趋势使用 **绝对值差异**，不是百分比
- 例如：50.8% - 43.7% = +7.1%（不是 +16.3%）

---

### 示例说明

**场景：** 现在是 2025-12-25 12:30

| 时间点 | batch_id | 累计消耗 | 累计收入 | ROAS |
|--------|----------|---------|---------|------|
| 今天 12:00 | `20251225_120000` | $40,000 | $20,320 | 50.8% |
| 今天 11:00 | `20251225_110000` | $36,353 | $18,200 | 50.1% |
| 昨天 12:00 | `20251224_120000` | $85,000 | $37,145 | 43.7% |

**计算结果：**

1. **截止当前总耗：** $40,000
2. **日环比：** (40,000 - 85,000) / 85,000 × 100% = **-53%**
3. **当前 Media ROAS：** 50.8%
4. **ROAS 日环比：** 50.8% - 43.7% = **+7.1%**
5. **新增消耗：** 40,000 - 36,353 = **$3,647**
6. **小时环比：** 3,647 / 36,353 × 100% = **+10%**
7. **ROAS 趋势：** 50.8% - 50.1% = **+0.7%**

---

## 重要说明：ROAS 字段定义

**所有 `roas` 字段均为 Media ROAS**，计算公式：

```sql
roas = SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))
```

即：`Media ROAS = media_user_revenue / spend`

| 字段位置 | 字段名 | 计算方式 |
|---------|--------|---------|
| `summary.media_roas` | Media ROAS | `SUM(media_user_revenue) / SUM(spend)` |
| `optimizer_spend[].roas` | Media ROAS | 按投手聚合后计算 |
| `stop_loss_campaigns[].roas` | Media ROAS | 按 campaign 聚合后计算 |
| `scale_up_campaigns[].roas` | Media ROAS | 按 campaign 聚合后计算 |
| `country_marginal_roas[].roas` | Media ROAS | 按国家聚合后计算 |

---

## 1. 数据输入结构

调用 `send_realtime_report(data)` 时需要传入的完整数据结构：

```python
data = {
    # 基础信息
    "current_hour": "17:48",           # 当前时间
    "data_delayed": False,             # 是否数据延迟
    "batch_time": "17:00",             # 当前批次时间
    "prev_batch_time": "16:06",        # 上一批次时间

    # 当前汇总数据
    "summary": {
        "total_spend": 56251.80,       # 截止当前总消耗
        "total_revenue": 20219.31,     # 截止当前总收入
        "media_roas": 0.45             # 当前 Media ROAS
    },

    # 昨日同时刻数据 (用于日环比)
    "yesterday_summary": {
        "total_spend": 72000,
        "total_revenue": 27300,
        "media_roas": 0.449
    },

    # 上一小时快照 (用于小时环比)
    "prev_hour_summary": {
        "total_spend": 53212.99,
        "media_roas": 0.455,
        "optimizer_data": [...]        # 上小时各投手数据
    },

    # 投手消耗明细
    "optimizer_spend": [...],

    # 止损预警计划
    "stop_loss_campaigns": [...],

    # 扩量机会计划
    "scale_up_campaigns": [...],

    # 国家边际 ROAS
    "country_marginal_roas": [...]
}
```

---

## 2. 标题区域

### 2.1 卡片标题

| 显示内容 | 数据来源 | 说明 |
|---------|---------|------|
| `实时战报 [17:48]` | `data.current_hour` | 直接取值，默认为当前系统时间 |

### 2.2 大盘状态

| 显示内容 | 数据来源 | 计算方式 |
|---------|---------|---------|
| `🟢 大盘健康：当前 ROAS 45.0%` | `summary.media_roas` | 与 `roas_green_threshold`(40%) 比较 |
| `🔴 大盘预警：当前 ROAS 38.5% (低于基线 40%)` | 同上 | `media_roas < 40%` 时显示 |

**判断逻辑：**
```python
roas_baseline = config.get("roas_green_threshold", 0.40)
if media_roas < roas_baseline:
    # 显示红色预警
else:
    # 显示绿色健康
```

---

## 3. ⏰ 实时战报板块

### 3.1 消耗数据

| 显示内容 | 数据来源 | 计算方式 |
|---------|---------|---------|
| 截止当前总耗 `$56,251.80` | `summary.total_spend` | 直接取值 |
| 日环比 `-22%` | `yesterday_summary.total_spend` | `(today - yesterday) / yesterday * 100` |

**计算代码：**
```python
total_spend = summary.get("total_spend", 0)
yesterday_spend = yesterday_summary.get("total_spend", 0)
daily_spend_change_pct = ((total_spend - yesterday_spend) / yesterday_spend * 100) if yesterday_spend > 0 else 0
```

### 3.2 收入数据

| 显示内容 | 数据来源 | 计算方式 |
|---------|---------|---------|
| 截止当前收入 `$20,219.31` | `summary.total_revenue` | 直接取值 |
| 日环比 `-26%` | `yesterday_summary.total_revenue` | `(today - yesterday) / yesterday * 100` |

### 3.3 ROAS 数据

| 显示内容 | 数据来源 | 计算方式 |
|---------|---------|---------|
| 当前 Media ROAS `45.0%` | `summary.media_roas` | 直接取值 |
| 日环比 `+0.1%` | `yesterday_summary.media_roas` | `today_roas - yesterday_roas` (绝对值差) |

### 3.4 小时环比数据

| 显示内容 | 数据来源 | 计算方式 |
|---------|---------|---------|
| 新增消耗 `$3,038.81` | `prev_hour_summary.total_spend` | `total_spend - prev_total_spend` |
| 环比 `+6%` | 同上 | `hourly_delta / prev_total_spend * 100` |
| ROAS 趋势 `↘️ 下滑 0.5%` | `prev_hour_summary.media_roas` | `media_roas - prev_roas` |

**计算代码：**
```python
prev_total_spend = prev_hour_summary.get("total_spend", 0)
prev_roas = prev_hour_summary.get("media_roas", 0)
hourly_spend_delta = total_spend - prev_total_spend
roas_trend = media_roas - prev_roas
hourly_spend_change_pct = (hourly_spend_delta / prev_total_spend * 100) if prev_total_spend > 0 else 0
```

---

## 4. 🔍 谁在花钱？(投手表格)

### 4.1 数据来源

**SQL 查询：** `bigquery_storage.py:1236-1248`

```sql
SELECT
    optimizer,
    SUM(spend) as spend,
    SUM(media_user_revenue) as revenue,
    SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
FROM `quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-25' AND batch_id = '20251225_140033'
  AND optimizer IS NOT NULL AND optimizer != ''
GROUP BY optimizer
ORDER BY spend DESC
```

**主力计划查询：** `bigquery_storage.py:1251-1264`

```sql
SELECT
    optimizer, campaign_name, drama_name, country, SUM(spend) as spend
FROM `quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-25' AND batch_id = '20251225_140033'
GROUP BY optimizer, campaign_name, drama_name, country
ORDER BY optimizer, spend DESC
```

### 4.2 字段映射（根据截图）

| 列名 | 数据来源 | SQL 字段 | 说明 |
|-----|---------|---------|------|
| **投手** | `optimizer` | `optimizer` | 直接取值 |
| **新增消耗** | 计算得出 | `当前 spend - 上小时 spend` | 需要上小时数据对比 |
| **累计消耗** | `spend` | `SUM(spend)` | 当日累计消耗 |
| **当前Media ROAS** | `roas` | `SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))` | Media ROAS |
| **主力计划** | `top_campaigns` | 取消耗最高的计划 | 格式：`drama_name(country)` |

### 4.3 状态判断规则

```python
delta = current_spend - prev_spend
if delta > 100:
    status = "🔥"           # 消耗活跃
elif delta < 50:
    status = "🐢 缓慢"      # 消耗缓慢
else:
    status = ""             # 正常
```

---

## 5. ⚠️ 过去1小时异动分析

### 5.1 触发条件

找出消耗激增但 ROAS 低的投手：

```python
roas_warning_threshold = config.get("roas_yellow_threshold", 0.30)
anomaly_optimizers = [
    opt for opt in optimizer_deltas
    if opt["delta"] > 200 and opt["roas"] < roas_warning_threshold
]
```

**条件：**
- 过去1小时新增消耗 > $200
- 当前 ROAS < 30%

### 5.2 显示内容

```
⚠️ 过去1小时变化：
🔥 **Kimi** 消耗激增 $600，但 ROAS 仅 20%
   请重点检查计划：Was It Just a Coincidence... or Fate?(KR)
```

---

## 6. ⚡️ 操作建议 (AI 分析)

### 6.1 ChatGPT 分析

| 显示内容 | 数据来源 | 说明 |
|---------|---------|------|
| 🤖 GPT分析 | `chatgpt_advisor.analyze_realtime_data()` | `hourly_trend` 字段 |
| 📈 节奏评估 | 同上 | `pace_assessment` 字段 |
| 🚨 紧急操作 | 同上 | `urgent_actions` 列表，最多2条 |
| 👀 观察项 | 同上 | `watch_list` 列表，最多2条 |

### 6.2 Gemini 分析

| 显示内容 | 数据来源 | 说明 |
|---------|---------|------|
| 📊 整体态势 | `gemini_advisor.generate_realtime_insights()` | `overall_assessment` 字段 |
| 🤖 AI止损建议 | 同上 | `stop_loss_advice` 字段 |
| 🤖 AI扩量建议 | 同上 | `scale_up_advice` 字段 |

### 6.3 降级规则 (AI 不可用时)

```python
if media_roas >= 0.40:
    overall = "大盘健康，继续保持当前节奏"
elif media_roas >= 0.30:
    overall = "效率略低，需关注低效计划"
else:
    overall = "效率偏低，建议收缩消耗、优先止损"
```

---

## 7. 🔴 止损预警表格

### 7.1 数据来源

```python
stop_loss_campaigns = data.get("stop_loss_campaigns", [])
# 结构示例：
[
    {
        "campaign_name": "app-vigloo_channel-fb_...",
        "optimizer": "zane",
        "drama_name": "You Want Some?",
        "country": "US",
        "spend": 847,
        "roas": 0.26
    }
]
```

### 7.2 筛选条件

由数据源预先筛选：
- `spend > $300`
- `roas < 30%`

### 7.3 字段映射

| 列名 | 数据来源 |
|-----|---------|
| 投手 | `stop_loss_campaigns[].optimizer` |
| 计划 | `stop_loss_campaigns[].campaign_name` |
| 剧集 | `stop_loss_campaigns[].drama_name` |
| 国家 | `stop_loss_campaigns[].country` |
| 消耗 | `stop_loss_campaigns[].spend` |
| ROAS | `stop_loss_campaigns[].roas` |
| 建议 | 固定值 "立即关停" |

### 7.4 @投手 建议

表格下方会显示带 @投手 的具体建议（最多2条）：

```
1. **zane** 关停 app-vigloo_channel-f (耗$847, ROAS低)
2. **juria** 关停 app-vigloo_channel-f (耗$621, ROAS低)
```

---

## 8. 🟢 扩量机会表格

### 8.1 数据来源

```python
scale_up_campaigns = data.get("scale_up_campaigns", [])
# 结构示例：
[
    {
        "campaign_name": "app-vigloo_channel-tt_...",
        "optimizer": "kino",
        "drama_name": "Eternal Love after One-Night Stand?",
        "country": "JP",
        "spend": 341,
        "roas": 0.91
    }
]
```

### 8.2 筛选条件

由数据源预先筛选：
- `spend > $300`
- `roas > 50%`

### 8.3 字段映射

| 列名 | 数据来源 |
|-----|---------|
| 投手 | `scale_up_campaigns[].optimizer` |
| 计划 | `scale_up_campaigns[].campaign_name` |
| 剧集 | `scale_up_campaigns[].drama_name` |
| 国家 | `scale_up_campaigns[].country` |
| 消耗 | `scale_up_campaigns[].spend` |
| ROAS | `scale_up_campaigns[].roas` |
| 建议 | 固定值 "大幅提预算" |

### 8.4 @投手 建议

```
1. **kino** Eternal Love after One-Night Stand?(JP) 跑得好(ROAS 91%)，请确认预算充足！
2. **silas** One Night, One Destiny(KR) 跑得好(ROAS 71%)，请确认预算充足！
```

---

## 9. 🌍 地区观察表格

### 9.1 数据来源

```python
country_marginal_roas = data.get("country_marginal_roas", [])
# 结构示例：
[
    {"country": "SG", "spend": 347, "roas": 0.86},
    {"country": "HK", "spend": 596, "roas": 0.63},
    {"country": "JP", "spend": 3054, "roas": 0.61}
]
```

### 9.2 筛选条件

在代码中筛选：
```python
high_roas_countries = [c for c in country_marginal_roas if c.get("roas", 0) > 0.50]
```

### 9.3 字段映射

| 列名 | 数据来源 |
|-----|---------|
| 国家 | `country_marginal_roas[].country` |
| 消耗 | `country_marginal_roas[].spend` |
| ROAS | `country_marginal_roas[].roas` |
| 建议 | 固定值 "关注是否加投" |

---

## 10. 卡片颜色规则

| 条件 | 颜色 | 说明 |
|-----|------|------|
| `media_roas >= 40%` | 绿色 (green) | 大盘健康 |
| `30% <= media_roas < 40%` | 黄色 (yellow) | 效率下滑 |
| `media_roas < 30%` | 红色 (red) | 需关注 |
| 有止损预警 | 红色 (red) | 强制红色 |

**代码逻辑：**
```python
roas_green = config.get("roas_green_threshold", 0.40)
roas_yellow = config.get("roas_yellow_threshold", 0.30)

if media_roas >= roas_green:
    color = "green"
elif media_roas >= roas_yellow:
    color = "yellow"
else:
    color = "red"

# 如果有止损预警，强制红色
if stop_loss_campaigns:
    color = "red"
```

---

## 11. 配置参数

在 `DEFAULT_CONFIG` 中定义：

```python
DEFAULT_CONFIG = {
    "roas_green_threshold": 0.40,    # ROAS >= 40%: 绿色
    "roas_yellow_threshold": 0.30,   # 30% <= ROAS < 40%: 黄色
}
```

---

## 12. @投手 功能

### 12.1 配置映射

在 `OPTIMIZER_USER_MAP` 中配置投手名到飞书 open_id 的映射：

```python
OPTIMIZER_USER_MAP: Dict[str, str] = {
    "kimi": "ou_xxxxxxxxxxxx",
    "kino": "ou_yyyyyyyyyyyy",
    "juria": "ou_zzzzzzzzzzzz",
}
```

### 12.2 格式化逻辑

```python
def _format_at_optimizer(self, optimizer_name: str) -> str:
    if optimizer_name in OPTIMIZER_USER_MAP:
        user_id = OPTIMIZER_USER_MAP[optimizer_name]
        return f"<at id={user_id}></at>"
    return f"**{optimizer_name}**"
```

- 如果配置了映射：显示为飞书 @格式，会真正 @到人
- 如果未配置：显示为 **加粗** 文本
