# 实时战报完整字段映射文档

## 概述

本文档详细说明实时战报中**所有字段**的数据来源、SQL 查询、代码位置和计算方式。

**数据源：** BigQuery `quickbi_data.quickbi_campaigns` 表
**查询方法：** `bigquery_storage.py:query_realtime_report_data()`
**播报方法：** `lark_bot.py:send_realtime_report()`

---

## 第一部分：基础数据和大盘指标

### 1.1 标题和时间

| 飞书显示内容 | 数据字段路径 | 数据来源 | 代码位置 | 说明 |
|------------|------------|---------|---------|------|
| **实时战报 [14:50]** | `data.current_hour` | 系统时间 | `lark_bot.py:1354` | `time.strftime("%H:%M")` |
| **batch_id** | `data.batch_id` | 最新批次ID | `bigquery_storage.py:1172` | 格式：`20251225_140033` |
| **batch_time** | `data.batch_time` | 从 batch_id 解析 | `bigquery_storage.py:1198` | 例如：`14:00` |

### 1.2 大盘健康状态

| 飞书显示内容 | 数据字段路径 | BigQuery SQL | 代码位置 | 计算方式 |
|------------|------------|-------------|---------|---------|
| **🟢 大盘健康：当前 ROAS 49.2%** | `summary.media_roas` | `SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))` | `bigquery_storage.py:1230` | 直接取值 |
| **🔴 大盘预警** | 同上 | 同上 | `lark_bot.py:1415` | `media_roas < 0.40` 时显示 |

**判断逻辑：**
```python
roas_baseline = 0.40  # 40% 基线
if media_roas < roas_baseline:
    显示 "🔴 大盘预警"
else:
    显示 "🟢 大盘健康"
```

---

## 第二部分：⏰ 实时战报板块

### 2.1 消耗数据

| 飞书显示内容 | 数据字段路径 | BigQuery SQL | 代码位置 | 计算方式 |
|------------|------------|-------------|---------|---------|
| **截止当前总耗：$44,111.07** | `summary.total_spend` | `SUM(spend) WHERE batch_id = '20251225_140033'` | `bigquery_storage.py:1227` | 直接取值 |
| **日环比 -6%** | 计算得出 | `(今天 - 昨天) / 昨天 × 100%` | `lark_bot.py:1375` | `(total_spend - yesterday_spend) / yesterday_spend * 100` |

**SQL 查询示例：**
```sql
-- 当前消耗
SELECT SUM(spend) as total_spend
FROM `quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-25' AND batch_id = '20251225_140033'

-- 昨天同时刻消耗（用于日环比）
SELECT SUM(spend) as total_spend
FROM `quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-24' AND batch_id = '20251224_140000'
```

### 2.2 收入数据

| 飞书显示内容 | 数据字段路径 | BigQuery SQL | 代码位置 | 计算方式 |
|------------|------------|-------------|---------|---------|
| **截止当前收入：$21,721.56** | `summary.total_media_revenue` | `SUM(media_user_revenue)` | `bigquery_storage.py:1229` | ✅ 使用媒体归因收入 |
| **日环比 -5%** | 计算得出 | `(今天 - 昨天) / 昨天 × 100%` | `lark_bot.py:1376` | `(total_revenue - yesterday_revenue) / yesterday_revenue * 100` |

**重要说明：**
- ✅ 收入字段使用 `media_user_revenue`（媒体归因收入），不是 `new_user_revenue`
- ✅ 代码位置：`lark_bot.py:1364` 和 `lark_bot.py:1370`

### 2.3 ROAS 数据

| 飞书显示内容 | 数据字段路径 | BigQuery SQL | 代码位置 | 计算方式 |
|------------|------------|-------------|---------|---------|
| **当前 Media ROAS：49.2%** | `summary.media_roas` | `SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))` | `bigquery_storage.py:1230` | 直接取值 |
| **日环比 +0.4%** | 计算得出 | `今天 ROAS - 昨天 ROAS` | `lark_bot.py:1377` | `media_roas - yesterday_media_roas` |

**重要说明：**
- ✅ ROAS 环比使用**绝对值差异**，不是百分比变化
- 例如：49.2% - 48.8% = +0.4%（不是 +0.8%）

### 2.4 小时环比数据

| 飞书显示内容 | 数据字段路径 | BigQuery SQL | 代码位置 | 计算方式 |
|------------|------------|-------------|---------|---------|
| **新增消耗 (14:00 vs 13:00)：$3,567.52** | 计算得出 | `当前累计 - 上小时累计` | `lark_bot.py:1389` | `total_spend - prev_total_spend` |
| **环比 +9%** | 计算得出 | `新增消耗 / 上小时累计 × 100%` | `lark_bot.py:1400` | `hourly_spend_delta / prev_total_spend * 100` |
| **过去1小时 ROAS 趋势：↘️ 下降 0.9%** | 计算得出 | `当前 ROAS - 上小时 ROAS` | `lark_bot.py:1390` | `media_roas - prev_roas` |

**数据来源：**
- 当前数据：`summary.total_spend` 和 `summary.media_roas`
- 上小时数据：`prev_hour_summary.total_spend` 和 `prev_hour_summary.media_roas`

---

## 第三部分：🔍 谁在花钱？(投手表格)

### 3.1 数据来源

**投手汇总查询：** `bigquery_storage.py:1236-1248`

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

### 3.2 表格字段映射

| 列名 | 数据字段路径 | SQL 字段 | 代码位置 | 计算方式 |
|-----|------------|---------|---------|---------|
| **投手** | `optimizer_spend[].optimizer` | `optimizer` | `lark_bot.py:1486` | 直接取值 |
| **新增消耗** | 计算得出 | `当前 spend - 上小时 spend` | `lark_bot.py:1489` | `current_spend - prev_spend` |
| **累计消耗** | `optimizer_spend[].spend` | `SUM(spend)` | `lark_bot.py:1487` | 当日累计消耗 |
| **当前Media ROAS** | `optimizer_spend[].roas` | `SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))` | `lark_bot.py:1509` | Media ROAS |
| **主力计划** | `optimizer_spend[].top_campaigns` | 取消耗最高的计划 | `lark_bot.py:1492-1503` | 格式：`drama_name(country)` |

### 3.3 状态标识

| 状态 | 触发条件 | 代码位置 |
|-----|---------|---------|
| 🔥 | 新增消耗 > $100 | `lark_bot.py:1520` |
| 🐢 缓慢 | 新增消耗 < $50 | `lark_bot.py:1520` |
| (空) | $50 ≤ 新增消耗 ≤ $100 | `lark_bot.py:1520` |

**计算逻辑：**
```python
delta = current_spend - prev_spend
if delta > 100:
    status = "🔥"
elif delta < 50:
    status = "🐢 缓慢"
else:
    status = ""
```

---

## 第四部分：🔴 止损预警表格

### 4.1 数据来源

**SQL 查询：** `bigquery_storage.py:1267-1283`

```sql
SELECT
    campaign_id, campaign_name, optimizer, drama_name, country,
    SUM(spend) as spend,
    SUM(media_user_revenue) as revenue,
    SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
FROM `quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-25' AND batch_id = '20251225_140033'
GROUP BY campaign_id, campaign_name, optimizer, drama_name, country
HAVING spend > 300 AND (revenue = 0 OR SAFE_DIVIDE(revenue, spend) < 0.30)
ORDER BY spend DESC
LIMIT 10
```

### 4.2 筛选条件

| 条件 | 阈值 | 说明 |
|-----|------|------|
| 消耗 | > $300 | 只关注消耗较大的计划 |
| ROAS | < 30% | 低于止损线 |

### 4.3 表格字段映射

| 列名 | 数据字段路径 | SQL 字段 | 说明 |
|-----|------------|---------|------|
| **投手** | `stop_loss_campaigns[].optimizer` | `optimizer` | 直接取值 |
| **计划** | `stop_loss_campaigns[].campaign_name` | `campaign_name` | 广告系列名称 |
| **剧集** | `stop_loss_campaigns[].drama_name` | `drama_name` | 剧集名称 |
| **国家** | `stop_loss_campaigns[].country` | `country` | 国家代码 |
| **消耗** | `stop_loss_campaigns[].spend` | `SUM(spend)` | 当日累计消耗 |
| **ROAS** | `stop_loss_campaigns[].roas` | `SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))` | Media ROAS |
| **建议** | 固定值 | - | "立即关停" |

---

## 第五部分：🟢 扩量机会表格

### 5.1 数据来源

**SQL 查询：** `bigquery_storage.py:1286-1302`

```sql
SELECT
    campaign_id, campaign_name, optimizer, drama_name, country,
    SUM(spend) as spend,
    SUM(media_user_revenue) as revenue,
    SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
FROM `quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-25' AND batch_id = '20251225_140033'
GROUP BY campaign_id, campaign_name, optimizer, drama_name, country
HAVING spend > 300 AND SAFE_DIVIDE(revenue, spend) > 0.50
ORDER BY roas DESC
LIMIT 10
```

### 5.2 筛选条件

| 条件 | 阈值 | 说明 |
|-----|------|------|
| 消耗 | > $300 | 只关注消耗较大的计划 |
| ROAS | > 50% | 高于扩量线 |

### 5.3 表格字段映射

| 列名 | 数据字段路径 | SQL 字段 | 说明 |
|-----|------------|---------|------|
| **投手** | `scale_up_campaigns[].optimizer` | `optimizer` | 直接取值 |
| **计划** | `scale_up_campaigns[].campaign_name` | `campaign_name` | 广告系列名称 |
| **剧集** | `scale_up_campaigns[].drama_name` | `drama_name` | 剧集名称 |
| **国家** | `scale_up_campaigns[].country` | `country` | 国家代码 |
| **消耗** | `scale_up_campaigns[].spend` | `SUM(spend)` | 当日累计消耗 |
| **ROAS** | `scale_up_campaigns[].roas` | `SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))` | Media ROAS |
| **建议** | 固定值 | - | "大幅提预算" |

---

## 第六部分：🌍 地区观察表格

### 6.1 数据来源

**SQL 查询：** `bigquery_storage.py:1305-1318`

```sql
SELECT
    country,
    SUM(spend) as spend,
    SUM(media_user_revenue) as revenue,
    SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend)) as roas
FROM `quickbi_data.quickbi_campaigns`
WHERE stat_date = '2025-12-25' AND batch_id = '20251225_140033'
  AND country IS NOT NULL AND country != ''
GROUP BY country
HAVING spend > 100
ORDER BY roas DESC
```

### 6.2 筛选条件

| 条件 | 阈值 | 代码位置 |
|-----|------|---------|
| 消耗 | > $100 | SQL HAVING 子句 |
| ROAS | > 50% | `lark_bot.py` 中过滤 |

### 6.3 表格字段映射

| 列名 | 数据字段路径 | SQL 字段 | 说明 |
|-----|------------|---------|------|
| **国家** | `country_marginal_roas[].country` | `country` | 国家代码（如 US, KR, JP） |
| **消耗** | `country_marginal_roas[].spend` | `SUM(spend)` | 该国家当日累计消耗 |
| **ROAS** | `country_marginal_roas[].roas` | `SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))` | Media ROAS |
| **建议** | 固定值 | - | "关注是否加投" |

---

## 第七部分：数据查询流程总结

### 7.1 三个关键 batch_id

实时播报需要查询 **3 个时间点** 的数据：

| batch 类型 | 用途 | 查询逻辑 | 代码位置 |
|-----------|------|---------|---------|
| **当前 batch** | 当前累计数据 | 获取今天最新的 batch | `bigquery_storage.py:1172` |
| **昨天同整点 batch** | 日环比对比 | 查找昨天当前整点（±10分钟）的 batch | `bigquery_storage.py:1320-1341` |
| **上一整点 batch** | 小时环比对比 | 查找今天上一整点（±10分钟）的 batch | `bigquery_storage.py:1362-1393` |

### 7.2 查询示例

**场景：** 现在是 2025-12-25 14:50

```sql
-- 1. 当前 batch (14:00)
WHERE stat_date = '2025-12-25' AND batch_id = '20251225_140033'

-- 2. 昨天同整点 batch (昨天 14:00)
WHERE stat_date = '2025-12-24' 
  AND batch_id >= '20251224_140000' 
  AND batch_id <= '20251224_141000'
ORDER BY batch_id DESC LIMIT 1

-- 3. 上一整点 batch (今天 13:00)
WHERE stat_date = '2025-12-25'
  AND batch_id >= '20251225_130000'
  AND batch_id <= '20251225_131000'
ORDER BY batch_id DESC LIMIT 1
```

---

## 第八部分：关键说明

### 8.1 收入字段确认

✅ **实时战报已使用 `media_user_revenue`（媒体归因收入）**

| 位置 | 代码 | 说明 |
|-----|------|------|
| 飞书播报 | `lark_bot.py:1364` | `total_revenue = summary.get("total_media_revenue", 0)` |
| 昨天数据 | `lark_bot.py:1370` | `yesterday_revenue = yesterday_summary.get("total_media_revenue", 0)` |
| BigQuery 查询 | `bigquery_storage.py:1229` | `SUM(media_user_revenue) as total_media_revenue` |

**对比：**
- ❌ 旧方案：`new_user_revenue`（新用户首日收入，较保守）
- ✅ 新方案：`media_user_revenue`（媒体归因收入，更准确）
- 差异：约 26.7%（根据 2025-12-25 10:00 数据）

### 8.2 ROAS 计算方式

✅ **所有 ROAS 均为 Media ROAS**

```sql
Media ROAS = SAFE_DIVIDE(SUM(media_user_revenue), SUM(spend))
```

**适用范围：**
- 大盘 ROAS：`summary.media_roas`
- 投手 ROAS：`optimizer_spend[].roas`
- 止损预警 ROAS：`stop_loss_campaigns[].roas`
- 扩量机会 ROAS：`scale_up_campaigns[].roas`
- 地区 ROAS：`country_marginal_roas[].roas`

### 8.3 环比计算特殊性

**ROAS 环比/趋势使用绝对值差异，不是百分比变化**

| 指标 | 计算方式 | 示例 |
|-----|---------|------|
| 消耗环比 | `(今天 - 昨天) / 昨天 × 100%` | `(44111 - 47000) / 47000 = -6%` |
| 收入环比 | `(今天 - 昨天) / 昨天 × 100%` | `(21722 - 22800) / 22800 = -5%` |
| **ROAS 环比** | `今天 ROAS - 昨天 ROAS` | `49.2% - 48.8% = +0.4%` ✅ |
| ~~ROAS 环比~~ | ~~`(今天 - 昨天) / 昨天 × 100%`~~ | ~~`(0.492 - 0.488) / 0.488 = +0.8%`~~ ❌ |

**原因：** ROAS 本身就是百分比，再计算百分比变化会导致数值失真。

---

## 第九部分：配置参数

### 9.1 阈值配置

| 参数名 | 默认值 | 用途 | 代码位置 |
|-------|--------|------|---------|
| `roas_green_threshold` | 0.40 (40%) | 大盘健康基线 | `lark_bot.py:1414` |
| `roas_yellow_threshold` | 0.30 (30%) | 止损预警线 | 配置文件 |
| 止损消耗阈值 | $300 | 止损预警最小消耗 | `bigquery_storage.py:1280` |
| 止损 ROAS 阈值 | 30% | 止损预警 ROAS 上限 | `bigquery_storage.py:1280` |
| 扩量消耗阈值 | $300 | 扩量机会最小消耗 | `bigquery_storage.py:1299` |
| 扩量 ROAS 阈值 | 50% | 扩量机会 ROAS 下限 | `bigquery_storage.py:1299` |
| 地区消耗阈值 | $100 | 地区观察最小消耗 | `bigquery_storage.py:1316` |

### 9.2 状态判断逻辑

**大盘健康状态：**
```python
if media_roas >= 0.40:
    显示 "🟢 大盘健康"
else:
    显示 "🔴 大盘预警"
```

**投手状态标识：**
```python
delta = current_spend - prev_spend
if delta > 100:
    status = "🔥"
elif delta < 50:
    status = "🐢 缓慢"
else:
    status = ""
```

---

## 第十部分：完整数据示例

### 10.1 场景说明

**时间：** 2025-12-25 14:50
**当前 batch_id：** `20251225_140033`
**上小时 batch_id：** `20251225_130029`
**昨天同时刻 batch_id：** `20251224_140015`

### 10.2 数据流转示例

```python
# 1. BigQuery 查询返回的数据结构
data = {
    "date": "2025-12-25",
    "current_hour": "14:50",
    "batch_id": "20251225_140033",
    "batch_time": "14:00",
    "data_delayed": False,
    
    # 当前汇总数据
    "summary": {
        "total_spend": 44111.07,
        "total_media_revenue": 21721.56,
        "media_roas": 0.492
    },
    
    # 昨天同时刻数据
    "yesterday_summary": {
        "total_spend": 47000.00,
        "total_media_revenue": 22800.00,
        "media_roas": 0.488
    },
    
    # 上一小时数据
    "prev_hour_summary": {
        "total_spend": 40543.55,
        "media_roas": 0.501,
        "optimizer_data": [
            {"optimizer": "juria", "spend": 9400},
            {"optimizer": "kino", "spend": 8000}
        ]
    },
    
    # 投手消耗明细
    "optimizer_spend": [
        {
            "optimizer": "juria",
            "spend": 9962,
            "revenue": 3913,
            "roas": 0.393,
            "top_campaigns": [
                {"name": "...", "drama_name": "Was It Just...", "country": "KR"}
            ]
        }
    ]
}
```

### 10.3 飞书播报计算过程

```python
# 消耗日环比
daily_spend_change = (44111.07 - 47000.00) / 47000.00 * 100 = -6%

# 收入日环比
daily_revenue_change = (21721.56 - 22800.00) / 22800.00 * 100 = -5%

# ROAS 日环比（绝对值）
daily_roas_change = 0.492 - 0.488 = +0.004 = +0.4%

# 新增消耗
hourly_spend_delta = 44111.07 - 40543.55 = 3567.52

# 小时环比
hourly_change = 3567.52 / 40543.55 * 100 = +9%

# ROAS 趋势（绝对值）
roas_trend = 0.492 - 0.501 = -0.009 = -0.9%
```

---

## 总结

### 核心要点

1. ✅ **数据源统一**：所有数据来自 `quickbi_data.quickbi_campaigns` 表
2. ✅ **收入字段正确**：使用 `media_user_revenue`（媒体归因收入）
3. ✅ **ROAS 计算统一**：所有 ROAS 均为 Media ROAS
4. ✅ **环比计算特殊**：ROAS 环比使用绝对值差异
5. ✅ **batch_id 机制**：通过 batch_id 实现时间点数据快照

### 文档维护

- **创建时间：** 2025-12-25
- **最后更新：** 2025-12-25
- **维护人员：** Claude Code
- **相关文档：** 
  - `DATA_ARCHITECTURE.md` - 数据架构总览
  - `realtime_report_fields.md` - 实时播报字段说明
  - `收入指标确认说明.md` - 收入指标选择说明

---
