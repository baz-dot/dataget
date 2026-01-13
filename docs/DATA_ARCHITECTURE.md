# 数据架构文档

## 概述

本项目是一个广告数据采集、存储和播报系统，主要服务于短剧出海业务的广告投放监控。

---

## 1. 数据采集层

### 1.1 QuickBI 数据采集 (主要数据源)

| 项目 | 说明 |
|------|------|
| **采集方式** | Cloud Run Job 定时调用 Quick BI API |
| **数据内容** | 广告系列投放数据 (消耗、收入、ROAS等) |
| **调度频率** | 每小时执行一次 |
| **部署脚本** | `deploy_quickbi.sh` |

**环境变量配置：**
```bash
QUICKBI_ACCESS_ID=xxx        # Quick BI Access ID
QUICKBI_ACCESS_SECRET=xxx    # Quick BI Access Secret
QUICKBI_CUBE_ID=xxx          # 数据立方体 ID
BQ_PROJECT_ID=xxx            # GCP 项目 ID
QUICKBI_BQ_DATASET_ID=quickbi_data  # BigQuery 数据集
```

### 1.2 XMP 素材数据采集

| 项目 | 说明 |
|------|------|
| **采集器** | `scraper.py` (XMPScraper) |
| **数据源** | https://xmp.mobvista.com |
| **数据内容** | 广告素材效果数据 (CTR、CVR、消耗等) |
| **采集方式** | Playwright 浏览器自动化 |

**环境变量配置：**
```bash
XMP_USERNAME=xxx             # XMP 登录用户名
XMP_PASSWORD=xxx             # XMP 登录密码
```

### 1.3 ADX (DataEye) 竞品素材采集

| 项目 | 说明 |
|------|------|
| **采集器** | `dataeye_scraper.py` (DataEyeScraper) |
| **数据源** | DataEye 海外版 + 国内版 |
| **数据内容** | 竞品广告素材、视频 |
| **调度频率** | 每6小时执行一次 |
| **部署脚本** | `deploy_adx.sh` / `deploy_adx_cn.sh` |

**两个版本：**
| 版本 | URL | 说明 |
|------|-----|------|
| 海外版 | oversea-v2.dataeye.com | 海外短剧素材 |
| 国内版 | adxray-app.dataeye.com | 国内行业素材 |

**环境变量配置：**
```bash
DATAEYE_USERNAME=xxx         # DataEye 登录用户名
DATAEYE_PASSWORD=xxx         # DataEye 登录密码
DATAEYE_VERSION=both         # both/overseas/china
DATAEYE_MAX_RECORDS=100      # 最大采集记录数
DATAEYE_DOWNLOAD_VIDEOS=true # 是否下载视频
```

---

## 2. 数据存储层 (BigQuery)

### 2.1 数据集概览

| 数据集 | 用途 |
|--------|------|
| `quickbi_data` | Quick BI 广告投放数据 (日报/实时播报) |
| `xmp_data` | XMP 素材效果数据 |
| `adx_data` | DataEye 竞品素材数据 |

### 2.2 quickbi_data 数据集

#### 表: quickbi_campaigns

广告系列维度的投放数据，每小时同步一次。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| stat_date | DATE | 统计日期 |
| batch_id | STRING | 批次ID (格式: YYYYMMDD_HHMMSS) |
| campaign_id | STRING | 广告系列 ID |
| campaign_name | STRING | 广告系列名称 |
| status | STRING | 投放状态 (Active/Stopped) |
| optimizer | STRING | 优化师 |
| drama_id | STRING | 剧集 ID |
| drama_name | STRING | 剧集名称 |
| channel | STRING | 渠道 (google/meta等) |
| country | STRING | 国家/地区 |
| spend | FLOAT | 消耗 ($) |
| impressions | INTEGER | 展示数 |
| clicks | INTEGER | 点击数 |
| new_users | INTEGER | 新用户数 |
| new_user_revenue | FLOAT | 新用户收入 ($) |
| new_payers | INTEGER | 付费用户数 |
| fetched_at | TIMESTAMP | 数据采集时间 |

#### 表: hourly_snapshots

小时级快照，用于计算环比变化。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| snapshot_time | TIMESTAMP | 快照时间 |
| stat_date | DATE | 统计日期 |
| hour | INTEGER | 小时 (0-23) |
| total_spend | FLOAT | 总消耗 |
| total_revenue | FLOAT | 总收入 |
| d0_roas | FLOAT | D0 ROAS |
| optimizer_data | JSON | 投手维度数据 |
| batch_id | STRING | 对应的批次ID |

---

## 3. 数据消费层 (飞书播报)

### 3.1 日报播报

**触发时间：** 每日 09:10
**数据范围：** T-1 日全天数据
**查询方法：** `BigQueryUploader.query_daily_report_data()`
**飞书方法：** `LarkBot.send_daily_report()`

#### 飞书播报板块与数据来源

| 板块 | 播报内容 | 数据来源 | 说明 |
|------|----------|----------|------|
| 📅 大盘综述 | 总消耗、综合ROAS、环比 | BigQuery `quickbi_campaigns` | T-1 最新 batch 聚合 |
| 💡 策略建议 | 放量剧目、机会市场 | Gemini AI + BigQuery | `_generate_strategy_insights()` |
| 🤖 AI洞察 | 核心洞察、异常点 | ChatGPT API | `chatgpt_advisor.analyze_daily_data()` |
| 🏆 投手表现 | 投手排行、评级 | BigQuery `quickbi_campaigns` | GROUP BY optimizer |
| 📊 数据明细 | 分投手/剧集/国家表 | BigQuery `quickbi_campaigns` | 多维度聚合 |

#### 返回字段详解 (含数据来源)

```python
{
    "date": "2025-12-23",           # 报告日期 (T-1)

    # ===== 来源: Quick BI 广告投放数据，T-1日汇总 =====
    "summary": {
        "total_spend": 50000.00,     # 消耗汇总 (Quick BI spend 字段)
        "total_revenue": 20000.00,   # 收入汇总 (Quick BI new_user_revenue 字段)
        "global_roas": 0.40          # 计算字段: 收入 / 消耗
    },

    # ===== 来源: Quick BI 广告投放数据，T-2日汇总 (用于环比) =====
    "summary_prev": {
        "total_spend": 48000.00,     # T-2 日消耗
        "total_revenue": 19000.00,   # T-2 日收入
        "global_roas": 0.396         # T-2 日 ROAS
    },

    # ===== 来源: Quick BI 按投手分组汇总，按消耗降序 =====
    "optimizers": [
        {
            "name": "张三",           # 投手名称 (Quick BI optimizer 字段)
            "spend": 15000.00,        # 该投手消耗汇总
            "revenue": 6000.00,       # 该投手收入汇总
            "roas": 0.40,             # 计算: 收入 / 消耗
            "campaign_count": 25,     # 该投手广告系列数量
            "top_campaign": "xxx"     # 该投手消耗最高的广告系列名称
        }
    ],

    # ===== 来源: Quick BI 按剧集分组汇总，取消耗前5 =====
    "dramas_top5": [
        {
            "name": "霸道总裁",        # 剧集名称 (Quick BI drama_name 字段)
            "spend": 8000.00,         # 该剧集消耗汇总
            "revenue": 4000.00,       # 该剧集收入汇总
            "roas": 0.50              # 计算: 收入 / 消耗
        }
    ],

    # ===== 来源: Quick BI 按国家分组汇总，取消耗前5 =====
    "countries_top5": [
        {
            "name": "US",             # 国家代码 (Quick BI country 字段)
            "spend": 20000.00,        # 该国家消耗汇总
            "revenue": 8000.00,       # 该国家收入汇总
            "roas": 0.40              # 计算: 收入 / 消耗
        }
    ],

    # ===== 来源: Quick BI 筛选消耗>$1000且ROAS>45%的剧集 =====
    "scale_up_dramas": [
        {
            "name": "甜蜜爱情",        # 剧集名称
            "spend": 5000.00,         # 消耗
            "roas": 0.55              # ROAS
        }
    ],

    # ===== 来源: Quick BI 筛选非主投国家(排除US/KR/JP)且消耗>$100且ROAS>50% =====
    "opportunity_markets": [
        {
            "drama_name": "霸道总裁",  # 剧集名称
            "country": "TW",          # 国家代码
            "spend": 500.00,          # 消耗
            "roas": 0.60              # ROAS
        }
    ]
}
```

### 3.2 实时播报

**触发时间：** 每日 09:10 - 23:10，每小时10分
**数据范围：** 当日累计数据
**查询方法：** `BigQueryUploader.query_realtime_report_data()`
**飞书方法：** `LarkBot.send_realtime_report()`

#### 飞书播报板块与数据来源

| 板块 | 播报内容 | 数据来源 | 说明 |
|------|----------|----------|------|
| ⚠️ 数据延迟警告 | API更新时间超2小时 | batch_id 解析 | 自动检测 |
| ⏰ 实时战报 | 总耗、D0 ROAS、新增消耗 | BigQuery `quickbi_campaigns` | 当日最新batch |
| 🔍 谁在花钱 | 投手消耗增量、主力计划 | BigQuery + hourly_snapshots | 当前batch vs 上一batch |
| ⚡️ 操作建议 | GPT分析、节奏评估 | ChatGPT API | `chatgpt_advisor.analyze_realtime_data()` |
| 📊 整体态势 | AI止损/扩量建议 | Gemini AI | `_generate_realtime_insights()` |

#### 返回字段详解 (含数据来源)

```python
{
    # ===== 来源: 系统时间 + 批次ID解析 =====
    "date": "2025-12-24",            # 当前日期
    "current_hour": "10:30",         # 当前时间
    "batch_id": "20251224_100033",   # Quick BI 最新批次ID
    "batch_time": "10:00",           # 从批次ID解析的时间点
    "api_update_time": "2025-12-24 10:00:33",  # API数据更新时间
    "data_delayed": False,           # 数据是否延迟超过2小时

    # ===== 来源: Quick BI 广告投放数据，当日汇总 =====
    "summary": {
        "total_spend": 33803.34,     # 当日累计消耗 (Quick BI spend 字段)
        "total_revenue": 11443.00,   # 当日累计收入 (Quick BI new_user_revenue 字段)
        "d0_roas": 0.3385            # 计算字段: 收入 / 消耗
    },

    # ===== 来源: Quick BI 按投手分组汇总，按消耗降序 =====
    "optimizer_spend": [
        {
            "optimizer": "张三",      # 投手名称 (Quick BI optimizer 字段)
            "spend": 10000.00,        # 该投手当日累计消耗
            "revenue": 4000.00,       # 该投手当日累计收入
            "roas": 0.40,             # 计算: 收入 / 消耗
            # ===== 来源: Quick BI 按投手+广告系列分组，取消耗最高的 =====
            "top_campaigns": [
                {
                    "campaign_name": "xxx",   # 广告系列名称
                    "drama_name": "霸道总裁",  # 剧集名称
                    "country": "US",          # 国家
                    "spend": 2000.00          # 消耗
                }
            ]
        }
    ],

    # ===== 来源: Quick BI 筛选消耗>$300且ROAS<30%的广告系列 =====
    "stop_loss_campaigns": [
        {
            "campaign_id": "123",     # 广告系列ID
            "campaign_name": "xxx",   # 广告系列名称
            "optimizer": "张三",       # 投手名称
            "drama_name": "霸道总裁",  # 剧集名称
            "country": "US",          # 国家
            "spend": 500.00,          # 消耗
            "revenue": 100.00,        # 收入
            "roas": 0.20              # ROAS (低于30%触发预警)
        }
    ],

    # ===== 来源: Quick BI 筛选消耗>$300且ROAS>50%的广告系列 =====
    "scale_up_campaigns": [
        {
            "campaign_id": "456",     # 广告系列ID
            "campaign_name": "xxx",   # 广告系列名称
            "optimizer": "李四",       # 投手名称
            "drama_name": "甜蜜爱情",  # 剧集名称
            "country": "KR",          # 国家
            "spend": 800.00,          # 消耗
            "revenue": 500.00,        # 收入
            "roas": 0.625             # ROAS (高于50%建议扩量)
        }
    ],

    # ===== 来源: Quick BI 按国家分组汇总，筛选消耗>$100 =====
    "country_marginal_roas": [
        {
            "country": "US",          # 国家代码
            "spend": 15000.00,        # 该国家消耗汇总
            "revenue": 6000.00,       # 该国家收入汇总
            "roas": 0.40              # 计算: 收入 / 消耗
        }
    ]
}
```

### 3.3 环比数据

**查询方法：** `BigQueryUploader.get_previous_batch_data()`
**数据来源：** BigQuery `quickbi_data.quickbi_campaigns` 表，查询上一个 batch_id 的数据

> 注意：环比数据直接从 quickbi_campaigns 表查询，不依赖 hourly_snapshots 快照表。
> 通过比较当日不同 batch_id 的数据来计算环比变化。

用于计算实时播报的环比变化（与上一个批次对比）。

```python
{
    # ===== 来源: Quick BI 上一批次数据汇总 =====
    "batch_id": "20251224_090033",   # 上一批次ID
    "batch_time": "09:00",           # 批次时间点
    "total_spend": 30371.08,         # 上一批次总消耗
    "d0_roas": 0.32,                 # 上一批次 ROAS

    # ===== 来源: Quick BI 上一批次按投手分组汇总 =====
    "optimizer_data": [              # 上一批次投手数据 (用于计算投手消耗增量)
        {
            "optimizer": "张三",      # 投手名称
            "spend": 8000.00          # 上一批次该投手消耗
        }
    ]
}
```

**环比计算逻辑 (在 LarkBot.send_realtime_report 中)：**
- 消耗增量 = 当前 total_spend - 上批次 total_spend
- ROAS 变化 = 当前 d0_roas - 上批次 d0_roas
- 投手增量 = 当前投手 spend - 上批次投手 spend

---

## 4. 定时任务配置

### 4.1 本地调度器

文件：`lark/scheduler_local.py`

| 任务 | 时间 | 说明 |
|------|------|------|
| 日报 | 每日 09:10 | 发送 T-1 日报 |
| 实时播报 | 09:10-23:10 每小时10分 | 发送当日实时数据 |

### 4.2 Cloud Run Jobs

| Job 名称 | 调度 | 说明 |
|----------|------|------|
| quickbi-data-scraper | 每小时 | Quick BI 数据同步 |
| adx-data-scraper | 每6小时 | DataEye 素材采集 |

---

## 5. 关键指标说明

| 指标 | 计算公式 | 说明 |
|------|----------|------|
| D0 ROAS | new_user_revenue / spend | 当日新用户收入回报率 |
| CPI | spend / new_users | 单用户获取成本 |
| CTR | clicks / impressions | 点击率 |
| CVR | new_users / clicks | 转化率 |

### 预警阈值

| 类型 | 条件 | 说明 |
|------|------|------|
| 止损预警 | Spend > $300 且 ROAS < 30% | 建议关停或优化 |
| 扩量机会 | Spend > $300 且 ROAS > 50% | 建议加大预算 |
| 放量剧目 | Spend > $1000 且 ROAS > 45% | 表现优秀的剧集 |
| 机会市场 | 非主投国家, Spend > $100, ROAS > 50% | 潜力市场 |

### 数据校验机制

实时播报前会进行以下数据校验 (在 `scheduler_local.py` 中)：

| 校验项 | 条件 | 触发动作 |
|--------|------|----------|
| 消耗为0 | total_spend == 0 | 发送告警，跳过播报 |
| 数据时效性 | 延迟超过70分钟 | 发送告警，跳过播报 |
| 数据延迟 | 延迟超过2小时 | 发送告警，跳过播报 |
| 消耗无变化 | 当前 <= 上批次 | 等待5分钟重试 |
| 单小时消耗异常 | 增量 > $50,000 | 发送告警，继续播报 |
| ROAS突变 | 变化 > 50% | 发送告警，继续播报 |
| 数据完整性 | 缺失关键字段 | 发送告警，跳过播报 |
| 投手数量 | 为0或少于3人 | 发送告警 |
| 同比消耗 | 与昨日同时段波动 > 50% | 发送告警，继续播报 |
| 同比投手数 | 比昨日减少 > 50% | 发送告警，继续播报 |

---

## 6. 环境变量汇总

```bash
# GCP 配置
BQ_PROJECT_ID=fleet-blend-469520-n7
BQ_DATASET_ID=quickbi_data

# 飞书配置
LARK_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxx
LARK_SECRET=xxx

# Quick BI 配置
QUICKBI_ACCESS_ID=xxx
QUICKBI_ACCESS_SECRET=xxx

# AI 分析 (可选)
GEMINI_API_KEY=xxx
OPENAI_API_KEY=xxx

# 日报 BI 链接
DAILY_REPORT_BI_LINK=https://bi.aliyun.com/xxx
```
