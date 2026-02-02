# CLAUDE.md
必须先跟我确认需求之后再执行

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个广告数据采集、存储和播报系统,主要服务于短剧出海业务的广告投放监控。系统通过定时采集多个数据源(QuickBI、XMP、DataEye),存储到 BigQuery,并通过飞书机器人进行日报和实时播报。

## 核心架构

### 三层架构

1. **数据采集层** (Scrapers)
   - `scraper.py` - XMP 素材数据采集器 (Playwright 自动化)
   - `dataeye_scraper.py` - DataEye 竞品素材采集器 (海外版 + 国内版)
   - `xmp/xmp_scheduler.py` - XMP 多渠道数据定时抓取 (TikTok + Meta)
   - QuickBI API 采集 - 广告投放数据 (通过 Cloud Run Job)

2. **数据存储层** (BigQuery)
   - `bigquery_storage.py` - BigQuery 数据上传和查询模块
   - 数据集: `quickbi_data`, `xmp_data`, `adx_data`
   - 核心表: `quickbi_campaigns`, `hourly_snapshots`

3. **数据消费层** (Lark Bot)
   - `lark/lark_bot.py` - 飞书机器人播报模块
   - `lark/gemini_advisor.py` - Gemini AI 策略分析
   - `lark/chatgpt_advisor.py` - ChatGPT AI 洞察分析

### 调度系统

- `scheduler.py` - 主调度器 (BrainScheduler),整合规则引擎和消息推送
- `lark/scheduler_local.py` - 本地定时任务调度器
  - 日报: 每日 09:10 (T-1 日数据)
  - 实时播报: 每日 09:10-23:10,每小时 10 分

### 规则引擎

- `rule_engine.py` - 广告投放规则引擎
  - 止损预警: Spend > $300 且 ROAS < 30%
  - 扩量机会: Spend > $300 且 ROAS > 50%
  - 素材刷新建议: CTR 下降超过 20%

## 常用命令

### 环境设置

```bash
# 安装依赖
pip install -r requirements.txt

# 安装 Playwright 浏览器 (用于 XMP/DataEye 爬虫)
playwright install chromium

# 配置环境变量
cp .env.example .env  # 然后编辑 .env 文件
```

### 本地开发

```bash
# 运行本地调度器 (日报 + 实时播报)
python lark/scheduler_local.py

# 测试日报播报
python lark/test_daily_report.py

# 测试实时播报
python lark/test_realtime_report.py

# 测试 ChatGPT 分析
python lark/test_chatgpt_advisor.py
```

### 数据采集

```bash
# 运行 XMP 素材采集
python scraper.py

# 运行 DataEye 素材采集
python dataeye_scraper.py
```

### 数据查询和验证

```bash
# 检查 BigQuery 数据
python check_spend.py              # 检查消耗数据
python check_realtime_data.py      # 检查实时数据
python check_batch_data.py         # 检查批次数据
python check_yesterday_comparison.py  # 检查昨日对比数据
```

### Cloud Run 部署

```bash
# 部署 QuickBI 数据采集器
./deploy_quickbi.sh

# 部署 DataEye 海外版采集器
./deploy_adx.sh

# 部署 DataEye 国内版采集器
./deploy_adx_cn.sh

# 部署实时播报调度器
./deploy_scheduler.sh

# 配置 Cloud Scheduler 定时任务
./setup_scheduler.sh
```

## 关键数据流

### 日报数据流 (Daily Report)

1. **数据查询**: `BigQueryUploader.query_daily_report_data(date=T-1)`
   - 从 `quickbi_campaigns` 表查询 T-1 日最新 batch 数据
   - 按投手、剧集、国家维度聚合
   - 计算放量剧目 (Spend > $1000, ROAS > 45%)
   - 计算机会市场 (非主投国家, Spend > $100, ROAS > 50%)

2. **AI 分析**:
   - Gemini AI: 生成策略建议 (`_generate_strategy_insights()`)
   - ChatGPT: 生成核心洞察 (`chatgpt_advisor.analyze_daily_data()`)

3. **飞书播报**: `LarkBot.send_daily_report()`
   - 大盘综述 (总消耗、ROAS、环比)
   - 策略建议 (放量剧目、机会市场)
   - AI 洞察
   - 投手表现排行
   - 数据明细表格

### 实时播报数据流 (Realtime Report)

1. **数据查询**: `BigQueryUploader.query_realtime_report_data()`
   - 从 `quickbi_campaigns` 表查询当日最新 batch 数据
   - 按投手维度聚合，获取 Top 3 主力计划
   - 筛选止损预警 (Spend > $300, ROAS < 30%)
   - 筛选扩量机会 (Spend > $300, ROAS > 50%)
   - 计算国家边际 ROAS

2. **环比数据**: `BigQueryUploader.get_previous_batch_data()`
   - 查询上一个 batch_id 的数据
   - 计算消耗增量、ROAS 变化、投手增量

3. **数据校验** (在 `scheduler_local.py` 中):
   - 消耗为 0 → 跳过播报
   - 数据延迟超过 70 分钟 → 跳过播报
   - 消耗无变化 → 等待 5 分钟重试
   - 单小时消耗异常 (增量 > $50,000) → 发送告警

4. **AI 分析**:
   - ChatGPT: 生成操作建议 (`chatgpt_advisor.analyze_realtime_data()`)
   - Gemini AI: 生成整体态势分析 (`_generate_realtime_insights()`)

5. **飞书播报**: `LarkBot.send_realtime_report()`
   - 数据延迟警告 (如有)
   - 实时战报 (总耗、D0 ROAS、新增消耗)
   - 谁在花钱 (投手消耗增量、主力计划)
   - 操作建议 (GPT 分析、节奏评估)
   - 整体态势 (AI 止损/扩量建议)

## 重要技术细节

### BigQuery 数据结构

**quickbi_campaigns 表** (核心表):
- `batch_id`: 批次ID (格式: YYYYMMDD_HHMMSS),用于区分不同时间点的数据
- `stat_date`: 统计日期
- `campaign_id/name`: 广告系列标识
- `optimizer`: 投手名称
- `drama_id/name`: 剧集标识
- `channel`: 渠道 (google/meta)
- `country`: 国家/地区
- `spend/revenue`: 消耗/收入
- `new_users/new_payers`: 新用户/付费用户

**关键查询逻辑**:
- 日报: 查询 T-1 日最新 batch_id 的数据
- 实时播报: 查询当日最新 batch_id 的数据
- 环比: 比较当前 batch_id 和上一个 batch_id

### 飞书机器人消息格式

`LarkBot` 支持多种消息类型:
- `send_text()` - 纯文本消息
- `send_rich_text()` - 富文本消息 (支持加粗、颜色、@人)
- `send_interactive_card()` - 交互式卡片 (按钮、表格)
- `send_daily_report()` - 日报专用格式
- `send_realtime_report()` - 实时播报专用格式

**@人功能**: 需要在 `OPTIMIZER_USER_MAP` 中配置投手名称到飞书 open_id 的映射

### 爬虫技术栈

**Playwright 自动化**:
- `scraper.py` 和 `dataeye_scraper.py` 使用 Playwright 进行浏览器自动化
- 支持无头模式 (`headless=True`)
- 自动处理登录、等待、截图、调试

**重试机制**:
- `utils/retry.py` 提供统一的重试装饰器
- BigQuery 操作使用 `@retry_with_backoff` 装饰器
- 配置: 最多重试 3 次，指数退避

### AI 分析集成

**Gemini AI** (`lark/gemini_advisor.py`):
- 用于日报和实时播报的策略建议
- 分析数据趋势，生成放量/止损建议
- 需要配置 `GEMINI_API_KEY`

**ChatGPT** (`lark/chatgpt_advisor.py`):
- 用于深度数据洞察和操作建议
- 分析投手表现、剧集效果、市场机会
- 需要配置 `OPENAI_API_KEY`

## 环境变量配置

必需的环境变量 (在 `.env` 文件中配置):

```bash
# GCP 配置
BQ_PROJECT_ID=fleet-blend-469520-n7
BQ_DATASET_ID=quickbi_data

# 飞书配置
LARK_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxx
LARK_SECRET=xxx

# QuickBI 配置
QUICKBI_ACCESS_ID=xxx
QUICKBI_ACCESS_SECRET=xxx
QUICKBI_CUBE_ID=xxx

# XMP 配置
XMP_USERNAME=xxx
XMP_PASSWORD=xxx

# DataEye 配置
DATAEYE_USERNAME=xxx
DATAEYE_PASSWORD=xxx
DATAEYE_VERSION=both  # both/overseas/china

# AI 分析 (可选)
GEMINI_API_KEY=xxx
OPENAI_API_KEY=xxx

# 日报 BI 链接
DAILY_REPORT_BI_LINK=https://bi.aliyun.com/xxx
```

## 关键业务指标

### ROAS 阈值
- **绿色 (S级)**: ROAS ≥ 40%
- **黄色 (效率下滑)**: 30% ≤ ROAS < 40%
- **红色 (需关注)**: ROAS < 30%

### 预警规则
- **止损预警**: Spend > $300 且 ROAS < 30%
- **扩量机会**: Spend > $300 且 ROAS > 50%
- **放量剧目**: Spend > $1000 且 ROAS > 45%
- **机会市场**: 非主投国家, Spend > $100, ROAS > 50%

## 项目文档

详细的架构和数据流文档请参考:
- [docs/DATA_ARCHITECTURE.md](docs/DATA_ARCHITECTURE.md) - 完整的数据架构文档
- [DEPLOY_CLOUD_RUN.md](DEPLOY_CLOUD_RUN.md) - Cloud Run 部署指南
- [lark/lark_bot_setup.md](lark/lark_bot_setup.md) - 飞书机器人配置指南

