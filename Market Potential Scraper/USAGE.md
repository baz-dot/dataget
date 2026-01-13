# 市场潜力分析工具 - 使用说明

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env` 并填写 DataEye 账号信息:

```bash
DATAEYE_USERNAME=your_username
DATAEYE_PASSWORD=your_password
```

### 3. 运行分析

```bash
# 使用默认配置文件 (dramas.json)
python market_potential_scraper.py

# 指定输出文件名
python market_potential_scraper.py --output 我的报告.xlsx

# 使用无头模式 (后台运行)
python market_potential_scraper.py --headless
```

## 命令行参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `--input, -i` | 输入文件路径 (.json 或 .txt) | `--input dramas.json` |
| `--dramas, -d` | 逗号分隔的剧名列表 | `--dramas "剧1,剧2,剧3"` |
| `--output, -o` | 输出 Excel 文件路径 | `--output report.xlsx` |
| `--recent-days` | 近期数据时间窗口 (天数) | `--recent-days 30` |
| `--lifecycle-days` | 生命周期时间窗口 (天数) | `--lifecycle-days 730` |
| `--headless` | 使用无头模式运行浏览器 | `--headless` |
| `--debug` | 启用调试模式 | `--debug` |

## 输出说明

程序会生成以下文件:

1. **Excel 报告** (`9部剧市场潜力分析报告_时间戳.xlsx`)
   - Sheet 1: 近30天日数据
   - Sheet 2: 生命周期汇总 (近2年)
   - Sheet 3: 聚合分析与排名

2. **原始数据** (`raw_data_时间戳.json`)
   - 从 DataEye 采集的原始 JSON 数据

3. **日志文件** (`market_potential_scraper.log`)
   - 详细的运行日志

## 优先级说明

| 优先级 | 含义 | 特征 |
|--------|------|------|
| **S** | 蓝海爆款 | 无或极少 ADX 记录,未被竞品发现 |
| **A** | 验证过的好剧 | 高2年曝光 + 长生命周期 + 低近期活跃 |
| **B** | 观望剧 | 中等数据,需要进一步观察 |
| **C** | 红海/烂剧 | 超高近期曝光 或 超短生命周期 |

## 常见问题

### Q: 登录失败怎么办?
A: 如果自动登录失败,程序会提示手动登录。在浏览器中完成登录后按回车继续。

### Q: 如何添加更多剧目?
A: 编辑 `dramas.json` 文件,按照现有格式添加新剧目。

### Q: 数据采集需要多长时间?
A: 每部剧约需 5-10 秒,9 部剧大约需要 1-2 分钟。

### Q: 可以批量分析更多剧目吗?
A: 可以,只需在 `dramas.json` 中添加更多剧目即可。

## 测试功能

运行测试脚本验证功能:

```bash
# 测试 Excel 生成器
python test_excel_generator.py

# 测试爬虫 (需要登录)
python test_scraper.py
```

## 技术支持

如有问题,请查看:
- 日志文件: `market_potential_scraper.log`
- 项目文档: `CLAUDE.md`
- README: `README.md`
