# 交互式搜索工具使用说明

## 功能介绍

这是一个**交互式搜索工具**，让你可以：
1. 在浏览器中手动输入剧名并搜索
2. 程序自动监听 API 请求，抓取返回的 JSON 数据
3. 自动保存原始数据到文件
4. 一键分析所有数据并生成 Excel 报告

## 快速开始

### 1. 安装依赖

```bash
pip install playwright pandas openpyxl
playwright install chromium
```

### 2. 运行程序

```bash
python interactive_search.py
```

### 3. 使用流程

程序启动后会：
1. 打开浏览器并访问 DataEye 平台
2. 自动加载已保存的 Cookie（如果有）
3. 显示操作提示

**你需要做的：**
- 在浏览器中手动输入剧名并点击搜索
- 程序会自动捕获 API 返回的数据
- 每次搜索后，数据会自动保存到 `captured_data/` 目录

## 可用命令

在命令行中输入以下命令：

| 命令 | 功能 |
|------|------|
| `status` | 查看已捕获的数据列表 |
| `analyze` | 分析所有数据并生成 Excel 报告 |
| `save` | 保存当前登录状态（Cookie） |
| `quit` | 退出程序 |

## 工作流程示例

```
1. 运行程序
   $ python interactive_search.py

2. 浏览器自动打开 DataEye 平台

3. 在浏览器中搜索第一个剧名
   → 程序自动捕获数据
   → 显示: ✓ 捕获到搜索结果: 回家的诱惑

4. 继续搜索其他剧名
   → 每次搜索都会自动捕获

5. 查看已捕获的数据
   输入命令: status
   → 显示所有已捕获的剧名列表

6. 生成分析报告
   输入命令: analyze
   → 自动分析所有数据
   → 生成 Excel 报告: 市场潜力分析报告_20231225_143022.xlsx

7. 退出程序
   输入命令: quit
```

## 数据存储

### 自动保存的文件

1. **原始 JSON 数据**
   - 位置: `captured_data/`
   - 格式: `{剧名}_{时间戳}.json`
   - 示例: `回家的诱惑_20231225_143022.json`

2. **Cookie 文件**
   - 位置: `dataeye_cookies.json`
   - 用途: 保存登录状态，下次运行时自动登录

3. **Excel 报告**
   - 位置: 当前目录
   - 格式: `市场潜力分析报告_{时间戳}.xlsx`
   - 包含: 日数据、生命周期数据、优先级排名

## 注意事项

### 首次使用

如果是第一次使用，需要手动登录：
1. 程序会提示 "未找到 Cookie 文件"
2. 在浏览器中手动登录 DataEye 平台
3. 登录成功后，输入命令 `save` 保存 Cookie
4. 下次运行时会自动登录

### 数据捕获

- 程序会监听所有包含 `search` 或 `searchCreative` 的 API 请求
- 每次搜索可能触发多个 API 请求（分页加载）
- 所有响应都会被捕获并保存

### 搜索关键词识别

程序会尝试从以下位置提取搜索关键词：
1. POST 请求的 body 中的 `keyword` 字段
2. URL 参数中的 `q` 或 `keyword` 参数
3. 如果无法提取，使用时间戳作为标识

## 高级功能

### 自定义搜索页面

如果需要访问其他页面，可以修改代码：

```python
tool.open_search_page("https://your-custom-url.com")
```

### 调试模式

如果需要查看详细的调试信息，修改日志级别：

```python
logging.basicConfig(level=logging.DEBUG)
```

## 故障排除

### 问题：浏览器无法打开

**解决方案：**
```bash
playwright install chromium
```

### 问题：无法捕获数据

**可能原因：**
1. API URL 已变化
2. 网络请求被拦截

**解决方案：**
- 检查浏览器开发者工具中的 Network 面板
- 查看实际的 API 请求 URL
- 修改代码中的 URL 匹配规则

### 问题：Cookie 失效

**解决方案：**
1. 删除 `dataeye_cookies.json`
2. 重新运行程序
3. 手动登录
4. 输入 `save` 命令保存新的 Cookie

## 与其他工具的对比

| 工具 | 优点 | 缺点 |
|------|------|------|
| `interactive_search.py` | 手动搜索，灵活控制 | 需要手动操作 |
| `dataeye_scraper.py` | 全自动，批量处理 | 需要配置剧名列表 |
| `manual_data_processor.py` | 离线处理 | 需要手动复制 JSON |

## 技术细节

### API 拦截机制

使用 Playwright 的 `route` 功能拦截网络请求：

```python
self.page.route("**/api/**", handle_route)
self.page.route("**/*search*", handle_route)
```

### 数据处理流程

1. 拦截 API 响应
2. 提取搜索关键词
3. 保存原始 JSON
4. 存储到内存（用于后续分析）
5. 显示实时统计信息

### Excel 报告生成

使用 `ExcelGenerator` 类生成包含以下内容的报告：
- Sheet 1: 近30天日数据
- Sheet 2: 2年生命周期数据
- Sheet 3: 优先级排名和热度分析

## 更新日志

### v1.0.0 (2023-12-25)
- 初始版本
- 支持交互式搜索
- 自动 API 拦截和数据捕获
- 集成数据分析和报告生成
