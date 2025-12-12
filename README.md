# XMP 数据抓取工具

从 XMP 平台 (https://xmp.mobvista.com) 自动抓取账户数据的 Python 工具。

## 功能特点

- 自动登录 XMP 平台
- 抓取账户汇总数据
- 支持多种数据导出格式（CSV、JSON、Excel）
- 可配置无头模式运行
- 错误处理和调试支持

## 环境要求

- Python 3.8+
- Windows/Linux/macOS

## 安装步骤

### 1. 克隆或下载项目

```bash
cd d:\workspace\dataget
```

### 2. 创建虚拟环境（推荐）

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/macOS
source venv/bin/activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 安装 Playwright 浏览器

```bash
playwright install chromium
```

### 5. 配置登录信息

复制 `.env.example` 为 `.env` 并填写你的账号信息：

```bash
copy .env.example .env  # Windows
# 或
cp .env.example .env    # Linux/macOS
```

编辑 `.env` 文件：

```
XMP_USERNAME=你的用户名
XMP_PASSWORD=你的密码
```

## 使用方法

### 基本使用

```bash
python scraper.py
```

### 自定义使用

在你的 Python 代码中：

```python
from scraper import XMPScraper

# 创建抓取器
scraper = XMPScraper(
    username="your_username",
    password="your_password",
    headless=False  # True=后台运行，False=显示浏览器
)

# 运行抓取
scraper.run(save_format='csv')  # 可选: 'csv', 'json', 'excel'
```

## 输出文件

数据会保存为带时间戳的文件：

- CSV 格式: `xmp_data_20231211_153045.csv`
- JSON 格式: `xmp_data_20231211_153045.json`
- Excel 格式: `xmp_data_20231211_153045.xlsx`

## 调试

如果遇到问题，脚本会自动生成调试文件：

- `login_error.png` - 登录失败时的截图
- `page_screenshot.png` - 数据页面截图
- `page_content.html` - 页面 HTML 内容

## 常见问题

### 1. 登录失败

- 检查 `.env` 文件中的用户名和密码是否正确
- 查看 `login_error.png` 截图了解页面状态
- 尝试设置 `headless=False` 观察登录过程

### 2. 无法提取数据

- 检查 `page_screenshot.png` 和 `page_content.html`
- 页面结构可能已更改，需要调整选择器
- 确保登录后正确跳转到数据页面

### 3. 浏览器启动失败

```bash
# 重新安装 Playwright 浏览器
playwright install chromium --force
```

## 项目结构

```
dataget/
├── scraper.py          # 主程序
├── requirements.txt    # Python 依赖
├── .env.example        # 配置文件模板
├── .env               # 实际配置（不提交到 git）
├── .gitignore         # Git 忽略文件
└── README.md          # 说明文档
```

## 高级功能

### 定时任务

使用 `schedule` 库实现定时抓取：

```python
import schedule
import time
from scraper import XMPScraper

def job():
    scraper = XMPScraper(username="xxx", password="xxx", headless=True)
    scraper.run()

# 每天早上 9 点执行
schedule.every().day.at("09:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### 自定义数据提取

如果默认提取逻辑不适用，可以修改 `scraper.py` 中的 `extract_data()` 方法：

```python
def extract_data(self):
    # 根据实际页面结构自定义提取逻辑
    data = self.page.query_selector('.your-selector')
    return data
```

## 注意事项

- 请遵守网站的使用条款和 robots.txt
- 不要过于频繁地请求，避免给服务器造成压力
- 妥善保管 `.env` 文件，不要泄露账号信息
- 定期检查页面结构变化，及时更新选择器

## 许可证

MIT License

## 支持

如有问题，请检查调试文件或联系开发者。
