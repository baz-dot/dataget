# 使用 Playwright 官方镜像（包含浏览器）
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 安装 Google Cloud 依赖
RUN pip install --no-cache-dir google-cloud-storage google-cloud-bigquery

# 复制代码文件
COPY scraper.py .
COPY gcs_storage.py .
COPY bigquery_storage.py .

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV CLOUD_RUN=true

# 运行爬虫（headless 模式）
CMD ["python", "scraper.py"]
