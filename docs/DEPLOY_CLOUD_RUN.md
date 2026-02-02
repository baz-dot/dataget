# Lark 实时播报 Cloud Run 部署指南

## 架构说明

使用 **Cloud Scheduler + Cloud Run** 组合：
- Cloud Scheduler: 每小时 10 分触发
- Cloud Run: 接收触发，执行一次播报

## 部署步骤

### 1. 准备工作

确保已安装：
- Google Cloud SDK
- Docker

登录 Google Cloud：
```bash
gcloud auth login
gcloud config set project fleet-blend-469520-n7
gcloud auth configure-docker
```

### 2. 部署 Cloud Run 服务

```bash
# 给脚本执行权限
chmod +x deploy_scheduler.sh

# 执行部署
./deploy_scheduler.sh
```

部署完成后，记录 Cloud Run 服务的 URL。

### 3. 配置 Cloud Scheduler

编辑 `setup_scheduler.sh`，将 `SERVICE_URL` 改为实际的 Cloud Run URL：
```bash
SERVICE_URL="https://lark-realtime-scheduler-xxxxxxxxxx-an.a.run.app"
```

然后执行：
```bash
chmod +x setup_scheduler.sh
./setup_scheduler.sh
```

### 4. 验证部署

查看 Cloud Scheduler 任务：
```bash
gcloud scheduler jobs list --location=asia-northeast1
```

手动触发测试：
```bash
gcloud scheduler jobs run lark-realtime-9h --location=asia-northeast1
```

### 5. 查看日志

```bash
gcloud run logs read lark-realtime-scheduler --region=asia-northeast1
```

## 环境变量配置

在 Cloud Run 中配置的环境变量：
- `BQ_PROJECT_ID`: BigQuery 项目 ID
- `LARK_WEBHOOK_URL`: 飞书 Webhook URL
- `GEMINI_API_KEY`: Gemini API Key

## 成本估算

- Cloud Run: 按调用次数计费，每小时 1 次 × 24 = 24 次/天
- Cloud Scheduler: 每月前 3 个任务免费
- 预计成本: < $5/月

## 故障排查

### 问题 1: 部署失败
检查 Docker 镜像是否构建成功：
```bash
docker images | grep lark-realtime-scheduler
```

### 问题 2: Scheduler 触发失败
检查服务账号权限：
```bash
gcloud run services add-iam-policy-binding lark-realtime-scheduler \
  --member=serviceAccount:fleet-blend-469520-n7@appspot.gserviceaccount.com \
  --role=roles/run.invoker \
  --region=asia-northeast1
```

### 问题 3: 播报未发送
查看 Cloud Run 日志排查错误。
