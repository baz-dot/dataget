#!/bin/bash
# Cloud Scheduler 配置脚本 - Drama Mapping 自动同步
# 每天凌晨 2 点运行，自动同步新的 drama 映射

# 配置变量
PROJECT_ID="fleet-blend-469520-n7"
SERVICE_NAME="drama-mapping-sync"
REGION="asia-northeast1"
JOB_NAME="sync-drama-mapping-daily"
SCHEDULE="0 2 * * *"  # 每天凌晨 2 点（北京时间）
TIMEZONE="Asia/Shanghai"

echo "配置 Cloud Scheduler 定时任务..."

# 获取 Cloud Run 服务 URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} \
  --platform managed \
  --region ${REGION} \
  --project ${PROJECT_ID} \
  --format 'value(status.url)')

if [ -z "$SERVICE_URL" ]; then
  echo "错误: 无法获取 Cloud Run 服务 URL"
  echo "请先运行 deploy_sync_drama.sh 部署服务"
  exit 1
fi

echo "服务 URL: ${SERVICE_URL}"

# 创建或更新 Cloud Scheduler 任务
echo "创建定时任务: ${JOB_NAME}"
gcloud scheduler jobs create http ${JOB_NAME} \
  --location ${REGION} \
  --schedule "${SCHEDULE}" \
  --time-zone ${TIMEZONE} \
  --uri "${SERVICE_URL}" \
  --http-method POST \
  --oidc-service-account-email "${PROJECT_ID}@appspot.gserviceaccount.com" \
  --project ${PROJECT_ID} \
  --description "每天自动同步 drama_mapping 表" \
  || \
gcloud scheduler jobs update http ${JOB_NAME} \
  --location ${REGION} \
  --schedule "${SCHEDULE}" \
  --time-zone ${TIMEZONE} \
  --uri "${SERVICE_URL}" \
  --http-method POST \
  --project ${PROJECT_ID}

echo "定时任务配置完成！"
echo "任务名称: ${JOB_NAME}"
echo "执行时间: 每天凌晨 2 点（北京时间）"
echo "下次执行: $(gcloud scheduler jobs describe ${JOB_NAME} --location ${REGION} --project ${PROJECT_ID} --format 'value(scheduleTime)')"
