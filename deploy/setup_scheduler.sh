#!/bin/bash
# Cloud Scheduler 配置脚本 - 每小时触发实时播报

PROJECT_ID="fleet-blend-469520-n7"
SERVICE_NAME="lark-realtime-scheduler"
REGION="asia-northeast1"
SERVICE_URL="https://${SERVICE_NAME}-xxxxxxxxxx-an.a.run.app"

echo "配置 Cloud Scheduler 定时任务..."

# 创建每小时触发的 Cloud Scheduler 任务
for hour in {0..23}; do
  JOB_NAME="lark-realtime-${hour}h"
  SCHEDULE="${hour} * * * *"  # 每天该小时的 10 分执行

  echo "创建任务: ${JOB_NAME} (每天 ${hour}:10)"

  gcloud scheduler jobs create http ${JOB_NAME} \
    --location=${REGION} \
    --schedule="10 ${hour} * * *" \
    --uri="${SERVICE_URL}" \
    --http-method=POST \
    --oidc-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com" \
    --project=${PROJECT_ID} \
    --time-zone="Asia/Hong_Kong" \
    --attempt-deadline=300s \
    || echo "任务 ${JOB_NAME} 已存在，跳过"
done

echo "Cloud Scheduler 配置完成！"
echo "所有任务将在每小时 10 分自动触发"
