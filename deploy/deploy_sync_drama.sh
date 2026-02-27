#!/bin/bash
# Cloud Run Job 部署脚本 - Drama Mapping 自动同步
# 每天凌晨 2 点执行，自动同步 drama_id → drama_name 映射

set -e

# ============ 配置区域 ============
PROJECT_ID="fleet-blend-469520-n7"
REGION="asia-northeast3"
JOB_NAME="drama-mapping-sync"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${JOB_NAME}"
SERVICE_ACCOUNT="xmp-data-scraper@${PROJECT_ID}.iam.gserviceaccount.com"

# ============ 部署步骤 ============

echo "=== 1. 设置项目 ==="
gcloud config set project ${PROJECT_ID}

echo "=== 2. 构建并推送 Docker 镜像 ==="
cd "$(dirname "$0")/.."
gcloud builds submit --config=deploy/cloudbuild-sync-drama.yaml .

echo "=== 3. 创建/更新 Cloud Run Job ==="
gcloud run jobs create ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 10m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${PROJECT_ID}" \
    --set-env-vars "CLOUD_RUN=true" \
    --set-env-vars "TZ=Asia/Shanghai" \
    --quiet || \
gcloud run jobs update ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 10m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${PROJECT_ID}" \
    --set-env-vars "CLOUD_RUN=true" \
    --set-env-vars "TZ=Asia/Shanghai" \
    --quiet

echo "=== 4. 创建 Cloud Scheduler 定时任务（每天凌晨 2 点）==="
gcloud scheduler jobs create http ${JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 2 * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet 2>/dev/null || \
gcloud scheduler jobs update http ${JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 2 * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet

echo ""
echo "=== 部署完成！ ==="
echo ""
echo "Cloud Run Job: https://console.cloud.google.com/run/jobs/details/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo "Cloud Scheduler: https://console.cloud.google.com/cloudscheduler?project=${PROJECT_ID}"
echo ""
echo "手动执行一次测试："
echo "  gcloud run jobs execute ${JOB_NAME} --region ${REGION}"
