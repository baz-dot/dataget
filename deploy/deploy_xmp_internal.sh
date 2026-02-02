#!/bin/bash
# XMP 内部 API 数据采集 - Cloud Run Jobs 部署脚本
# 每半小时执行，获取实时数据

set -e

# ============ 配置区域 ============
PROJECT_ID="fleet-blend-469520-n7"
REGION="asia-northeast3"
JOB_NAME="xmp-internal-scraper"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${JOB_NAME}"
SERVICE_ACCOUNT="xmp-data-scraper@${PROJECT_ID}.iam.gserviceaccount.com"

# 环境变量
BQ_PROJECT_ID="${PROJECT_ID}"
BQ_DATASET_ID="xmp_data"

# XMP 登录凭据
XMP_USERNAME="${XMP_USERNAME:-violet@spoonlabs-partners.com}"
XMP_PASSWORD="${XMP_PASSWORD:-M8NkLc8dyr}"

# XMP Open API 凭据 (素材报表)
XMP_CLIENT_ID="3d546dedfe2f3613af0da8202419b937"
XMP_CLIENT_SECRET="50172bbe0c83836153bc13c37dbe3830"

# ============ 部署步骤 ============

echo "=== 1. 设置项目 ==="
gcloud config set project ${PROJECT_ID}

echo "=== 2. 启用必要的 API ==="
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    cloudscheduler.googleapis.com

echo "=== 3. 构建并推送 Docker 镜像 ==="
gcloud builds submit --config=deploy/cloudbuild-xmp-internal.yaml .

echo "=== 4. 创建/更新 Cloud Run Job ==="
gcloud run jobs create ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 1Gi \
    --cpu 1 \
    --task-timeout 20m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "XMP_USERNAME=${XMP_USERNAME}" \
    --set-env-vars "XMP_PASSWORD=${XMP_PASSWORD}" \
    --set-env-vars "XMP_CLIENT_ID=${XMP_CLIENT_ID}" \
    --set-env-vars "XMP_CLIENT_SECRET=${XMP_CLIENT_SECRET}" \
    --set-env-vars "CLOUD_RUN=true" \
    --quiet 2>/dev/null || \
gcloud run jobs update ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --memory 1Gi \
    --cpu 1 \
    --task-timeout 20m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "XMP_USERNAME=${XMP_USERNAME}" \
    --set-env-vars "XMP_PASSWORD=${XMP_PASSWORD}" \
    --set-env-vars "XMP_CLIENT_ID=${XMP_CLIENT_ID}" \
    --set-env-vars "XMP_CLIENT_SECRET=${XMP_CLIENT_SECRET}" \
    --set-env-vars "CLOUD_RUN=true" \
    --quiet

echo "=== 5. 创建 Cloud Scheduler（每半小时执行）==="
gcloud scheduler jobs create http ${JOB_NAME}-30min \
    --location ${REGION} \
    --schedule "*/30 * * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet 2>/dev/null || \
gcloud scheduler jobs update http ${JOB_NAME}-30min \
    --location ${REGION} \
    --schedule "*/30 * * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet

echo "=== 6. 创建 T-1 日数据采集任务（凌晨 2:00）==="
# 这个任务在凌晨 2:00 触发，采集昨天的完整数据，确保日报数据准确
DAILY_JOB_NAME="${JOB_NAME}-daily"

# 创建专门用于 T-1 采集的 Job
gcloud run jobs create ${DAILY_JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 1Gi \
    --cpu 1 \
    --task-timeout 20m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "XMP_USERNAME=${XMP_USERNAME}" \
    --set-env-vars "XMP_PASSWORD=${XMP_PASSWORD}" \
    --set-env-vars "XMP_CLIENT_ID=${XMP_CLIENT_ID}" \
    --set-env-vars "XMP_CLIENT_SECRET=${XMP_CLIENT_SECRET}" \
    --set-env-vars "CLOUD_RUN=true" \
    --set-env-vars "FETCH_YESTERDAY=true" \
    --quiet 2>/dev/null || \
gcloud run jobs update ${DAILY_JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --memory 1Gi \
    --cpu 1 \
    --task-timeout 20m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "XMP_USERNAME=${XMP_USERNAME}" \
    --set-env-vars "XMP_PASSWORD=${XMP_PASSWORD}" \
    --set-env-vars "XMP_CLIENT_ID=${XMP_CLIENT_ID}" \
    --set-env-vars "XMP_CLIENT_SECRET=${XMP_CLIENT_SECRET}" \
    --set-env-vars "CLOUD_RUN=true" \
    --set-env-vars "FETCH_YESTERDAY=true" \
    --quiet

# 创建凌晨 1:00 的定时任务
gcloud scheduler jobs create http ${DAILY_JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 2 * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${DAILY_JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet 2>/dev/null || \
gcloud scheduler jobs update http ${DAILY_JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 2 * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${DAILY_JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet

echo ""
echo "=== 部署完成！ ==="
echo ""
echo "Cloud Run Job: https://console.cloud.google.com/run/jobs/details/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo "Cloud Scheduler: https://console.cloud.google.com/cloudscheduler?project=${PROJECT_ID}"
echo ""
echo "手动执行测试："
echo "  gcloud run jobs execute ${JOB_NAME} --region ${REGION}"
