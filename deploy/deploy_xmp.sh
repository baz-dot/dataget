#!/bin/bash
# XMP API 数据采集 - Cloud Run Jobs 部署脚本
# 每小时整点执行，获取 campaign 级别数据

set -e

# ============ 配置区域 ============
PROJECT_ID="fleet-blend-469520-n7"
REGION="asia-northeast3"  # 首尔
JOB_NAME="xmp-campaign-scraper"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${JOB_NAME}"
SERVICE_ACCOUNT="xmp-data-scraper@${PROJECT_ID}.iam.gserviceaccount.com"

# 环境变量
BQ_PROJECT_ID="${PROJECT_ID}"
BQ_DATASET_ID="xmp_data"
XMP_CLIENT_ID="3d546dedfe2f3613af0da8202419b937"
XMP_CLIENT_SECRET="50172bbe0c83836153bc13c37dbe3830"

# ============ 部署步骤 ============

echo "=== 1. 设置项目 ==="
gcloud config set project ${PROJECT_ID}

echo "=== 2. 启用必要的 API ==="
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    cloudscheduler.googleapis.com \
    artifactregistry.googleapis.com

echo "=== 3. 构建并推送 Docker 镜像 ==="
# 从项目根目录执行构建
cd "$(dirname "$0")/.."
gcloud builds submit --config=deploy/cloudbuild-xmp.yaml .

echo "=== 4. 创建/更新 Cloud Run Job ==="
gcloud run jobs create ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 15m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "XMP_CLIENT_ID=${XMP_CLIENT_ID}" \
    --set-env-vars "XMP_CLIENT_SECRET=${XMP_CLIENT_SECRET}" \
    --set-env-vars "CLOUD_RUN=true" \
    --quiet 2>/dev/null || \
gcloud run jobs update ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 15m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "XMP_CLIENT_ID=${XMP_CLIENT_ID}" \
    --set-env-vars "XMP_CLIENT_SECRET=${XMP_CLIENT_SECRET}" \
    --set-env-vars "CLOUD_RUN=true" \
    --quiet

echo "=== 5. 创建 Cloud Scheduler 定时任务（每小时整点执行）==="
gcloud scheduler jobs create http ${JOB_NAME}-hourly \
    --location ${REGION} \
    --schedule "0 * * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet 2>/dev/null || \
gcloud scheduler jobs update http ${JOB_NAME}-hourly \
    --location ${REGION} \
    --schedule "0 * * * *" \
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
echo "BigQuery 数据: https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m4!1m3!3m2!1s${PROJECT_ID}!2s${BQ_DATASET_ID}"
echo ""
echo "手动执行一次测试："
echo "  gcloud run jobs execute ${JOB_NAME} --region ${REGION}"