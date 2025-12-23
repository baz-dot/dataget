#!/bin/bash
# Quick BI 数据抓取 - Cloud Run Jobs 部署脚本
# 每小时执行一次，获取广告投放数据

set -e

# ============ 配置区域 ============
PROJECT_ID="fleet-blend-469520-n7"
REGION="asia-northeast3"  # 首尔
JOB_NAME="quickbi-data-scraper"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${JOB_NAME}"
SERVICE_ACCOUNT="xmp-data-scraper@${PROJECT_ID}.iam.gserviceaccount.com"

# 环境变量
GCS_BUCKET_NAME="xmp_raw_data_storage"
BQ_PROJECT_ID="${PROJECT_ID}"
QUICKBI_BQ_DATASET_ID="quickbi_data"

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
gcloud builds submit --tag ${IMAGE_NAME} --dockerfile Dockerfile.quickbi .

echo "=== 4. 创建 Cloud Run Job ==="
gcloud run jobs create ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 10m \
    --max-retries 2 \
    --set-env-vars "GCS_BUCKET_NAME=${GCS_BUCKET_NAME}" \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "QUICKBI_BQ_DATASET_ID=${QUICKBI_BQ_DATASET_ID}" \
    --set-env-vars "CLOUD_RUN=true" \
    --quiet || \
gcloud run jobs update ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 10m \
    --max-retries 2 \
    --set-env-vars "GCS_BUCKET_NAME=${GCS_BUCKET_NAME}" \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "QUICKBI_BQ_DATASET_ID=${QUICKBI_BQ_DATASET_ID}" \
    --set-env-vars "CLOUD_RUN=true" \
    --quiet

echo "=== 5. 创建 Cloud Scheduler 定时任务（每小时执行一次）==="
gcloud scheduler jobs create http ${JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 * * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet 2>/dev/null || \
gcloud scheduler jobs update http ${JOB_NAME}-scheduler \
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
echo "BigQuery 数据: https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m4!1m3!3m2!1s${PROJECT_ID}!2sxmp_data"
echo ""
echo "手动执行一次测试："
echo "  gcloud run jobs execute ${JOB_NAME} --region ${REGION}"
