#!/bin/bash
# ADX (DataEye) 国内行业版 - Cloud Run Jobs 部署脚本
# 目标 URL: https://adxray-app.dataeye.com/creative/material

set -e

# ============ 配置区域 ============
PROJECT_ID="fleet-blend-469520-n7"
REGION="asia-northeast3"  # 首尔
JOB_NAME="adx-cn-data-scraper"
IMAGE_NAME="gcr.io/${PROJECT_ID}/adx-data-scraper"  # 使用同一个镜像
SERVICE_ACCOUNT="xmp-data-scraper@${PROJECT_ID}.iam.gserviceaccount.com"

# 环境变量（敏感信息）- 使用与海外版相同的账号
DATAEYE_USERNAME="katherine.withyang@gmail.com"
DATAEYE_PASSWORD="3LB2c!C6HZ.9MJn"
GCS_BUCKET_NAME="xmp_raw_data_storage"
BQ_PROJECT_ID="${PROJECT_ID}"
ADX_BQ_DATASET_ID="adx_data"

# 国内版特定配置
DATAEYE_VERSION="china"
DATAEYE_MAX_RECORDS="100"
DATAEYE_DATE_FILTER="today"
DATAEYE_DOWNLOAD_VIDEOS="false"
DATAEYE_UPLOAD_TO_GCS="true"

# ============ 部署步骤 ============

echo "=== 1. 设置项目 ==="
gcloud config set project ${PROJECT_ID}

echo "=== 2. 启用必要的 API ==="
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    cloudscheduler.googleapis.com \
    artifactregistry.googleapis.com

echo "=== 3. 创建/更新 Cloud Run Job（使用已有镜像）==="
gcloud run jobs create ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 4Gi \
    --cpu 2 \
    --task-timeout 60m \
    --max-retries 1 \
    --set-env-vars "DATAEYE_USERNAME=${DATAEYE_USERNAME}" \
    --set-env-vars "DATAEYE_PASSWORD=${DATAEYE_PASSWORD}" \
    --set-env-vars "GCS_BUCKET_NAME=${GCS_BUCKET_NAME}" \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "ADX_BQ_DATASET_ID=${ADX_BQ_DATASET_ID}" \
    --set-env-vars "DATAEYE_VERSION=${DATAEYE_VERSION}" \
    --set-env-vars "DATAEYE_MAX_RECORDS=${DATAEYE_MAX_RECORDS}" \
    --set-env-vars "DATAEYE_DATE_FILTER=${DATAEYE_DATE_FILTER}" \
    --set-env-vars "DATAEYE_DOWNLOAD_VIDEOS=${DATAEYE_DOWNLOAD_VIDEOS}" \
    --set-env-vars "DATAEYE_UPLOAD_TO_GCS=${DATAEYE_UPLOAD_TO_GCS}" \
    --set-env-vars "CLOUD_RUN=true" \
    --quiet || \
gcloud run jobs update ${JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 4Gi \
    --cpu 2 \
    --task-timeout 60m \
    --max-retries 1 \
    --set-env-vars "DATAEYE_USERNAME=${DATAEYE_USERNAME}" \
    --set-env-vars "DATAEYE_PASSWORD=${DATAEYE_PASSWORD}" \
    --set-env-vars "GCS_BUCKET_NAME=${GCS_BUCKET_NAME}" \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "ADX_BQ_DATASET_ID=${ADX_BQ_DATASET_ID}" \
    --set-env-vars "DATAEYE_VERSION=${DATAEYE_VERSION}" \
    --set-env-vars "DATAEYE_MAX_RECORDS=${DATAEYE_MAX_RECORDS}" \
    --set-env-vars "DATAEYE_DATE_FILTER=${DATAEYE_DATE_FILTER}" \
    --set-env-vars "DATAEYE_DOWNLOAD_VIDEOS=${DATAEYE_DOWNLOAD_VIDEOS}" \
    --set-env-vars "DATAEYE_UPLOAD_TO_GCS=${DATAEYE_UPLOAD_TO_GCS}" \
    --set-env-vars "CLOUD_RUN=true" \
    --quiet

echo "=== 5. 创建 Cloud Scheduler 定时任务（每天执行一次）==="
gcloud scheduler jobs create http ${JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 9 * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet 2>/dev/null || \
gcloud scheduler jobs update http ${JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 9 * * *" \
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
echo "BigQuery 数据: https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m4!1m3!3m2!1s${PROJECT_ID}!2sadx_data"
echo "GCS 数据: https://console.cloud.google.com/storage/browser/${GCS_BUCKET_NAME}/adx_cn?project=${PROJECT_ID}"
echo ""
echo "手动执行一次测试："
echo "  gcloud run jobs execute ${JOB_NAME} --region ${REGION}"
