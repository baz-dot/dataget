#!/bin/bash
# XMP 日报/周报生成 - Cloud Run Jobs 部署脚本
# 日报: 每天凌晨 3:00 执行，生成 T-1 日报
# 周报: 每周一凌晨 4:00 执行，生成上周周报

set -e

# ============ 配置区域 ============
PROJECT_ID="fleet-blend-469520-n7"
REGION="asia-northeast3"
IMAGE_NAME="gcr.io/${PROJECT_ID}/xmp-report"
SERVICE_ACCOUNT="xmp-data-scraper@${PROJECT_ID}.iam.gserviceaccount.com"

# BigQuery 配置
BQ_PROJECT_ID="${PROJECT_ID}"
BQ_DATASET_ID="xmp_data"

# 飞书 API 配置 (从环境变量或 Secret Manager 获取)
LARK_APP_ID="${LARK_APP_ID:-cli_a9ef435701b85ed0}"
LARK_APP_SECRET="${LARK_APP_SECRET:-YPEmLhKg1hSLarpVN7OVWge8lv2jm8na}"
XMP_DOC_TOKEN="${XMP_DOC_TOKEN:-XMYlw4GkDicYhwk3VWglkF0DgZb}"

# ============ 部署步骤 ============

echo "=== 1. 设置项目 ==="
gcloud config set project ${PROJECT_ID}

echo "=== 2. 启用必要的 API ==="
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    cloudscheduler.googleapis.com

echo "=== 3. 构建并推送 Docker 镜像 ==="
cd "$(dirname "$0")/.."
gcloud builds submit --config=deploy/cloudbuild-xmp-report.yaml .

echo "=== 4. 创建日报 Cloud Run Job ==="
DAILY_JOB_NAME="xmp-daily-report"

gcloud run jobs create ${DAILY_JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 10m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "LARK_APP_ID=${LARK_APP_ID}" \
    --set-env-vars "LARK_APP_SECRET=${LARK_APP_SECRET}" \
    --set-env-vars "XMP_DOC_TOKEN=${XMP_DOC_TOKEN}" \
    --set-env-vars "CLOUD_RUN=true" \
    --set-env-vars "REPORT_MODE=daily" \
    --quiet 2>/dev/null || \
gcloud run jobs update ${DAILY_JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 10m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "LARK_APP_ID=${LARK_APP_ID}" \
    --set-env-vars "LARK_APP_SECRET=${LARK_APP_SECRET}" \
    --set-env-vars "XMP_DOC_TOKEN=${XMP_DOC_TOKEN}" \
    --set-env-vars "CLOUD_RUN=true" \
    --set-env-vars "REPORT_MODE=daily" \
    --quiet

echo "=== 5. 创建日报 Cloud Scheduler（每天凌晨 3:00）==="
gcloud scheduler jobs create http ${DAILY_JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 3 * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${DAILY_JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet 2>/dev/null || \
gcloud scheduler jobs update http ${DAILY_JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 3 * * *" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${DAILY_JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet

echo "=== 6. 创建周报 Cloud Run Job ==="
WEEKLY_JOB_NAME="xmp-weekly-report"

gcloud run jobs create ${WEEKLY_JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 15m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "LARK_APP_ID=${LARK_APP_ID}" \
    --set-env-vars "LARK_APP_SECRET=${LARK_APP_SECRET}" \
    --set-env-vars "XMP_DOC_TOKEN=${XMP_DOC_TOKEN}" \
    --set-env-vars "CLOUD_RUN=true" \
    --set-env-vars "REPORT_MODE=weekly" \
    --quiet 2>/dev/null || \
gcloud run jobs update ${WEEKLY_JOB_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --memory 512Mi \
    --cpu 1 \
    --task-timeout 15m \
    --max-retries 2 \
    --set-env-vars "BQ_PROJECT_ID=${BQ_PROJECT_ID}" \
    --set-env-vars "BQ_DATASET_ID=${BQ_DATASET_ID}" \
    --set-env-vars "LARK_APP_ID=${LARK_APP_ID}" \
    --set-env-vars "LARK_APP_SECRET=${LARK_APP_SECRET}" \
    --set-env-vars "XMP_DOC_TOKEN=${XMP_DOC_TOKEN}" \
    --set-env-vars "CLOUD_RUN=true" \
    --set-env-vars "REPORT_MODE=weekly" \
    --quiet

echo "=== 7. 创建周报 Cloud Scheduler（每周一凌晨 4:00）==="
gcloud scheduler jobs create http ${WEEKLY_JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 4 * * 1" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${WEEKLY_JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet 2>/dev/null || \
gcloud scheduler jobs update http ${WEEKLY_JOB_NAME}-scheduler \
    --location ${REGION} \
    --schedule "0 4 * * 1" \
    --time-zone "Asia/Shanghai" \
    --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${WEEKLY_JOB_NAME}:run" \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --quiet

echo ""
echo "=== 部署完成！ ==="
echo ""
echo "Cloud Run Jobs:"
echo "  日报: https://console.cloud.google.com/run/jobs/details/${REGION}/${DAILY_JOB_NAME}?project=${PROJECT_ID}"
echo "  周报: https://console.cloud.google.com/run/jobs/details/${REGION}/${WEEKLY_JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "Cloud Scheduler: https://console.cloud.google.com/cloudscheduler?project=${PROJECT_ID}"
echo ""
echo "手动执行测试："
echo "  日报: gcloud run jobs execute ${DAILY_JOB_NAME} --region ${REGION}"
echo "  周报: gcloud run jobs execute ${WEEKLY_JOB_NAME} --region ${REGION}"
