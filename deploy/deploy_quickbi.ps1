# Quick BI 数据抓取 - Cloud Run Jobs 部署脚本 (PowerShell 版本)

$ErrorActionPreference = "Stop"

# ============ 配置区域 ============
$PROJECT_ID = "fleet-blend-469520-n7"
$REGION = "asia-northeast3"
$JOB_NAME = "quickbi-data-scraper"
$IMAGE_NAME = "gcr.io/$PROJECT_ID/$JOB_NAME"
$SERVICE_ACCOUNT = "xmp-data-scraper@$PROJECT_ID.iam.gserviceaccount.com"

# 环境变量
$GCS_BUCKET_NAME = "xmp_raw_data_storage"
$BQ_PROJECT_ID = $PROJECT_ID
$QUICKBI_BQ_DATASET_ID = "quickbi_data"

# ============ 部署步骤 ============

Write-Host "=== 1. 设置项目 ===" -ForegroundColor Green
gcloud config set project $PROJECT_ID

Write-Host "=== 2. 启用必要的 API ===" -ForegroundColor Green
gcloud services enable cloudbuild.googleapis.com run.googleapis.com cloudscheduler.googleapis.com artifactregistry.googleapis.com

Write-Host "=== 3. 构建并推送 Docker 镜像 ===" -ForegroundColor Green
Set-Location (Split-Path $PSScriptRoot -Parent)
gcloud builds submit --config=deploy/cloudbuild-quickbi.yaml . --timeout=600s

Write-Host "=== 4. 创建/更新 Cloud Run Job ===" -ForegroundColor Green
$jobExists = gcloud run jobs list --region=$REGION --format="value(name)" 2>$null | Select-String $JOB_NAME

if ($jobExists) {
    gcloud run jobs update $JOB_NAME `
        --image $IMAGE_NAME `
        --region $REGION `
        --service-account $SERVICE_ACCOUNT `
        --memory 1Gi `
        --cpu 1 `
        --task-timeout 10m `
        --max-retries 2 `
        --set-env-vars "GCS_BUCKET_NAME=$GCS_BUCKET_NAME" `
        --set-env-vars "BQ_PROJECT_ID=$BQ_PROJECT_ID" `
        --set-env-vars "QUICKBI_BQ_DATASET_ID=$QUICKBI_BQ_DATASET_ID" `
        --set-env-vars "CLOUD_RUN=true" `
        --set-env-vars "TZ=Asia/Shanghai" `
        --quiet
} else {
    gcloud run jobs create $JOB_NAME `
        --image $IMAGE_NAME `
        --region $REGION `
        --service-account $SERVICE_ACCOUNT `
        --memory 1Gi `
        --cpu 1 `
        --task-timeout 10m `
        --max-retries 2 `
        --set-env-vars "GCS_BUCKET_NAME=$GCS_BUCKET_NAME" `
        --set-env-vars "BQ_PROJECT_ID=$BQ_PROJECT_ID" `
        --set-env-vars "QUICKBI_BQ_DATASET_ID=$QUICKBI_BQ_DATASET_ID" `
        --set-env-vars "CLOUD_RUN=true" `
        --set-env-vars "TZ=Asia/Shanghai" `
        --quiet
}

Write-Host "=== 5. 创建/更新每小时调度任务 ===" -ForegroundColor Green
$schedulerExists = gcloud scheduler jobs list --location=$REGION --format="value(name)" 2>$null | Select-String "$JOB_NAME-scheduler"

$URI = "https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/${JOB_NAME}:run"

if ($schedulerExists) {
    gcloud scheduler jobs update http "$JOB_NAME-scheduler" `
        --location $REGION `
        --schedule "0,3 * * * *" `
        --time-zone "Asia/Shanghai" `
        --uri $URI `
        --http-method POST `
        --oauth-service-account-email $SERVICE_ACCOUNT `
        --quiet
} else {
    gcloud scheduler jobs create http "$JOB_NAME-scheduler" `
        --location $REGION `
        --schedule "0,3 * * * *" `
        --time-zone "Asia/Shanghai" `
        --uri $URI `
        --http-method POST `
        --oauth-service-account-email $SERVICE_ACCOUNT `
        --quiet
}

Write-Host "=== 6. 创建凌晨 1 点 T-1 日数据采集任务（用于日报）===" -ForegroundColor Green
$dailyExists = gcloud scheduler jobs list --location=$REGION --format="value(name)" 2>$null | Select-String "$JOB_NAME-daily"

if ($dailyExists) {
    gcloud scheduler jobs update http "$JOB_NAME-daily" `
        --location $REGION `
        --schedule "0 1 * * *" `
        --time-zone "Asia/Shanghai" `
        --uri $URI `
        --http-method POST `
        --oauth-service-account-email $SERVICE_ACCOUNT `
        --quiet
} else {
    gcloud scheduler jobs create http "$JOB_NAME-daily" `
        --location $REGION `
        --schedule "0 1 * * *" `
        --time-zone "Asia/Shanghai" `
        --uri $URI `
        --http-method POST `
        --oauth-service-account-email $SERVICE_ACCOUNT `
        --quiet
}

Write-Host ""
Write-Host "=== Deploy Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Cloud Run Job: https://console.cloud.google.com/run/jobs/details/$REGION/$JOB_NAME"
Write-Host "Cloud Scheduler: https://console.cloud.google.com/cloudscheduler"
Write-Host ""
Write-Host "Manual test:"
Write-Host "  gcloud run jobs execute $JOB_NAME --region $REGION"
