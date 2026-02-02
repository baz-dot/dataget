#!/bin/bash
# Cloud Run 部署脚本 - Lark 播报调度器

# 配置变量
PROJECT_ID="fleet-blend-469520-n7"
SERVICE_NAME="lark-realtime-scheduler"
REGION="asia-northeast1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "开始部署 Lark 播报调度器到 Cloud Run..."

# 切换到项目根目录
cd "$(dirname "$0")/.."

# 1. 构建 Docker 镜像
echo "步骤 1: 构建 Docker 镜像..."
docker build -f deploy/Dockerfile.scheduler -t ${IMAGE_NAME} .

# 2. 推送镜像到 Google Container Registry
echo "步骤 2: 推送镜像到 GCR..."
docker push ${IMAGE_NAME}

# 3. 部署到 Cloud Run
echo "步骤 3: 部署到 Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${IMAGE_NAME} \
  --platform managed \
  --region ${REGION} \
  --project ${PROJECT_ID} \
  --no-allow-unauthenticated \
  --timeout 3600 \
  --memory 1Gi \
  --min-instances 1 \
  --max-instances 1 \
  --set-env-vars "TZ=Asia/Shanghai" \
  --set-env-vars "BQ_PROJECT_ID=${PROJECT_ID}" \
  --set-env-vars "BQ_DATASET_ID=quickbi_data" \
  --set-env-vars "LARK_WEBHOOK_URL=https://open.larksuite.com/open-apis/bot/v2/hook/e1ae0a7e-0958-4425-aeaf-0bed11160b8a" \
  --set-env-vars "LARK_APP_ID=cli_a9ef435701b85ed0" \
  --set-env-vars "LARK_APP_SECRET=YPEmLhKg1hSLarpVN7OVWge8lv2jm8na" \
  --set-env-vars "WEEKLY_REPORT_DOC_TOKEN=J04hwAsG6iTSjskVZeklMmArg1f" \
  --set-env-vars "XMP_CLIENT_ID=3d546dedfe2f3613af0da8202419b937" \
  --set-env-vars "XMP_CLIENT_SECRET=50172bbe0c83836153bc13c37dbe3830" \
  --set-env-vars "RULE_STOP_LOSS_MIN_SPEND=300" \
  --set-env-vars "RULE_STOP_LOSS_MAX_ROAS=0.30"

echo ""
echo "部署完成！"
echo "服务将自动运行定时任务："
echo "  - 日报: 每日 09:00"
echo "  - 实时播报: 每日 08:10-23:10 (每小时)"
echo "  - 个人播报: 每日 09:15-23:15 (每小时)"
echo "  - 周报: 每周一 09:30"
echo "  - XMP统计: 每日 02:00"
