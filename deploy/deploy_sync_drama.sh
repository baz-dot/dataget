#!/bin/bash
# Cloud Run 部署脚本 - Drama Mapping 自动同步

# 配置变量
PROJECT_ID="fleet-blend-469520-n7"
SERVICE_NAME="drama-mapping-sync"
REGION="asia-northeast1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "开始部署 Drama Mapping 同步服务到 Cloud Run..."

# 切换到项目根目录
cd "$(dirname "$0")/.."

# 1. 构建 Docker 镜像
echo "步骤 1: 构建 Docker 镜像..."
docker build -f deploy/Dockerfile.sync-drama -t ${IMAGE_NAME} .

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
  --timeout 600 \
  --memory 512Mi \
  --cpu 1 \
  --set-env-vars "TZ=Asia/Shanghai" \
  --set-env-vars "BQ_PROJECT_ID=${PROJECT_ID}"

echo "部署完成！"
echo "服务名称: ${SERVICE_NAME}"
echo "区域: ${REGION}"
