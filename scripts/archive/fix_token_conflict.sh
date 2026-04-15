#!/bin/bash
# XMP Token 冲突快速修复脚本
# 解决多个任务使用同一账号导致的 Token 互相挤掉问题

set -e

PROJECT_ID="fleet-blend-469520-n7"
REGION="asia-northeast3"

echo "=========================================="
echo "XMP Token 冲突修复"
echo "=========================================="
echo ""

# 1. 查看当前所有调度任务
echo "[1] 查看当前调度任务..."
gcloud scheduler jobs list \
    --location ${REGION} \
    --project ${PROJECT_ID} \
    --format="table(name,schedule,state)"

echo ""
echo "=========================================="
echo ""

# 2. 暂停 xmp-data-scraper（Playwright 爬虫）
echo "[2] 暂停 xmp-data-scraper 任务..."
echo "    原因: 与 xmp-internal-scraper 功能重复，且会导致 Token 冲突"
echo ""

read -p "是否暂停 xmp-data-scraper-scheduler? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    gcloud scheduler jobs pause xmp-data-scraper-scheduler \
        --location ${REGION} \
        --project ${PROJECT_ID}
    echo "✓ 已暂停 xmp-data-scraper-scheduler"
else
    echo "跳过暂停 xmp-data-scraper-scheduler"
fi

echo ""
echo "=========================================="
echo ""

# 3. 调整 xmp-internal-scraper-daily 时间
echo "[3] 调整 xmp-internal-scraper-daily 执行时间..."
echo "    当前: 02:00"
echo "    调整为: 01:00"
echo "    原因: 避免与其他任务冲突"
echo ""

read -p "是否调整时间? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    gcloud scheduler jobs update http xmp-internal-scraper-daily-scheduler \
        --location ${REGION} \
        --schedule "0 1 * * *" \
        --time-zone "Asia/Shanghai" \
        --project ${PROJECT_ID}
    echo "✓ 已调整为每天 01:00 执行"
else
    echo "跳过时间调整"
fi

echo ""
echo "=========================================="
echo "修复完成！"
echo "=========================================="
echo ""
echo "调整后的任务时间表:"
echo "  01:00 - xmp-internal-scraper-daily (T-1 数据)"
echo "  每 30 分钟 - xmp-internal-scraper (实时数据)"
echo "  已暂停 - xmp-data-scraper (Playwright 爬虫)"
echo ""
