#!/bin/bash
# XMP 报表启动脚本
# 根据 REPORT_MODE 环境变量运行日报或周报

set -e

if [ "$REPORT_MODE" = "weekly" ]; then
    echo "=== 运行周报模式 ==="
    python -m xmp.xmp_scheduler --weekly --from-bq --lark-doc --days 5
else
    echo "=== 运行日报模式 ==="
    python -m xmp.xmp_scheduler --from-bq --lark-doc --yesterday
fi
