#!/usr/bin/env bash
# SmartMoney 每日数据管道入口
# 用法: ./run_daily.sh
# 推荐通过 crontab 每日收盘后执行

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

TODAY=$(date +%Y%m%d)
LOG_FILE="$LOG_DIR/daily_pipeline_$TODAY.log"

echo "$(date '+%Y-%m-%d %H:%M:%S') 开始每日数据管道" | tee -a "$LOG_FILE"

# .env 里的 XUEQIU_TOKEN / XUEQIU_USER_ID 会被 smartmoney_hunter.xueqiu 自动加载
uv run python daily_pipeline.py --task all --resume >> "$LOG_FILE" 2>&1 || {
    echo "$(date '+%Y-%m-%d %H:%M:%S') 数据管道失败，查看日志: $LOG_FILE" | tee -a "$LOG_FILE"
    exit 1
}

echo "$(date '+%Y-%m-%d %H:%M:%S') 每日数据管道完成" | tee -a "$LOG_FILE"
