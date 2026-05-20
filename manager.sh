#!/bin/bash
# SmartMoney 数据管道管理脚本
# 用法: ./pipeline_manager.sh [status|start|stop|run|health|logs]

set -e

LABEL_UPDATE="com.smartmoney.update"
LABEL_HEALTH="com.smartmoney.healthcheck"
PYTHON="/Users/hainingyu/Code/smartmoney_hunter/.venv/bin/python3"
PIPELINE_DIR="/Users/hainingyu/Code/quant_pipeline"

# 颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

show_status() {
    echo ""
    echo "📊 SmartMoney 数据管道状态"
    echo "=========================="
    echo ""

    echo "【定时任务】"
    for label in "$LABEL_UPDATE" "$LABEL_HEALTH"; do
        status=$(launchctl list | grep "$label" | awk '{print $1}')
        if [ -n "$status" ]; then
            echo "  ✅ $label 已加载"
        else
            echo "  ❌ $label 未加载"
        fi
    done

    echo ""
    echo "【日志文件】"
    for log in "$HOME/Code/data/quant_data/logs/launchd_update.out.log" "$HOME/Code/data/quant_data/logs/launchd_health.out.log"; do
        if [ -f "$log" ]; then
            size=$(ls -lh "$log" | awk '{print $5}')
            mtime=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M" "$log")
            echo "  📄 $(basename "$log") ($size, 修改于 $mtime)"
        else
            echo "  📝 $(basename "$log") (尚未生成)"
        fi
    done

    echo ""
    echo "【数据库】"
    if [ -f "$HOME/Code/data/quant_data/quant_core.db" ]; then
        size=$(ls -lh "$HOME/Code/data/quant_data/quant_core.db" | awk '{print $5}')
        echo "  📦 quant_core.db ($size)"
    fi
}

start_agents() {
    echo "🚀 启动 SmartMoney 定时任务..."
    launchctl load "$HOME/Library/LaunchAgents/com.smartmoney.update.plist" 2>/dev/null || true
    launchctl load "$HOME/Library/LaunchAgents/com.smartmoney.healthcheck.plist" 2>/dev/null || true
    echo -e "${GREEN}✅ 定时任务已启动${NC}"
    echo "   • 每天 15:35 自动更新数据"
    echo "   • 每天 16:30 自动健康检查"
}

stop_agents() {
    echo "🛑 停止 SmartMoney 定时任务..."
    launchctl unload "$HOME/Library/LaunchAgents/com.smartmoney.update.plist" 2>/dev/null || true
    launchctl unload "$HOME/Library/LaunchAgents/com.smartmoney.healthcheck.plist" 2>/dev/null || true
    echo -e "${GREEN}✅ 定时任务已停止${NC}"
}

run_now() {
    echo "▶️  立即执行数据更新..."
    cd "$PIPELINE_DIR"
    $PYTHON daily_pipeline.py --task all --force
}

run_resume() {
    echo "🔄 断点续传：从上次中断位置继续..."
    cd "$PIPELINE_DIR"
    $PYTHON daily_pipeline.py --task update_bars --resume --force
}

run_health() {
    echo "🏥 立即执行健康检查..."
    cd "$PIPELINE_DIR"
    $PYTHON daily_pipeline.py --task health_check --force
}

show_logs() {
    echo "📜 最近的日志输出："
    echo ""
    latest_log=$(ls -t "$HOME/Code/data/quant_data/logs/smartmoney_"*.log 2>/dev/null | head -1)
    echo "--- 主日志 ($(basename "$latest_log")) ---"
    if [ -n "$latest_log" ] && [ -f "$latest_log" ]; then
        tail -30 "$latest_log"
    else
        echo "(暂无)"
    fi
}

monitor_pipeline() {
    echo "🔍 实时监控运行中的数据管道..."
    echo "（按 Ctrl+C 退出）"
    echo ""
    
    # 显示当前进程
    pid=$(pgrep -f "daily_pipeline.py" | head -1)
    if [ -n "$pid" ]; then
        echo "【运行状态】"
        echo "  PID: $pid"
        ps -o etime= -p "$pid" 2>/dev/null | xargs echo "  已运行时间:"
        echo ""
        echo "【实时日志】"
        tail -f "$HOME/Code/data/quant_data/logs/smartmoney_$(date +%Y%m%d).log" 2>/dev/null
    else
        echo "ℹ️  当前没有运行中的数据管道进程"
        echo ""
        echo "最近的日志:"
        latest_log=$(ls -t "$HOME/Code/data/quant_data/logs/smartmoney_"*.log 2>/dev/null | head -1)
        if [ -n "$latest_log" ]; then
            tail -20 "$latest_log"
        else
            echo "(暂无日志)"
        fi
    fi
}

daemon_run() {
    local task_args="$1"
    local pidfile="/tmp/smartmoney_daemon.pid"
    local logfile="$LOG_DIR/daemon.log"

    if [ -f "$pidfile" ]; then
        old_pid=$(cat "$pidfile")
        if kill -0 "$old_pid" 2>/dev/null; then
            echo "守护进程已在运行 (PID: $old_pid)"
            echo "   日志: $logfile"
            return 1
        fi
    fi

    echo "🔄 启动守护进程（崩溃自动重启）..."
    {
        while true; do
            echo "=== $(date '+%Y-%m-%d %H:%M:%S') 启动任务 ==="
            cd "$PIPELINE_DIR"
            $PYTHON daily_pipeline.py --task update_bars $task_args --force
            exit_code=$?
            if [ $exit_code -eq 0 ]; then
                echo "=== $(date '+%Y-%m-%d %H:%M:%S') 正常完成，等待1小时后重试 ==="
                sleep 3600
            elif [ $exit_code -eq 130 ] || [ $exit_code -eq 143 ]; then
                echo "=== $(date '+%Y-%m-%d %H:%M:%S') 被中断，退出守护 ==="
                break
            else
                echo "=== $(date '+%Y-%m-%d %H:%M:%S') 异常退出(code=$exit_code)，30秒后重启 ==="
                sleep 30
            fi
        done
    } >> "$logfile" 2>&1 &

    daemon_pid=$!
    echo "$daemon_pid" > "$pidfile"
    echo -e "${GREEN}✅ 守护进程已启动 (PID: $daemon_pid)${NC}"
    echo "   日志: $logfile"
    echo "   停止: $0 daemon-stop"
}

daemon_stop() {
    local pidfile="/tmp/smartmoney_daemon.pid"
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            echo "🛑 停止守护进程 (PID: $pid)..."
            kill -TERM "$pid" 2>/dev/null
            sleep 2
            if kill -0 "$pid" 2>/dev/null; then
                kill -KILL "$pid" 2>/dev/null
            fi
            echo -e "${GREEN}✅ 守护进程已停止${NC}"
        else
            echo "守护进程未在运行"
        fi
        rm -f "$pidfile"
    else
        echo "守护进程未运行"
    fi
}

# 主入口
case "${1:-status}" in
    status)
        show_status
        ;;
    start)
        start_agents
        ;;
    stop)
        stop_agents
        ;;
    restart)
        stop_agents
        sleep 1
        start_agents
        ;;
    run)
        run_now
        ;;
    resume)
        run_resume
        ;;
    health)
        run_health
        ;;
    logs)
        show_logs
        ;;
    monitor)
        monitor_pipeline
        ;;
    daemon)
        daemon_run ""
        ;;
    daemon-resume)
        daemon_run "--resume"
        ;;
    daemon-stop)
        daemon_stop
        ;;
    *)
        echo "Quant Data Pipeline 管理器"
        echo ""
        echo "用法: $0 [命令]"
        echo ""
        echo "命令:"
        echo "  status   查看定时任务和数据库状态 (默认)"
        echo "  start    启动定时任务"
        echo "  stop     停止定时任务"
        echo "  restart  重启定时任务"
        echo "  run      立即执行完整数据更新"
        echo "  resume   断点续传（从上次中断处继续）"
        echo "  health   立即执行健康检查"
        echo "  logs     查看最近日志"
        echo "  monitor      实时监控运行中的进程和日志"
    echo "  daemon       启动守护进程（崩溃自动重启）"
    echo "  daemon-resume  守护进程 + 断点续传"
    echo "  daemon-stop  停止守护进程"
        echo ""
        ;;
esac
