#!/bin/bash

# 脚本所在的目录
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# 主程序文件
RUN_PY="$DIR/run.py"
# 虚拟环境目录
VENV_DIR="$DIR/venv"
# 日志文件和PID文件
LOG_FILE="$DIR/app.log"
PID_FILE="$DIR/app.pid"

# 检查虚拟环境和主程序文件
check_files() {
#    if [ ! -d "$VENV_DIR" ]; then
#        echo "错误：找不到虚拟环境 'venv'。请先创建并激活它，然后安装依赖。"
#        echo "例如："
#        echo "python3 -m venv venv"
#        echo "source venv/bin/activate"
#        echo "pip install -r requirements.txt"
#        exit 1
#    fi
    if [ ! -f "$RUN_PY" ]; then
        echo "错误：找不到主程序文件 'run.py'。"
        exit 1
    fi
}

# 启动应用
start() {
    check_files
    if [ -f "$PID_FILE" ]; then
        echo "应用似乎已在运行 (PID 文件存在)。请先使用 'stop' 命令停止它。"
        exit 1
    fi

    echo "正在启动应用守护进程..."

    # 使用 nohup 在后台运行一个循环来守护 Python 应用
    nohup /bin/bash -c "
        # source \"$VENV_DIR/bin/activate\"
        while true; do
            echo \"[\$(date)] Starting run.py...\"
            python -u \"$RUN_PY\"
            echo \"[\$(date)] run.py exited with code \$?. Restarting in 5 seconds...\"
            sleep 5
        done
    " >> "$LOG_FILE" 2>&1 &

    # 保存守护进程的 PID
    echo $! > "$PID_FILE"

    echo "应用守护进程已启动。日志请查看: $LOG_FILE"
    echo "守护进程PID已保存到: $PID_FILE"
}

# 停止应用
stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "找不到 PID 文件。应用可能没有在运行。"
        exit 1
    fi

    GUARDIAN_PID=$(cat "$PID_FILE")
    if [ -z "$GUARDIAN_PID" ]; then
        echo "PID 文件为空。无法停止应用。"
        exit 1
    fi

    echo "正在停止守护进程: $GUARDIAN_PID"
    # 停止守护进程循环
    kill "$GUARDIAN_PID"

    # 杀死所有相关的 python 进程
    echo "正在停止所有 run.py 进程..."
    pkill -f "python -u $RUN_PY"

    # 检查守护进程是否已停止
    if ! kill -0 "$GUARDIAN_PID" 2>/dev/null; then
        echo "守护进程 $GUARDIAN_PID 已成功停止。"
        rm "$PID_FILE"
    else
        echo "无法停止守护进程 $GUARDIAN_PID。可能需要手动检查。"
    fi
    echo "应用已停止。"
}

# 检查应用状态
status() {
    if [ ! -f "$PID_FILE" ]; then
        echo "应用未运行 (找不到 PID 文件)。"
        exit 1
    fi

    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null; then
        echo "应用正在运行。守护进程 PID: $PID"
    else
        echo "应用未运行 (但 PID 文件存在，可能上次未正常关闭)。"
        echo "建议删除 PID 文件: rm $PID_FILE"
    fi
}

# 主逻辑：根据传入的参数执行不同操作
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    *)
        echo "用法: $0 {start|stop|status}"
        exit 1
esac

exit 0
