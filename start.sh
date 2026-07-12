#!/usr/bin/env bash
# 启动 task-schedule
# - 用 pid 文件定位进程(支持同机多实例:每个实例独立目录,各自维护 task-schedule.pid)
# - 自动检测 CPU 架构,选对应二进制:task-schedule-linux-amd64 / task-schedule-linux-arm64
# 用法: ./start.sh

set -euo pipefail
cd "$(dirname "$0")"

PIDFILE=task-schedule.pid
LOG=stdio.log

# 1) 用 pid 文件查重:已在运行则不重复拉起
if [ -f "$PIDFILE" ]; then
  PID=$(cat "$PIDFILE" 2>/dev/null || true)
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    echo "已在运行,PID=$PID(按 $PIDFILE)"
    exit 1
  fi
  # pid 文件残留但进程已退出,清理
  rm -f "$PIDFILE"
fi

# 2) 检测架构,选择二进制
ARCH=$(uname -m)
case "$ARCH" in
  x86_64|amd64)  BIN=task-schedule-linux-amd64 ;;
  aarch64|arm64) BIN=task-schedule-linux-arm64 ;;
  *) echo "错误: 不支持的架构 $ARCH" >&2; exit 1 ;;
esac

if [ ! -f "$BIN" ]; then
  echo "错误: 找不到可执行文件 $BIN(当前架构 $ARCH)" >&2
  exit 1
fi

# 3) 启动
nohup ./"$BIN" > "$LOG" 2>&1 &
PID=$!
echo "$PID" > "$PIDFILE"
echo "已启动 [$BIN] PID=$PID,日志: $LOG,pid: $PIDFILE"

sleep 1
if kill -0 "$PID" 2>/dev/null; then
  echo "进程存活,启动成功"
else
  echo "警告: 进程未存活,请检查 $LOG" >&2
  rm -f "$PIDFILE"
  exit 1
fi
