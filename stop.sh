#!/usr/bin/env bash
# 停止 dida:只按 pid 文件里的 PID 精确停这一个进程
# (同机多实例时,每个实例独立目录、各自的 dida.pid,互不影响)
# 先优雅 SIGTERM,超时再 SIGKILL
# 用法: ./stop.sh

cd "$(dirname "$0")"

PIDFILE=dida.pid
WAIT=15  # 优雅等待秒数

if [ ! -f "$PIDFILE" ]; then
  echo "未找到 $PIDFILE,可能未在运行"
  exit 0
fi

PID=$(cat "$PIDFILE" 2>/dev/null || true)
if [ -z "$PID" ]; then
  echo "$PIDFILE 为空"
  rm -f "$PIDFILE"
  exit 0
fi

if ! kill -0 "$PID" 2>/dev/null; then
  echo "进程 $PID 不在运行,清理残留 $PIDFILE"
  rm -f "$PIDFILE"
  exit 0
fi

echo "停止中 PID=$PID"
kill "$PID" 2>/dev/null || true

for _ in $(seq 1 "$WAIT"); do
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "已停止"
    rm -f "$PIDFILE"
    exit 0
  fi
  sleep 1
done

echo "优雅停止超时,强制杀死 PID=$PID"
kill -9 "$PID" 2>/dev/null || true
rm -f "$PIDFILE"
echo "已强制停止"
