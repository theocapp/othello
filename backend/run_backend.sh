#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="/tmp/othello_v2_backend.log"
PID_FILE="/tmp/othello_v2_backend.pid"
if [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/venv/bin/python"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi
# Keep the web process lean by default. Heavy background refresh belongs in worker.py.
SCHEDULER_ENABLED="false"

cd "$ROOT_DIR"

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$existing_pid" ]] && [[ "$existing_pid" != "0" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "already-running:$existing_pid"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

OTHELLO_INTERNAL_SCHEDULER="$SCHEDULER_ENABLED" nohup "$PYTHON_BIN" -m uvicorn main:app --host 127.0.0.1 --port 8001 >"$LOG_FILE" 2>&1 &
new_pid=$!
echo "$new_pid" > "$PID_FILE"
echo "started:$new_pid"
