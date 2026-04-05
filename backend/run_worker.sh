#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="/tmp/othello_v2_worker.log"
PID_FILE="/tmp/othello_v2_worker.pid"
if [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/venv/bin/python"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi
SCHEDULER_ENABLED="true"
BOOTSTRAP_MODE="${OTHELLO_WORKER_BOOTSTRAP_MODE:-ingest}"
ENABLE_INGESTION="${OTHELLO_WORKER_ENABLE_INGESTION:-true}"
ENABLE_TRANSLATIONS="${OTHELLO_WORKER_ENABLE_TRANSLATIONS:-false}"
ENABLE_ANALYTICS="${OTHELLO_WORKER_ENABLE_ANALYTICS:-false}"

cd "$ROOT_DIR"

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "already-running:$existing_pid"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

OTHELLO_INTERNAL_SCHEDULER="$SCHEDULER_ENABLED" \
OTHELLO_WORKER_BOOTSTRAP_MODE="$BOOTSTRAP_MODE" \
OTHELLO_WORKER_ENABLE_INGESTION="$ENABLE_INGESTION" \
OTHELLO_WORKER_ENABLE_TRANSLATIONS="$ENABLE_TRANSLATIONS" \
OTHELLO_WORKER_ENABLE_ANALYTICS="$ENABLE_ANALYTICS" \
nohup "$PYTHON_BIN" worker.py >"$LOG_FILE" 2>&1 &
new_pid=$!
echo "$new_pid" > "$PID_FILE"
echo "started:$new_pid"
