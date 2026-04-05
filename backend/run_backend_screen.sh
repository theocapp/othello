#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SESSION_NAME="othello_v2_api"
LOG_FILE="/tmp/othello_v2_backend.log"

if [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/venv/bin/python"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi
SCHEDULER_ENABLED="${OTHELLO_INTERNAL_SCHEDULER:-false}"

if screen -ls | grep -q "[.]${SESSION_NAME}[[:space:]]"; then
  echo "already-running:${SESSION_NAME}"
  exit 0
fi

screen -dmS "$SESSION_NAME" /bin/zsh -lc "cd '$ROOT_DIR' && OTHELLO_INTERNAL_SCHEDULER='$SCHEDULER_ENABLED' '$PYTHON_BIN' -m uvicorn main:app --host 127.0.0.1 --port 8001 >'$LOG_FILE' 2>&1"
echo "started:${SESSION_NAME}"
