#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SESSION_NAME="othello_v2_worker"
LOG_FILE="/tmp/othello_v2_worker.log"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/venv/bin/python"
else
  PYTHON_BIN="python3"
fi

if screen -ls | grep -q "[.]${SESSION_NAME}[[:space:]]"; then
  echo "already-running:${SESSION_NAME}"
  exit 0
fi

screen -dmS "$SESSION_NAME" /bin/zsh -lc "cd '$ROOT_DIR' && '$PYTHON_BIN' worker.py >'$LOG_FILE' 2>&1"
echo "started:${SESSION_NAME}"
