#!/bin/zsh
set -euo pipefail

PID_FILE="/tmp/othello_v2_backend.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "not-running"
  exit 0
fi

pid="$(cat "$PID_FILE" 2>/dev/null || true)"
if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
fi

rm -f "$PID_FILE"
echo "stopped"
