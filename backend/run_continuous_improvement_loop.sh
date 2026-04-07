#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/venv/bin/python"
else
  PYTHON_BIN="python3"
fi

TOPIC="all"
PROVIDER="groq"
MODEL=""
COUNT=20
BACKFILL_DAYS=2
INTERVAL_MINUTES=30
MAX_CYCLES=0
LOAD_ENV=true
LIGHT_INGEST=true
LOG_FILE="$ROOT_DIR/reports/continuous_loop.log"
AUTO_TUNE_CLUSTERING=false
TUNE_ITERATIONS=12
TUNE_SEED=42

usage() {
  cat <<'EOF'
Usage:
  ./run_continuous_improvement_loop.sh [options]

Options:
  --topic TOPIC             geopolitics | economics | all (default: all)
  --provider NAME           anthropic | groq (default: groq)
  --model NAME              Optional provider model override
  --count N                 Labeled fixture pairs per cycle (default: 20)
  --backfill-days N         Rolling ingest window size in days (default: 2)
  --interval-minutes N      Minutes between cycles (default: 30)
  --max-cycles N            Stop after N cycles (0 = run forever, default: 0)
  --log-file PATH           Log output file (default: backend/reports/continuous_loop.log)
  --auto-tune-clustering    Run parameter auto-tuning when eval fails
  --tune-iterations N       Auto-tune iterations per failed cycle (default: 12)
  --tune-seed N             Auto-tune random seed (default: 42)
  --no-env-load             Do not source backend/.env
  --full-ingest             Use full ingest path instead of lightweight ingest
  --help                    Show this help

Examples:
  ./run_continuous_improvement_loop.sh
  ./run_continuous_improvement_loop.sh --topic geopolitics --count 30 --interval-minutes 20
  ./run_continuous_improvement_loop.sh --provider anthropic --max-cycles 6
  ./run_continuous_improvement_loop.sh --auto-tune-clustering --tune-iterations 20
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --topic)
      TOPIC="$2"
      shift 2
      ;;
    --provider)
      PROVIDER="$2"
      shift 2
      ;;
    --model)
      MODEL="$2"
      shift 2
      ;;
    --count)
      COUNT="$2"
      shift 2
      ;;
    --backfill-days)
      BACKFILL_DAYS="$2"
      shift 2
      ;;
    --interval-minutes)
      INTERVAL_MINUTES="$2"
      shift 2
      ;;
    --max-cycles)
      MAX_CYCLES="$2"
      shift 2
      ;;
    --log-file)
      LOG_FILE="$2"
      shift 2
      ;;
    --auto-tune-clustering)
      AUTO_TUNE_CLUSTERING=true
      shift
      ;;
    --tune-iterations)
      TUNE_ITERATIONS="$2"
      shift 2
      ;;
    --tune-seed)
      TUNE_SEED="$2"
      shift 2
      ;;
    --no-env-load)
      LOAD_ENV=false
      shift
      ;;
    --full-ingest)
      LIGHT_INGEST=false
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ "$PROVIDER" != "anthropic" && "$PROVIDER" != "groq" ]]; then
  echo "ERROR: --provider must be 'anthropic' or 'groq'"
  exit 1
fi

if [[ "$LOAD_ENV" == true && -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

if [[ "$PROVIDER" == "anthropic" ]]; then
  if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
    echo "ERROR: ANTHROPIC_API_KEY is not set."
    exit 1
  fi
  if ! "$PYTHON_BIN" -c "import anthropic" >/dev/null 2>&1; then
    echo "ERROR: anthropic package is not installed for $PYTHON_BIN"
    exit 1
  fi
else
  if [[ -z "${GROQ_API_KEY:-}" ]]; then
    echo "ERROR: GROQ_API_KEY is not set."
    exit 1
  fi
fi

mkdir -p "$(dirname "$LOG_FILE")"

cycle=0
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] continuous-loop start (topic=$TOPIC provider=$PROVIDER count=$COUNT interval=${INTERVAL_MINUTES}m backfill=${BACKFILL_DAYS}d)" | tee -a "$LOG_FILE"

while true; do
  cycle=$((cycle + 1))
  cycle_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  read -r START_DATE END_DATE <<<"$($PYTHON_BIN - <<PY
from datetime import datetime, timedelta, timezone
days = int(${BACKFILL_DAYS})
end = datetime.now(timezone.utc).date()
start = end - timedelta(days=days)
print(start.isoformat(), end.isoformat())
PY
)"

  echo "[$cycle_start] cycle=$cycle start_date=$START_DATE end_date=$END_DATE" | tee -a "$LOG_FILE"

  ingest_ok=true
  if [[ "$LIGHT_INGEST" == true ]]; then
    if ! "$PYTHON_BIN" -m ingest_gdelt --topic "$TOPIC" --start-date "$START_DATE" --end-date "$END_DATE" --skip-entities --skip-chroma >>"$LOG_FILE" 2>&1; then
      ingest_ok=false
    fi
  else
    if ! "$PYTHON_BIN" -m ingest_gdelt --topic "$TOPIC" --start-date "$START_DATE" --end-date "$END_DATE" >>"$LOG_FILE" 2>&1; then
      ingest_ok=false
    fi
  fi

  if [[ "$ingest_ok" != true ]]; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] cycle=$cycle ingest_failed continuing_with_existing_corpus" | tee -a "$LOG_FILE"
  fi

  model_arg=()
  if [[ -n "$MODEL" ]]; then
    model_arg=(--model "$MODEL")
  fi

  if ! "$PYTHON_BIN" -m eval.generate_fixtures --provider "$PROVIDER" --count "$COUNT" --append "${model_arg[@]}" >>"$LOG_FILE" 2>&1; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] cycle=$cycle fixture_generation_failed" | tee -a "$LOG_FILE"
  fi

  if "$PYTHON_BIN" -m eval.run_all >>"$LOG_FILE" 2>&1; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] cycle=$cycle eval=PASS" | tee -a "$LOG_FILE"
  else
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] cycle=$cycle eval=FAIL" | tee -a "$LOG_FILE"
    tail -n 120 "$LOG_FILE" | grep -E "\[FAIL\]|Clustering failures|Importance failures|Identity failures|generated_" | tail -n 40 || true

    if [[ "$AUTO_TUNE_CLUSTERING" == true ]]; then
      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] cycle=$cycle auto_tune=START iterations=$TUNE_ITERATIONS" | tee -a "$LOG_FILE"
      if "$PYTHON_BIN" -m eval.auto_tune_clustering --iterations "$TUNE_ITERATIONS" --seed "$TUNE_SEED" >>"$LOG_FILE" 2>&1; then
        if "$PYTHON_BIN" -m eval.run_all >>"$LOG_FILE" 2>&1; then
          echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] cycle=$cycle auto_tune=IMPROVED eval=PASS" | tee -a "$LOG_FILE"
        else
          echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] cycle=$cycle auto_tune=DONE eval=STILL_FAIL" | tee -a "$LOG_FILE"
        fi
      else
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] cycle=$cycle auto_tune=FAILED" | tee -a "$LOG_FILE"
      fi
    fi
  fi

  if [[ "$MAX_CYCLES" -gt 0 && "$cycle" -ge "$MAX_CYCLES" ]]; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] reached max cycles ($MAX_CYCLES), stopping" | tee -a "$LOG_FILE"
    break
  fi

  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] sleeping ${INTERVAL_MINUTES}m before next cycle" | tee -a "$LOG_FILE"
  sleep "$((INTERVAL_MINUTES * 60))"
done
