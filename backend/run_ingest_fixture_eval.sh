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
START_DATE=""
END_DATE=""
COUNT=100
APPEND=true
PROVIDER="anthropic"
MODEL=""
SKIP_INGEST=false
SKIP_GENERATE=false
SKIP_EVAL=false
LOAD_ENV=true

usage() {
  cat <<'EOF'
Usage:
  ./run_ingest_fixture_eval.sh --start-date YYYY-MM-DD --end-date YYYY-MM-DD [options]

Required:
  --start-date DATE     Start date for GDELT backfill window
  --end-date DATE       End date for GDELT backfill window

Options:
  --topic TOPIC         geopolitics | economics | all (default: all)
  --count N             Number of generated pairs to label (default: 100)
  --provider NAME       Label provider: anthropic or groq (default: anthropic)
  --model NAME          Optional model override passed to eval.generate_fixtures
  --no-append           Overwrite generated fixtures instead of appending
  --skip-ingest         Skip GDELT ingest backfill step
  --skip-generate       Skip fixture generation step
  --skip-eval           Skip eval.run_all step
  --no-env-load         Do not source backend/.env
  --help                Show this help

Examples:
  ./run_ingest_fixture_eval.sh --start-date 2026-04-01 --end-date 2026-04-07 --count 100
  ./run_ingest_fixture_eval.sh --start-date 2026-04-01 --end-date 2026-04-07 --topic geopolitics --count 50
  ./run_ingest_fixture_eval.sh --start-date 2026-04-01 --end-date 2026-04-07 --provider groq --count 100
  ./run_ingest_fixture_eval.sh --start-date 2026-04-01 --end-date 2026-04-07 --skip-ingest
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --topic)
      TOPIC="$2"
      shift 2
      ;;
    --start-date)
      START_DATE="$2"
      shift 2
      ;;
    --end-date)
      END_DATE="$2"
      shift 2
      ;;
    --count)
      COUNT="$2"
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
    --no-append)
      APPEND=false
      shift
      ;;
    --skip-ingest)
      SKIP_INGEST=true
      shift
      ;;
    --skip-generate)
      SKIP_GENERATE=true
      shift
      ;;
    --skip-eval)
      SKIP_EVAL=true
      shift
      ;;
    --no-env-load)
      LOAD_ENV=false
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

if [[ "$SKIP_INGEST" != true ]]; then
  if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "ERROR: --start-date and --end-date are required unless --skip-ingest is set."
    usage
    exit 1
  fi
fi

if [[ "$LOAD_ENV" == true && -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

if [[ "$SKIP_INGEST" != true ]]; then
  echo "[loop] Ingesting real articles from GDELT (topic=$TOPIC, start=$START_DATE, end=$END_DATE)..."
  "$PYTHON_BIN" -m ingest_gdelt --topic "$TOPIC" --start-date "$START_DATE" --end-date "$END_DATE"
fi

if [[ "$SKIP_GENERATE" != true ]]; then
  if [[ "$PROVIDER" != "anthropic" && "$PROVIDER" != "groq" ]]; then
    echo "ERROR: --provider must be 'anthropic' or 'groq'"
    exit 1
  fi

  if [[ "$PROVIDER" == "anthropic" ]]; then
    if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
      echo "ERROR: ANTHROPIC_API_KEY is not set."
      echo "Set it in backend/.env or export it before running this script."
      exit 1
    fi
    if ! "$PYTHON_BIN" -c "import anthropic" >/dev/null 2>&1; then
      echo "ERROR: anthropic package is not installed for $PYTHON_BIN"
      echo "Install with: $PYTHON_BIN -m pip install anthropic"
      exit 1
    fi
  else
    if [[ -z "${GROQ_API_KEY:-}" ]]; then
      echo "ERROR: GROQ_API_KEY is not set."
      echo "Set it in backend/.env or export it before running this script."
      exit 1
    fi
  fi

  echo "[loop] Generating labeled fixtures from corpus (--provider $PROVIDER --count $COUNT)..."
  model_arg=()
  if [[ -n "$MODEL" ]]; then
    model_arg=(--model "$MODEL")
  fi
  if [[ "$APPEND" == true ]]; then
    "$PYTHON_BIN" -m eval.generate_fixtures --provider "$PROVIDER" --count "$COUNT" --append "${model_arg[@]}"
  else
    "$PYTHON_BIN" -m eval.generate_fixtures --provider "$PROVIDER" --count "$COUNT" "${model_arg[@]}"
  fi
fi

if [[ "$SKIP_EVAL" != true ]]; then
  echo "[loop] Running eval harness..."
  "$PYTHON_BIN" -m eval.run_all
fi

echo "[loop] Done."