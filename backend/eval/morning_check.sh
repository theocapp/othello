#!/bin/bash
set -a
source "$(dirname "$0")/../.env"
set +a

cd "$(dirname "$0")/.."

echo "========================================"
echo "Check: $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "========================================"

echo ""
echo "--- Corpus size ---"
.venv/bin/python -c "
from db.common import _connect
with _connect() as conn:
    total = conn.execute(\"SELECT COUNT(*) FROM articles\").fetchone()
    english = conn.execute(\"SELECT COUNT(*) FROM articles WHERE language = 'en'\").fetchone()
    recent = conn.execute(\"SELECT COUNT(*) FROM articles WHERE last_ingested_at > extract(epoch from now() - interval '24 hours')\").fetchone()
    print(f'Total: {dict(total)[\"count\"]}')
    print(f'English: {dict(english)[\"count\"]}')
    print(f'Ingested last 24h: {dict(recent)[\"count\"]}')
"

echo ""
echo "--- Eval harness ---"
.venv/bin/python -m eval.run_all 2>&1 | grep -E "PASS|FAIL|failures|passed|Known"
