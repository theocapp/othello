# Othello V2

Othello V2 is an ingestion-first intelligence dashboard rather than a startup-fetch news app.

## Architecture

The system is split into four layers:

1. `news.py`
   GDELT-first source adapter with NewsAPI fallback.
2. `backend/db` package (formerly `corpus.py`)
   Durable article store and ingestion history. A small compatibility shim remains at `backend/corpus.py` that re-exports the new `db.*` modules; callers should migrate to importing from `db.*` directly.
3. `app_factory.py` + `bootstrap.py` + `core/` + `services/`
   API wiring, runtime initialization, scheduling, and derived intelligence services.
4. `frontend/`
   Presentation layer that reads the corpus-derived intelligence view.

`main.py` is now a thin entrypoint that constructs the FastAPI app.

## Backend flow

- articles are ingested into `othello_corpus.db`
- article embeddings are stored in Chroma for semantic retrieval
- entity mentions and co-occurrences are persisted in `entities.db`
- events are clustered from stored articles
- headlines and briefings are built from those clustered events

The homepage no longer assumes startup warmup created a temporary cache. It is driven by the stored corpus.

## Runtime behavior

- ingestion runs every 15 minutes
- snapshots refresh every hour
- if the corpus is empty at startup, Othello initializes storage, seeds sources, and attempts a bootstrap from any legacy cache data already present
- if `GROQ_API_KEY` is missing, the site still works with deterministic fallbacks
- if `ANTHROPIC_API_KEY` is missing, contradiction analysis is skipped

## Environment

Backend examples live in `backend/.env.example`.
Frontend examples live in `frontend/.env.example`.

Required for full analyst generation:

- `GROQ_API_KEY`

Optional:

- `ANTHROPIC_API_KEY`
- `NEWS_API_KEY`
- `GROQ_MODEL`
- `OTHELLO_SOURCE_PROVIDER`
- `OTHELLO_ADMIN_API_KEY`
- `OTHELLO_CORS_ORIGINS`
- `VITE_API_BASE_URL`

Notes:

- GDELT is the primary ingestion source and does not require an API key.
- NewsAPI is retained as a fallback adapter.
- In production, the frontend now works cleanly with a same-origin API when `VITE_API_BASE_URL` is unset.

## Frontend

The frontend presents:

- corpus status
- top stories derived from the article universe
- clustered event radar
- topic briefing rooms
- entity movement and source distribution
- archive-driven analysis and timelines

## Dev

Backend:

```bash
cd backend
source .venv/bin/activate
uvicorn main:app --reload --port 8001
```

Recommended local split:

```bash
cd backend
./run_backend.sh
```

This starts the API with `OTHELLO_INTERNAL_SCHEDULER=false`, so the web server stays lean.

Worker:

```bash
cd backend
./run_worker.sh
```

This runs ingestion, translations, and scheduled refresh jobs outside the API process.

Default worker behavior is intentionally lean:

- `OTHELLO_WORKER_ENABLE_INGESTION=true` keeps the corpus/feed refresh loop on
- `OTHELLO_WORKER_BOOTSTRAP_MODE=ingest` performs the light startup ingest path by default
- `OTHELLO_WORKER_ENABLE_TRANSLATIONS=false` keeps local translation models out of the always-on worker
- `OTHELLO_WORKER_ENABLE_ANALYTICS=false` keeps heavy narrative/foresight/source-reliability jobs out of the always-on worker

If you want a fuller background worker later, opt in with env vars before starting it.

Frontend:

```bash
cd frontend
npm run dev
```

Or use the root Makefile shortcuts:

```bash
make backend
make worker
make frontend
make test
```

## CI

GitHub Actions now runs:

- backend smoke tests and runtime wiring tests
- frontend production build

For non-local clients, write/refresh routes require `X-API-Key: <OTHELLO_ADMIN_API_KEY>`.
