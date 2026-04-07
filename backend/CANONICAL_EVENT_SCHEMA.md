# Canonical event schema & migration plan

Status: draft

Purpose
-------
Make `canonical_events` the single source-of-truth for event-level semantics. All ingestion and downstream logic should prefer `canonical_events` for event identity, with `structured_events` and `materialized_story_clusters` treated as normalized evidence or materialized derivatives.

Design goals
------------
- Single canonical record per real-world event (or event cluster) with stable `event_id`.
- Keep source-level evidence (dataset rows, article URLs, raw payloads) as inputs, not the authoritative event representation.
- Keep per-source claims/perspectives attached to the canonical event via `event_perspectives`.
- Mark large, computed tables (story clusters, contradiction snapshots) as materialized/derived and refreshable.

Canonical schema (recommended)
------------------------------
Postgres DDL (recommended final shape):

```sql
CREATE TABLE IF NOT EXISTS canonical_events (
  event_id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  label TEXT NOT NULL,
  event_type TEXT,
  status TEXT NOT NULL DEFAULT 'developing',
  geo_country TEXT,
  geo_region TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  first_reported_at TIMESTAMPTZ,
  last_updated_at TIMESTAMPTZ,
  article_count INTEGER NOT NULL DEFAULT 0,
  source_count INTEGER NOT NULL DEFAULT 0,
  perspective_count INTEGER NOT NULL DEFAULT 0,
  contradiction_count INTEGER NOT NULL DEFAULT 0,
  neutral_summary TEXT,
  neutral_confidence DOUBLE PRECISION,
  neutral_generated_at TIMESTAMPTZ,
  linked_structured_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  article_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_canonical_events_topic ON canonical_events (topic, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_canonical_events_status ON canonical_events (status, last_updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_canonical_events_linked_gin ON canonical_events USING GIN (linked_structured_event_ids jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_canonical_events_article_urls_gin ON canonical_events USING GIN (article_urls jsonb_path_ops);
```

Notes on SQLite / legacy DB
- The repo has both a Postgres-optimized path and a SQLite fallback. Keep the existing `canonical_events` rows for compatibility; use JSON/text columns on SQLite and skip GIN indexes.

Minimal tightening rules
-----------------------
- `topic` and `label` should be NOT NULL (add via two-step migration: fill nulls → alter column).
- `first_seen_at` and `computed_at` should be populated and comparable timestamps.
- `linked_structured_event_ids` must record provenance for every canonical record (empty array allowed during migration but aim to populate).

Migration plan (zero-to-low downtime)
------------------------------------
Prereqs: backup DB (pg_dump / copy sqlite file), run in stage first.

1) Back up the production DB.

2) Backfill canonical events from existing materialized pipelines.
   - The repo already contains a story-materialization pipeline (`backend/story_materialization.py`) which builds canonical rows and perspectives. Run it to seed canonical rows.
   - If a one-shot script is preferred, run the story materializer over historical windows (week / month) and verify counts.

3) Verify parity and provenance.
   - Query counts: number of `structured_events` vs `canonical_events` and distribution of `linked_structured_event_ids`.
   - Spot-check 50 canonical rows to ensure `linked_structured_event_ids` is populated and `payload` contains provenance.

4) Tighten schema in safe steps.
   - Update null topics/labels:
     ```sql
     UPDATE canonical_events SET topic = 'uncategorized' WHERE topic IS NULL;
     UPDATE canonical_events SET label = coalesce(label, 'unnamed event') WHERE label IS NULL;
     ```
   - Then add NOT NULL constraints (Postgres):
     ```sql
     ALTER TABLE canonical_events ALTER COLUMN topic SET NOT NULL;
     ALTER TABLE canonical_events ALTER COLUMN label SET NOT NULL;
     ```

5) Add indexes for query patterns (GIN on JSONB for Postgres). See DDL above.

6) Switch consumers to prefer `canonical_events`.
   - Files to change (search results):
     - `backend/structured_story_rollups.py` — prefer canonical events for clustering and deduping.
     - `backend/correlation_engine.py` — use canonical events as input for correlation and aggregation.
     - `backend/services/map_service.py` — prefer `get_canonical_events` and `get_canonical_event` for map summaries.
     - `backend/country_instability.py` — read canonical events for event-level analysis; fall back to `structured_events` only when canonical mapping missing.
     - `backend/db/events_repo.py` and `backend/corpus.py` — keep both DB-layer implementations in sync.

   - Refactor pattern: replace `get_recent_structured_events(...)` with `get_canonical_events(...)` or with `get_canonical_event(event_id)` + `get_event_perspectives(event_id)` when per-event detail required.

7) Make derived tables explicitly materialized and refreshable.
   - Add DB comments to mark intent, e.g.:
     ```sql
     COMMENT ON TABLE materialized_story_clusters IS 'MATERIALIZED: derived from canonical_events; refresh via story_materialization pipeline';
     ```
   - Consider renaming long-lived but derived tables (optional): `materialized_story_clusters` → `materialized_story_clusters__derived_v1` to signal non-authoritative nature.

8) Tests & validation
   - Add unit/integration tests asserting that ingestion pipelines create canonical events with `linked_structured_event_ids` populated.
   - Update `backend/test_api_smoke.py` and `backend/test_story_materialization.py` to cover canonical invariants.

9) Rollout
   - Deploy code that prefers canonical reads but keeps fallback paths behind a feature-flag.
   - After monitoring for a few hours/days, run constraints/add NOT NULLs and drop or deprecate duplicate tables only after clients are migrated.

Implementation notes for maintainers
----------------------------------
- There are two DB-layer implementations (`backend/corpus.py` for generic/PG and `backend/db/*.py` variants). Keep both updated when changing function signatures or DDL.
- Ingestion should be idempotent: `upsert_structured_events` (now) should also ensure `canonical_events` are created/linked. We added a best-effort `upsert_canonical_events` call in the ingestion path; the long-term plan is to centralize canonical creation in story-materialization (authoritative) and keep ingestion as a lightweight source-updater.
- Performance: add GIN indexes for Postgres JSONB; consider partial indexes for active topics/statuses.

Appendix: quick verification queries
----------------------------------

Count canonical events with no linked structured IDs:

```sql
SELECT COUNT(*) FROM canonical_events WHERE jsonb_array_length(linked_structured_event_ids) = 0;
```

Top topics:

```sql
SELECT topic, COUNT(*) FROM canonical_events GROUP BY topic ORDER BY COUNT(*) DESC LIMIT 20;
```

Check for stale canonical rows with no recent updates:

```sql
SELECT event_id FROM canonical_events WHERE computed_at < now() - interval '30 days';
```

---

If you want, I can now:

- generate the migration SQL scripts for Postgres + SQLite,
- start implementing code changes for one consumer (pick a file), or
- add tests that assert ingestion → canonical upsert behavior.

Choose which of the above you want me to do next.
