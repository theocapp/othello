-- Migration: add canonical event importance fields and observation history (Postgres)
-- Safe to run multiple times. Back up DB before running.

BEGIN;

ALTER TABLE canonical_events
  ADD COLUMN IF NOT EXISTS importance_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS importance_reasons JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS canonical_event_observations (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL REFERENCES canonical_events(event_id) ON DELETE CASCADE,
  topic TEXT,
  observation_key TEXT NOT NULL,
  observed_at DOUBLE PRECISION NOT NULL,
  article_count INTEGER NOT NULL,
  source_count INTEGER NOT NULL,
  contradiction_count INTEGER NOT NULL,
  tier_1_source_count INTEGER NOT NULL DEFAULT 0,
  importance_score DOUBLE PRECISION,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (event_id, observation_key)
);

CREATE INDEX IF NOT EXISTS idx_canonical_event_obs_event ON canonical_event_observations (event_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_canonical_event_obs_topic ON canonical_event_observations (topic, observed_at DESC);

COMMIT;
