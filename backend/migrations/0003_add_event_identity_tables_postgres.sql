-- Migration: add event identity mapping tables and indexes (Postgres)
-- Safe to run multiple times. Back up DB before running.

BEGIN;

CREATE TABLE IF NOT EXISTS event_identity_map (
  observation_key TEXT PRIMARY KEY,
  event_id TEXT NOT NULL REFERENCES canonical_events(event_id) ON DELETE CASCADE,
  topic TEXT,
  first_mapped_at DOUBLE PRECISION NOT NULL,
  last_seen_at DOUBLE PRECISION NOT NULL,
  identity_confidence DOUBLE PRECISION,
  identity_reasons JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS event_identity_events (
  id BIGSERIAL PRIMARY KEY,
  observation_key TEXT NOT NULL,
  event_id TEXT NOT NULL REFERENCES canonical_events(event_id) ON DELETE CASCADE,
  action TEXT NOT NULL,
  confidence DOUBLE PRECISION,
  reasons JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_identity_map_event ON event_identity_map (event_id, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_event_identity_map_topic ON event_identity_map (topic, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_event_identity_events_event ON event_identity_events (event_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_event_identity_events_obs ON event_identity_events (observation_key, created_at DESC);

COMMIT;
