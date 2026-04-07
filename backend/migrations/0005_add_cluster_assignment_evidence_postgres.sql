-- Migration: add per-article cluster assignment evidence table (Postgres)
-- Safe to run multiple times. Back up DB before running.

BEGIN;

CREATE TABLE IF NOT EXISTS cluster_assignment_evidence (
  id BIGSERIAL PRIMARY KEY,
  observation_key TEXT NOT NULL,
  event_id TEXT REFERENCES canonical_events(event_id) ON DELETE CASCADE,
  topic TEXT,
  article_url TEXT NOT NULL REFERENCES articles(url) ON DELETE CASCADE,
  rule TEXT NOT NULL,
  entity_overlap INTEGER NOT NULL DEFAULT 0,
  anchor_overlap INTEGER NOT NULL DEFAULT 0,
  keyword_overlap INTEGER NOT NULL DEFAULT 0,
  time_gap_hours DOUBLE PRECISION,
  final_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  computed_at DOUBLE PRECISION NOT NULL,
  UNIQUE (observation_key, article_url)
);

COMMENT ON TABLE cluster_assignment_evidence IS 'DERIVED: per-article assignment evidence for volatile observation clusters; refresh via story_materialization pipeline';

CREATE INDEX IF NOT EXISTS idx_cluster_assignment_obs ON cluster_assignment_evidence (observation_key, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_cluster_assignment_event ON cluster_assignment_evidence (event_id, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_cluster_assignment_topic ON cluster_assignment_evidence (topic, computed_at DESC);

COMMIT;
