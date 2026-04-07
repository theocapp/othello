-- Migration: add/ensure canonical_events columns and indexes (Postgres)
-- Safe to run multiple times. Back up DB before running.

BEGIN;

-- Add new columns if missing
ALTER TABLE canonical_events
  ADD COLUMN IF NOT EXISTS neutral_summary TEXT,
  ADD COLUMN IF NOT EXISTS neutral_confidence DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS neutral_generated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS linked_structured_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS article_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Safe backfills for NOT NULL tightening
UPDATE canonical_events SET topic = 'uncategorized' WHERE topic IS NULL OR topic = '';
UPDATE canonical_events SET label = COALESCE(label, 'unnamed event') WHERE label IS NULL OR label = '';

-- After backfill, add NOT NULL constraints
ALTER TABLE canonical_events ALTER COLUMN topic SET NOT NULL;
ALTER TABLE canonical_events ALTER COLUMN label SET NOT NULL;

-- Indexes for JSONB columns to speed provenance queries
CREATE INDEX IF NOT EXISTS idx_canonical_events_linked_gin ON canonical_events USING GIN (linked_structured_event_ids);
CREATE INDEX IF NOT EXISTS idx_canonical_events_article_urls_gin ON canonical_events USING GIN (article_urls);

COMMIT;

-- Notes:
-- - If your PG version supports jsonb_path_ops and you prefer that operator class,
--   create indexes with jsonb_path_ops for smaller GIN size.
-- - Run this during a maintenance window if your table is very large; the ALTERs
--   may take time depending on the storage and WAL settings.
