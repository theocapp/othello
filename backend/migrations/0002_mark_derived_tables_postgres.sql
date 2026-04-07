-- Mark selected tables as derived/materialized to clarify intent.
-- Run in Postgres: psql $DATABASE_URL -f backend/migrations/0002_mark_derived_tables_postgres.sql

BEGIN;

COMMENT ON TABLE materialized_story_clusters IS 'MATERIALIZED: derived from canonical_events; refresh via story_materialization pipeline';
COMMENT ON TABLE contradiction_records IS 'DERIVED: contradiction snapshots computed from perspectives and articles; refresh via analytics pipeline';
COMMENT ON TABLE claim_resolution_records IS 'DERIVED: claim resolution snapshots; refresh via claim-resolution pipeline';
COMMENT ON TABLE event_observation_archive IS 'DERIVED: observation archive derived from ingestion evidence; refresh via ingestion/observation pipeline';

COMMIT;
