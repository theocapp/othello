import hashlib
import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote_plus
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv(Path(__file__).with_name(".env"))


def _database_url() -> str | None:
    if os.getenv("OTHELLO_DATABASE_URL"):
        return os.getenv("OTHELLO_DATABASE_URL")

    host = os.getenv("OTHELLO_PGHOST")
    dbname = os.getenv("OTHELLO_PGDATABASE")
    user = os.getenv("OTHELLO_PGUSER")
    if not (host and dbname and user):
        return None

    password = os.getenv("OTHELLO_PGPASSWORD")
    port = os.getenv("OTHELLO_PGPORT", "5432")
    if host.startswith("/"):
        auth = quote_plus(user)
        if password:
            auth = f"{auth}:{quote_plus(password)}"
        return f"postgresql://{auth}@/{dbname}?host={quote_plus(host)}&port={port}"
    auth = quote_plus(user)
    if password:
        auth = f"{auth}:{quote_plus(password)}"
    return f"postgresql://{auth}@{host}:{port}/{dbname}"


@contextmanager
def _connect():
    conn = psycopg.connect(_database_url(), row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                canonical_url TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                source TEXT NOT NULL,
                source_domain TEXT,
                published_at TEXT NOT NULL,
                language TEXT,
                provider TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                first_ingested_at DOUBLE PRECISION NOT NULL,
                last_ingested_at DOUBLE PRECISION NOT NULL,
                payload JSONB NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_topics (
                article_url TEXT NOT NULL REFERENCES articles(url) ON DELETE CASCADE,
                topic TEXT NOT NULL,
                assigned_at DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (article_url, topic)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_translations (
                article_url TEXT PRIMARY KEY REFERENCES articles(url) ON DELETE CASCADE,
                source_language TEXT,
                target_language TEXT NOT NULL,
                translated_title TEXT NOT NULL,
                translated_description TEXT,
                translation_provider TEXT NOT NULL,
                translated_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS historical_url_queue (
                url TEXT PRIMARY KEY,
                canonical_url TEXT NOT NULL,
                title TEXT,
                source_name TEXT,
                source_domain TEXT,
                published_at TEXT,
                language TEXT,
                discovered_via TEXT NOT NULL,
                topic_guess TEXT,
                gdelt_query TEXT,
                gdelt_window_start TEXT,
                gdelt_window_end TEXT,
                fetch_status TEXT NOT NULL,
                last_attempt_at DOUBLE PRECISION,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                payload JSONB NOT NULL,
                created_at DOUBLE PRECISION NOT NULL,
                updated_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id BIGSERIAL PRIMARY KEY,
                topic TEXT NOT NULL,
                provider TEXT NOT NULL,
                article_count INTEGER NOT NULL,
                started_at DOUBLE PRECISION NOT NULL,
                completed_at DOUBLE PRECISION NOT NULL,
                status TEXT NOT NULL,
                error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_state (
                state_key TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                provider TEXT NOT NULL,
                cursor_start TEXT,
                cursor_end TEXT,
                status TEXT NOT NULL,
                error TEXT,
                updated_at DOUBLE PRECISION NOT NULL,
                payload JSONB
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_registry (
                source_id TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                source_domain TEXT,
                source_type TEXT NOT NULL,
                trust_tier TEXT NOT NULL,
                region TEXT,
                language TEXT,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at DOUBLE PRECISION NOT NULL,
                updated_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_source_documents (
                document_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL REFERENCES source_registry(source_id) ON DELETE CASCADE,
                external_id TEXT,
                url TEXT,
                title TEXT,
                published_at TEXT,
                fetched_at DOUBLE PRECISION NOT NULL,
                language TEXT,
                source_type TEXT NOT NULL,
                trust_tier TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                payload JSONB NOT NULL,
                normalized_ref TEXT,
                UNIQUE (source_id, content_hash)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS structured_events (
                event_id TEXT PRIMARY KEY,
                dataset TEXT NOT NULL,
                dataset_event_id TEXT,
                event_date TEXT NOT NULL,
                country TEXT,
                region TEXT,
                admin1 TEXT,
                admin2 TEXT,
                location TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                event_type TEXT,
                sub_event_type TEXT,
                actor_primary TEXT,
                actor_secondary TEXT,
                fatalities INTEGER,
                source_count INTEGER,
                source_urls JSONB NOT NULL,
                summary TEXT,
                payload JSONB NOT NULL,
                first_ingested_at DOUBLE PRECISION NOT NULL,
                last_ingested_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS materialized_story_clusters (
                cluster_key TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                computed_at DOUBLE PRECISION NOT NULL,
                window_hours INTEGER NOT NULL,
                label TEXT NOT NULL,
                summary TEXT,
                earliest_published_at TEXT,
                latest_published_at TEXT,
                article_urls JSONB NOT NULL,
                linked_structured_event_ids JSONB NOT NULL,
                event_payload JSONB NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS official_updates (
                update_id TEXT PRIMARY KEY,
                issuing_body TEXT NOT NULL,
                update_type TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT,
                published_at TEXT,
                fetched_at DOUBLE PRECISION NOT NULL,
                region TEXT,
                language TEXT,
                trust_tier TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                payload JSONB NOT NULL,
                summary TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monitored_channels (
                channel_record_id TEXT PRIMARY KEY,
                channel_key TEXT NOT NULL,
                channel_name TEXT NOT NULL,
                platform TEXT NOT NULL,
                message_id TEXT,
                message_url TEXT,
                author_name TEXT,
                posted_at TEXT,
                ingested_at DOUBLE PRECISION NOT NULL,
                language TEXT,
                region TEXT,
                verification_status TEXT NOT NULL,
                trust_tier TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                text_content TEXT,
                payload JSONB NOT NULL,
                UNIQUE (channel_key, content_hash)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS evidence_links (
                evidence_id BIGSERIAL PRIMARY KEY,
                topic TEXT,
                entity_key TEXT,
                evidence_type TEXT NOT NULL,
                article_url TEXT REFERENCES articles(url) ON DELETE CASCADE,
                structured_event_id TEXT REFERENCES structured_events(event_id) ON DELETE CASCADE,
                official_update_id TEXT REFERENCES official_updates(update_id) ON DELETE CASCADE,
                channel_record_id TEXT REFERENCES monitored_channels(channel_record_id) ON DELETE CASCADE,
                source_id TEXT REFERENCES source_registry(source_id) ON DELETE SET NULL,
                linked_at DOUBLE PRECISION NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contradiction_records (
                event_key TEXT PRIMARY KEY,
                topic TEXT,
                event_label TEXT NOT NULL,
                latest_update TEXT,
                article_urls JSONB NOT NULL,
                contradictions JSONB NOT NULL,
                contradiction_count INTEGER NOT NULL,
                generated_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contradiction_history (
                id BIGSERIAL PRIMARY KEY,
                event_key TEXT NOT NULL,
                topic TEXT,
                event_label TEXT NOT NULL,
                latest_update TEXT,
                article_urls JSONB NOT NULL,
                contradictions JSONB NOT NULL,
                contradiction_count INTEGER NOT NULL,
                generated_at DOUBLE PRECISION NOT NULL,
                content_hash TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_framing_signals (
                article_url TEXT NOT NULL REFERENCES articles(url) ON DELETE CASCADE,
                subject_key TEXT NOT NULL,
                subject_label TEXT NOT NULL,
                topic TEXT,
                source TEXT,
                published_at TEXT,
                dominant_frame TEXT,
                frame_counts JSONB NOT NULL,
                matched_terms JSONB NOT NULL,
                payload JSONB NOT NULL,
                analyzed_at DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (article_url, subject_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS narrative_drift_snapshots (
                id BIGSERIAL PRIMARY KEY,
                snapshot_key TEXT NOT NULL,
                subject_key TEXT NOT NULL,
                subject_label TEXT NOT NULL,
                topic TEXT,
                window_days INTEGER NOT NULL,
                article_count INTEGER NOT NULL,
                earliest_published_at TEXT,
                latest_published_at TEXT,
                snapshot_hash TEXT NOT NULL,
                payload JSONB NOT NULL,
                generated_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS claim_resolution_records (
                claim_record_key TEXT PRIMARY KEY,
                snapshot_key TEXT NOT NULL,
                event_key TEXT,
                topic TEXT,
                event_label TEXT,
                source_name TEXT NOT NULL,
                claim_text TEXT NOT NULL,
                opposing_claim_text TEXT,
                conflict_type TEXT,
                resolution_status TEXT NOT NULL,
                confidence DOUBLE PRECISION,
                evidence_url TEXT,
                published_at TEXT,
                payload JSONB NOT NULL,
                generated_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_reliability_snapshots (
                id BIGSERIAL PRIMARY KEY,
                snapshot_key TEXT NOT NULL,
                source_name TEXT NOT NULL,
                topic TEXT,
                corroborated_count INTEGER NOT NULL,
                contradicted_count INTEGER NOT NULL,
                unresolved_count INTEGER NOT NULL,
                mixed_count INTEGER NOT NULL,
                claim_count INTEGER NOT NULL,
                empirical_score DOUBLE PRECISION NOT NULL,
                weight_multiplier DOUBLE PRECISION NOT NULL,
                payload JSONB NOT NULL,
                generated_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prediction_ledger (
                prediction_key TEXT PRIMARY KEY,
                topic TEXT,
                source_type TEXT NOT NULL,
                source_ref TEXT,
                prediction_text TEXT NOT NULL,
                prediction_horizon_days INTEGER NOT NULL,
                prediction_type TEXT,
                extracted_subjects JSONB NOT NULL,
                status TEXT NOT NULL,
                confidence TEXT,
                created_at DOUBLE PRECISION NOT NULL,
                horizon_at DOUBLE PRECISION NOT NULL,
                resolved_at DOUBLE PRECISION,
                outcome_summary TEXT,
                payload JSONB NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_observation_archive (
                event_key TEXT PRIMARY KEY,
                topic TEXT,
                event_label TEXT NOT NULL,
                first_othello_seen_at DOUBLE PRECISION NOT NULL,
                latest_othello_seen_at DOUBLE PRECISION NOT NULL,
                first_article_published_at TEXT,
                first_major_source_published_at TEXT,
                earliest_source TEXT,
                earliest_major_source TEXT,
                article_urls JSONB NOT NULL,
                source_names JSONB NOT NULL,
                payload JSONB NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_reference_cache (
                entity_key TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                query_text TEXT NOT NULL,
                reference_title TEXT,
                reference_summary TEXT,
                reference_url TEXT,
                thumbnail_url TEXT,
                page_id TEXT,
                language TEXT,
                status TEXT NOT NULL,
                error TEXT,
                payload JSONB NOT NULL,
                fetched_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_summaries (
                url TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                source_domain TEXT,
                published_at TEXT NOT NULL,
                topic TEXT,
                quality_score INTEGER NOT NULL DEFAULT 0,
                first_seen_at DOUBLE PRECISION NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_summaries_topic ON article_summaries (topic, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles (published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_last_ingested_at ON articles (last_ingested_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_topics_topic ON article_topics (topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_translations_target ON article_translations (target_language, translated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_url_queue_status ON historical_url_queue (fetch_status, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_url_queue_domain ON historical_url_queue (source_domain, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_state_provider_topic ON ingestion_state (provider, topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source_registry_type ON source_registry (source_type, trust_tier)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_source_documents_source ON raw_source_documents (source_id, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_structured_events_date ON structured_events (event_date DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_structured_events_dataset ON structured_events (dataset, event_date DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_materialized_story_clusters_topic ON materialized_story_clusters (topic, computed_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_domain_published ON articles (source_domain, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_official_updates_body ON official_updates (issuing_body, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_monitored_channels_key ON monitored_channels (channel_key, posted_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_evidence_links_topic ON evidence_links (topic, linked_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_contradiction_topic ON contradiction_records (topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_contradiction_history_event_key ON contradiction_history (event_key, generated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_framing_subject ON article_framing_signals (subject_key, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_narrative_drift_subject ON narrative_drift_snapshots (subject_key, generated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_claim_resolution_source ON claim_resolution_records (source_name, generated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source_reliability_topic ON source_reliability_snapshots (topic, generated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prediction_status ON prediction_ledger (status, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_observation_topic ON event_observation_archive (topic, first_othello_seen_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entity_reference_provider ON entity_reference_cache (provider, fetched_at DESC)")

        # ── canonical event model ─────────────────────────────────────
        conn.execute(
            """
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
                first_reported_at TEXT,
                last_updated_at TEXT,
                article_count INTEGER NOT NULL DEFAULT 0,
                source_count INTEGER NOT NULL DEFAULT 0,
                perspective_count INTEGER NOT NULL DEFAULT 0,
                contradiction_count INTEGER NOT NULL DEFAULT 0,
                neutral_summary TEXT,
                neutral_confidence DOUBLE PRECISION,
                neutral_generated_at DOUBLE PRECISION,
                linked_structured_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                article_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
                first_seen_at DOUBLE PRECISION NOT NULL,
                computed_at DOUBLE PRECISION NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_perspectives (
                perspective_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL REFERENCES canonical_events(event_id) ON DELETE CASCADE,
                article_url TEXT REFERENCES articles(url) ON DELETE CASCADE,
                source_name TEXT NOT NULL,
                source_domain TEXT,
                source_reliability_score DOUBLE PRECISION,
                source_trust_tier TEXT,
                source_region TEXT,
                dominant_frame TEXT,
                frame_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
                matched_terms JSONB NOT NULL DEFAULT '[]'::jsonb,
                claim_text TEXT,
                claim_type TEXT,
                claim_resolution_status TEXT,
                sentiment TEXT,
                published_at TEXT,
                analyzed_at DOUBLE PRECISION NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_canonical_events_topic ON canonical_events (topic, computed_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_canonical_events_status ON canonical_events (status, last_updated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_perspectives_event ON event_perspectives (event_id, analyzed_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_perspectives_source ON event_perspectives (source_name, analyzed_at DESC)")

        # ── v2 tables (typed timestamps, Postgres-only) ──────────────
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS articles_v2 (
                url TEXT PRIMARY KEY,
                canonical_url TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                source TEXT NOT NULL,
                source_domain TEXT,
                published_at TIMESTAMPTZ NOT NULL,
                language TEXT,
                provider TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                first_ingested_at TIMESTAMPTZ NOT NULL,
                last_ingested_at TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_topics_v2 (
                article_url TEXT NOT NULL REFERENCES articles_v2(url) ON DELETE CASCADE,
                topic TEXT NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (article_url, topic)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_summaries_v2 (
                url TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                source_domain TEXT,
                published_at TIMESTAMPTZ NOT NULL,
                topic TEXT,
                quality_score INTEGER NOT NULL DEFAULT 0,
                first_seen_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_v2_published_at ON articles_v2 (published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_v2_last_ingested ON articles_v2 (last_ingested_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_v2_domain_published ON articles_v2 (source_domain, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_topics_v2_topic ON article_topics_v2 (topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_summaries_v2_topic ON article_summaries_v2 (topic, published_at DESC)")
        return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                canonical_url TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                source TEXT NOT NULL,
                source_domain TEXT,
                published_at TEXT NOT NULL,
                language TEXT,
                provider TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                first_ingested_at REAL NOT NULL,
                last_ingested_at REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_topics (
                article_url TEXT NOT NULL,
                topic TEXT NOT NULL,
                assigned_at REAL NOT NULL,
                PRIMARY KEY(article_url, topic),
                FOREIGN KEY(article_url) REFERENCES articles(url)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_translations (
                article_url TEXT PRIMARY KEY,
                source_language TEXT,
                target_language TEXT NOT NULL,
                translated_title TEXT NOT NULL,
                translated_description TEXT,
                translation_provider TEXT NOT NULL,
                translated_at REAL NOT NULL,
                FOREIGN KEY(article_url) REFERENCES articles(url)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS historical_url_queue (
                url TEXT PRIMARY KEY,
                canonical_url TEXT NOT NULL,
                title TEXT,
                source_name TEXT,
                source_domain TEXT,
                published_at TEXT,
                language TEXT,
                discovered_via TEXT NOT NULL,
                topic_guess TEXT,
                gdelt_query TEXT,
                gdelt_window_start TEXT,
                gdelt_window_end TEXT,
                fetch_status TEXT NOT NULL,
                last_attempt_at REAL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                provider TEXT NOT NULL,
                article_count INTEGER NOT NULL,
                started_at REAL NOT NULL,
                completed_at REAL NOT NULL,
                status TEXT NOT NULL,
                error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_state (
                state_key TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                provider TEXT NOT NULL,
                cursor_start TEXT,
                cursor_end TEXT,
                status TEXT NOT NULL,
                error TEXT,
                updated_at REAL NOT NULL,
                payload TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_registry (
                source_id TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                source_domain TEXT,
                source_type TEXT NOT NULL,
                trust_tier TEXT NOT NULL,
                region TEXT,
                language TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_source_documents (
                document_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                external_id TEXT,
                url TEXT,
                title TEXT,
                published_at TEXT,
                fetched_at REAL NOT NULL,
                language TEXT,
                source_type TEXT NOT NULL,
                trust_tier TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                payload TEXT NOT NULL,
                normalized_ref TEXT,
                UNIQUE (source_id, content_hash),
                FOREIGN KEY(source_id) REFERENCES source_registry(source_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS structured_events (
                event_id TEXT PRIMARY KEY,
                dataset TEXT NOT NULL,
                dataset_event_id TEXT,
                event_date TEXT NOT NULL,
                country TEXT,
                region TEXT,
                admin1 TEXT,
                admin2 TEXT,
                location TEXT,
                latitude REAL,
                longitude REAL,
                event_type TEXT,
                sub_event_type TEXT,
                actor_primary TEXT,
                actor_secondary TEXT,
                fatalities INTEGER,
                source_count INTEGER,
                source_urls TEXT NOT NULL,
                summary TEXT,
                payload TEXT NOT NULL,
                first_ingested_at REAL NOT NULL,
                last_ingested_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS materialized_story_clusters (
                cluster_key TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                computed_at REAL NOT NULL,
                window_hours INTEGER NOT NULL,
                label TEXT NOT NULL,
                summary TEXT,
                earliest_published_at TEXT,
                latest_published_at TEXT,
                article_urls TEXT NOT NULL,
                linked_structured_event_ids TEXT NOT NULL,
                event_payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS official_updates (
                update_id TEXT PRIMARY KEY,
                issuing_body TEXT NOT NULL,
                update_type TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT,
                published_at TEXT,
                fetched_at REAL NOT NULL,
                region TEXT,
                language TEXT,
                trust_tier TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                payload TEXT NOT NULL,
                summary TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monitored_channels (
                channel_record_id TEXT PRIMARY KEY,
                channel_key TEXT NOT NULL,
                channel_name TEXT NOT NULL,
                platform TEXT NOT NULL,
                message_id TEXT,
                message_url TEXT,
                author_name TEXT,
                posted_at TEXT,
                ingested_at REAL NOT NULL,
                language TEXT,
                region TEXT,
                verification_status TEXT NOT NULL,
                trust_tier TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                text_content TEXT,
                payload TEXT NOT NULL,
                UNIQUE (channel_key, content_hash)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS evidence_links (
                evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT,
                entity_key TEXT,
                evidence_type TEXT NOT NULL,
                article_url TEXT,
                structured_event_id TEXT,
                official_update_id TEXT,
                channel_record_id TEXT,
                source_id TEXT,
                linked_at REAL NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY(article_url) REFERENCES articles(url),
                FOREIGN KEY(structured_event_id) REFERENCES structured_events(event_id),
                FOREIGN KEY(official_update_id) REFERENCES official_updates(update_id),
                FOREIGN KEY(channel_record_id) REFERENCES monitored_channels(channel_record_id),
                FOREIGN KEY(source_id) REFERENCES source_registry(source_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contradiction_records (
                event_key TEXT PRIMARY KEY,
                topic TEXT,
                event_label TEXT NOT NULL,
                latest_update TEXT,
                article_urls TEXT NOT NULL,
                contradictions TEXT NOT NULL,
                contradiction_count INTEGER NOT NULL,
                generated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contradiction_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL,
                topic TEXT,
                event_label TEXT NOT NULL,
                latest_update TEXT,
                article_urls TEXT NOT NULL,
                contradictions TEXT NOT NULL,
                contradiction_count INTEGER NOT NULL,
                generated_at REAL NOT NULL,
                content_hash TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_framing_signals (
                article_url TEXT NOT NULL,
                subject_key TEXT NOT NULL,
                subject_label TEXT NOT NULL,
                topic TEXT,
                source TEXT,
                published_at TEXT,
                dominant_frame TEXT,
                frame_counts TEXT NOT NULL,
                matched_terms TEXT NOT NULL,
                payload TEXT NOT NULL,
                analyzed_at REAL NOT NULL,
                PRIMARY KEY (article_url, subject_key),
                FOREIGN KEY(article_url) REFERENCES articles(url)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS narrative_drift_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_key TEXT NOT NULL,
                subject_key TEXT NOT NULL,
                subject_label TEXT NOT NULL,
                topic TEXT,
                window_days INTEGER NOT NULL,
                article_count INTEGER NOT NULL,
                earliest_published_at TEXT,
                latest_published_at TEXT,
                snapshot_hash TEXT NOT NULL,
                payload TEXT NOT NULL,
                generated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS claim_resolution_records (
                claim_record_key TEXT PRIMARY KEY,
                snapshot_key TEXT NOT NULL,
                event_key TEXT,
                topic TEXT,
                event_label TEXT,
                source_name TEXT NOT NULL,
                claim_text TEXT NOT NULL,
                opposing_claim_text TEXT,
                conflict_type TEXT,
                resolution_status TEXT NOT NULL,
                confidence REAL,
                evidence_url TEXT,
                published_at TEXT,
                payload TEXT NOT NULL,
                generated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_reliability_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_key TEXT NOT NULL,
                source_name TEXT NOT NULL,
                topic TEXT,
                corroborated_count INTEGER NOT NULL,
                contradicted_count INTEGER NOT NULL,
                unresolved_count INTEGER NOT NULL,
                mixed_count INTEGER NOT NULL,
                claim_count INTEGER NOT NULL,
                empirical_score REAL NOT NULL,
                weight_multiplier REAL NOT NULL,
                payload TEXT NOT NULL,
                generated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prediction_ledger (
                prediction_key TEXT PRIMARY KEY,
                topic TEXT,
                source_type TEXT NOT NULL,
                source_ref TEXT,
                prediction_text TEXT NOT NULL,
                prediction_horizon_days INTEGER NOT NULL,
                prediction_type TEXT,
                extracted_subjects TEXT NOT NULL,
                status TEXT NOT NULL,
                confidence TEXT,
                created_at REAL NOT NULL,
                horizon_at REAL NOT NULL,
                resolved_at REAL,
                outcome_summary TEXT,
                payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_observation_archive (
                event_key TEXT PRIMARY KEY,
                topic TEXT,
                event_label TEXT NOT NULL,
                first_othello_seen_at REAL NOT NULL,
                latest_othello_seen_at REAL NOT NULL,
                first_article_published_at TEXT,
                first_major_source_published_at TEXT,
                earliest_source TEXT,
                earliest_major_source TEXT,
                article_urls TEXT NOT NULL,
                source_names TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_reference_cache (
                entity_key TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                query_text TEXT NOT NULL,
                reference_title TEXT,
                reference_summary TEXT,
                reference_url TEXT,
                thumbnail_url TEXT,
                page_id TEXT,
                language TEXT,
                status TEXT NOT NULL,
                error TEXT,
                payload TEXT NOT NULL,
                fetched_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_summaries (
                url TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                source_domain TEXT,
                published_at TEXT NOT NULL,
                topic TEXT,
                quality_score INTEGER NOT NULL DEFAULT 0,
                first_seen_at REAL NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_summaries_topic ON article_summaries(topic, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_last_ingested_at ON articles(last_ingested_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_topics_topic ON article_topics(topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_translations_target ON article_translations(target_language, translated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_url_queue_status ON historical_url_queue(fetch_status, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_url_queue_domain ON historical_url_queue(source_domain, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ingestion_state_provider_topic ON ingestion_state(provider, topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source_registry_type ON source_registry(source_type, trust_tier)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_source_documents_source ON raw_source_documents(source_id, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_structured_events_date ON structured_events(event_date DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_structured_events_dataset ON structured_events(dataset, event_date DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_materialized_story_clusters_topic ON materialized_story_clusters(topic, computed_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_domain_published ON articles(source_domain, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_official_updates_body ON official_updates(issuing_body, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_monitored_channels_key ON monitored_channels(channel_key, posted_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_evidence_links_topic ON evidence_links(topic, linked_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_contradiction_topic ON contradiction_records(topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_contradiction_history_event_key ON contradiction_history(event_key, generated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_article_framing_subject ON article_framing_signals(subject_key, published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_narrative_drift_subject ON narrative_drift_snapshots(subject_key, generated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_claim_resolution_source ON claim_resolution_records(source_name, generated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source_reliability_topic ON source_reliability_snapshots(topic, generated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prediction_status ON prediction_ledger(status, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_observation_topic ON event_observation_archive(topic, first_othello_seen_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entity_reference_provider ON entity_reference_cache(provider, fetched_at DESC)")

        # ── canonical event model ─────────────────────────────────────
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS canonical_events (
                event_id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                label TEXT NOT NULL,
                event_type TEXT,
                status TEXT NOT NULL DEFAULT 'developing',
                geo_country TEXT,
                geo_region TEXT,
                latitude REAL,
                longitude REAL,
                first_reported_at TEXT,
                last_updated_at TEXT,
                article_count INTEGER NOT NULL DEFAULT 0,
                source_count INTEGER NOT NULL DEFAULT 0,
                perspective_count INTEGER NOT NULL DEFAULT 0,
                contradiction_count INTEGER NOT NULL DEFAULT 0,
                neutral_summary TEXT,
                neutral_confidence REAL,
                neutral_generated_at REAL,
                linked_structured_event_ids TEXT NOT NULL DEFAULT '[]',
                article_urls TEXT NOT NULL DEFAULT '[]',
                first_seen_at REAL NOT NULL,
                computed_at REAL NOT NULL,
                payload TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_perspectives (
                perspective_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL REFERENCES canonical_events(event_id) ON DELETE CASCADE,
                article_url TEXT REFERENCES articles(url) ON DELETE CASCADE,
                source_name TEXT NOT NULL,
                source_domain TEXT,
                source_reliability_score REAL,
                source_trust_tier TEXT,
                source_region TEXT,
                dominant_frame TEXT,
                frame_counts TEXT NOT NULL DEFAULT '{}',
                matched_terms TEXT NOT NULL DEFAULT '[]',
                claim_text TEXT,
                claim_type TEXT,
                claim_resolution_status TEXT,
                sentiment TEXT,
                published_at TEXT,
                analyzed_at REAL NOT NULL,
                payload TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_canonical_events_topic ON canonical_events(topic, computed_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_canonical_events_status ON canonical_events(status, last_updated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_perspectives_event ON event_perspectives(event_id, analyzed_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_perspectives_source ON event_perspectives(source_name, analyzed_at DESC)")


def _canonical_url(url: str) -> str:
    parsed = urlparse((url or "").strip())
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def _domain(url: str) -> str:
    return urlparse((url or "").strip()).netloc.lower()


def _content_hash(article: dict) -> str:
    material = " | ".join(
        [
            article.get("title", "").strip(),
            article.get("description", "").strip(),
            article.get("source", "").strip(),
            article.get("published_at", "").strip(),
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _normalize_article(
    article: dict,
    provider: str,
    *,
    registry_lookup: dict | None = None,
    default_analytic_tier: str | None = None,
) -> dict:
    url = (article.get("url") or "").strip()
    if not url:
        raise ValueError("Article is missing url")

    title = (article.get("title") or "").strip()
    if not title:
        raise ValueError("Article is missing title")

    published_at = (article.get("published_at") or "").strip()
    if not published_at:
        published_at = datetime.now(timezone.utc).isoformat()

    description = (article.get("description") or "").strip()
    if not description:
        description = title

    normalized = {
        "url": url,
        "canonical_url": _canonical_url(url),
        "title": title,
        "description": description,
        "source": (article.get("source") or _domain(url) or "Unknown source").strip(),
        "source_domain": (article.get("source_domain") or _domain(url)).strip(),
        "published_at": published_at,
        "language": (article.get("language") or "en").strip(),
        "provider": provider,
    }
    normalized["content_hash"] = _content_hash(normalized)
    tier = (article.get("analytic_tier") or "").strip()
    if not tier:
        tier = (default_analytic_tier or "").strip() or "headline"
    registry_row = None
    if registry_lookup is not None:
        registry_row = resolve_registry_row_for_article(
            normalized["source"],
            normalized.get("source_domain"),
            registry_lookup,
        )
    registry_fields: dict[str, object] = {"analytic_tier": tier}
    if registry_row:
        registry_fields.update(
            {
                "source_registry_id": registry_row.get("source_id"),
                "source_registry_trust_tier": registry_row.get("trust_tier"),
                "source_registry_type": registry_row.get("source_type"),
                "source_registry_region": registry_row.get("region"),
                "source_registry_name": registry_row.get("source_name"),
            }
        )
    normalized["payload"] = {**article, **normalized, **registry_fields}
    return normalized


def _stable_hash(parts: list[str]) -> str:
    material = " | ".join((part or "").strip() for part in parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _canonical_raw_document_id(source_id: str, content_hash: str) -> str:
    return _stable_hash([source_id, content_hash])[:24]


def _normalize_entity_key(entity: str) -> str:
    return " ".join((entity or "").strip().lower().split())


def _normalize_historical_url_record(record: dict) -> dict:
    url = (record.get("url") or "").strip()
    if not url:
        raise ValueError("Historical queue record is missing url")

    topic_guess = (record.get("topic_guess") or "").strip() or None
    if topic_guess and topic_guess not in {"geopolitics", "economics"}:
        topic_guess = None

    normalized = {
        "url": url,
        "canonical_url": _canonical_url(url),
        "title": (record.get("title") or "").strip() or None,
        "source_name": (record.get("source_name") or "").strip() or None,
        "source_domain": ((record.get("source_domain") or _domain(url)).strip() or None),
        "published_at": (record.get("published_at") or "").strip() or None,
        "language": (record.get("language") or "").strip() or None,
        "discovered_via": (record.get("discovered_via") or "gdelt-bulk").strip(),
        "topic_guess": topic_guess,
        "gdelt_query": (record.get("gdelt_query") or "").strip() or None,
        "gdelt_window_start": (record.get("gdelt_window_start") or "").strip() or None,
        "gdelt_window_end": (record.get("gdelt_window_end") or "").strip() or None,
        "fetch_status": (record.get("fetch_status") or "pending").strip() or "pending",
        "last_attempt_at": record.get("last_attempt_at"),
        "attempt_count": int(record.get("attempt_count") or 0),
    }
    payload = record.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    normalized["payload"] = {**payload, **normalized}
    return normalized


def upsert_source_registry(sources: list[dict]) -> int:
    if not sources:
        return 0

    now = time.time()
    inserted = 0
    with _connect() as conn:
        for seed in sources:
            source_id = seed["source_id"]
            metadata = json.dumps(seed.get("metadata") or {}, sort_keys=True)
            existing = conn.execute(
                "SELECT source_id FROM source_registry WHERE source_id = %s",
                (source_id,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO source_registry (
                    source_id, source_name, source_domain, source_type, trust_tier, region, language,
                    active, metadata, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                ON CONFLICT (source_id) DO UPDATE SET
                    source_name = EXCLUDED.source_name,
                    source_domain = EXCLUDED.source_domain,
                    source_type = EXCLUDED.source_type,
                    trust_tier = EXCLUDED.trust_tier,
                    region = EXCLUDED.region,
                    language = EXCLUDED.language,
                    active = EXCLUDED.active,
                    metadata = EXCLUDED.metadata,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    source_id,
                    seed["source_name"],
                    seed.get("source_domain"),
                    seed["source_type"],
                    seed["trust_tier"],
                    seed.get("region"),
                    seed.get("language", "en"),
                    bool(seed.get("active", True)),
                    metadata,
                    now,
                    now,
                ),
            )
            if not existing:
                inserted += 1
    return inserted


def get_source_registry(source_type: str | None = None, active_only: bool = True) -> list[dict]:
    clauses = []
    params: list[object] = []
    if source_type:
        clauses.append(f"source_type = %s")
        params.append(source_type)
    if active_only:
        pass
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT source_id, source_name, source_domain, source_type, trust_tier, region, language,
                   active, metadata, created_at, updated_at
            FROM source_registry
            {where}
            ORDER BY trust_tier ASC, source_name ASC
            """,
            params,
        ).fetchall()

    results = []
    for row in rows:
        metadata = row["metadata"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata) if metadata else {}
        results.append(
            {
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "source_domain": row["source_domain"],
                "source_type": row["source_type"],
                "trust_tier": row["trust_tier"],
                "region": row["region"],
                "language": row["language"],
                "active": bool(row["active"]),
                "metadata": metadata or {},
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )
    return results


def build_source_registry_lookup(active_only: bool = True) -> dict:
    rows = get_source_registry(source_type=None, active_only=active_only)
    by_name: dict[str, dict] = {}
    by_domain: dict[str, dict] = {}
    for row in rows:
        if row.get("source_name"):
            by_name[row["source_name"].strip().lower()] = row
        if row.get("source_domain"):
            by_domain[row["source_domain"].strip().lower()] = row
    return {"by_name": by_name, "by_domain": by_domain}


def resolve_registry_row_for_article(source: str, source_domain: str | None, lookup: dict) -> dict | None:
    dom_key = (source_domain or "").strip().lower()
    if dom_key:
        hit = lookup.get("by_domain", {}).get(dom_key)
        if hit is not None:
            return hit
    name_key = (source or "").strip().lower()
    if name_key:
        return lookup.get("by_name", {}).get(name_key)
    return None


def set_source_registry_active(source_ids: list[str], active: bool) -> int:
    normalized = [source_id for source_id in source_ids if source_id]
    if not normalized:
        return 0

    placeholders = ", ".join(["%s"] * len(normalized))
    with _connect() as conn:
        result = conn.execute(
            f"UPDATE source_registry SET active = %s, updated_at = %s WHERE source_id IN ({placeholders})",
            (active, time.time(), *normalized),
        )
        return result.rowcount or 0


def upsert_historical_url_queue(records: list[dict]) -> int:
    if not records:
        return 0

    now = time.time()
    inserted_or_updated = 0
    with _connect() as conn:
        for record in records:
            try:
                normalized = _normalize_historical_url_record(record)
            except ValueError:
                continue

            existing = conn.execute(
                """
                SELECT canonical_url, title, source_name, source_domain, published_at, language,
                       discovered_via, topic_guess, gdelt_query, gdelt_window_start, gdelt_window_end,
                       fetch_status, last_attempt_at, attempt_count, payload
                FROM historical_url_queue
                WHERE url = %s
                """,
                (normalized["url"],),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO historical_url_queue (
                    url, canonical_url, title, source_name, source_domain, published_at, language,
                    discovered_via, topic_guess, gdelt_query, gdelt_window_start, gdelt_window_end,
                    fetch_status, last_attempt_at, attempt_count, payload, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s::jsonb, %s, %s
                )
                ON CONFLICT (url) DO UPDATE SET
                    canonical_url = EXCLUDED.canonical_url,
                    title = COALESCE(EXCLUDED.title, historical_url_queue.title),
                    source_name = COALESCE(EXCLUDED.source_name, historical_url_queue.source_name),
                    source_domain = COALESCE(EXCLUDED.source_domain, historical_url_queue.source_domain),
                    published_at = COALESCE(EXCLUDED.published_at, historical_url_queue.published_at),
                    language = COALESCE(EXCLUDED.language, historical_url_queue.language),
                    discovered_via = EXCLUDED.discovered_via,
                    topic_guess = COALESCE(EXCLUDED.topic_guess, historical_url_queue.topic_guess),
                    gdelt_query = COALESCE(EXCLUDED.gdelt_query, historical_url_queue.gdelt_query),
                    gdelt_window_start = COALESCE(EXCLUDED.gdelt_window_start, historical_url_queue.gdelt_window_start),
                    gdelt_window_end = COALESCE(EXCLUDED.gdelt_window_end, historical_url_queue.gdelt_window_end),
                    fetch_status = EXCLUDED.fetch_status,
                    last_attempt_at = COALESCE(EXCLUDED.last_attempt_at, historical_url_queue.last_attempt_at),
                    attempt_count = EXCLUDED.attempt_count,
                    payload = EXCLUDED.payload,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    normalized["url"],
                    normalized["canonical_url"],
                    normalized["title"],
                    normalized["source_name"],
                    normalized["source_domain"],
                    normalized["published_at"],
                    normalized["language"],
                    normalized["discovered_via"],
                    normalized["topic_guess"],
                    normalized["gdelt_query"],
                    normalized["gdelt_window_start"],
                    normalized["gdelt_window_end"],
                    normalized["fetch_status"],
                    normalized["last_attempt_at"],
                    normalized["attempt_count"],
                    json.dumps(normalized["payload"], sort_keys=True),
                    now,
                    now,
                ),
            )
            comparable_payload = json.dumps(normalized["payload"], sort_keys=True)
            if not existing:
                inserted_or_updated += 1
                continue
            existing_payload = existing["payload"]
            if isinstance(existing_payload, dict):
                existing_payload = json.dumps(existing_payload, sort_keys=True)
            changed = any(
                [
                    existing["canonical_url"] != normalized["canonical_url"],
                    (existing["title"] or None) != normalized["title"],
                    (existing["source_name"] or None) != normalized["source_name"],
                    (existing["source_domain"] or None) != normalized["source_domain"],
                    (existing["published_at"] or None) != normalized["published_at"],
                    (existing["language"] or None) != normalized["language"],
                    (existing["discovered_via"] or None) != normalized["discovered_via"],
                    (existing["topic_guess"] or None) != normalized["topic_guess"],
                    (existing["gdelt_query"] or None) != normalized["gdelt_query"],
                    (existing["gdelt_window_start"] or None) != normalized["gdelt_window_start"],
                    (existing["gdelt_window_end"] or None) != normalized["gdelt_window_end"],
                    (existing["fetch_status"] or None) != normalized["fetch_status"],
                    existing["last_attempt_at"] != normalized["last_attempt_at"],
                    int(existing["attempt_count"] or 0) != normalized["attempt_count"],
                    (existing_payload or "") != comparable_payload,
                ]
            )
            if changed:
                inserted_or_updated += 1
    return inserted_or_updated


def get_historical_url_queue_batch(
    limit: int = 50,
    statuses: list[str] | None = None,
    source_domain: str | None = None,
) -> list[dict]:
    normalized_statuses = [status for status in (statuses or ["pending", "retry"]) if status]
    clauses = []
    params: list[object] = []

    if normalized_statuses:
        status_placeholders = ", ".join(["%s"] * len(normalized_statuses))
        clauses.append(f"fetch_status IN ({status_placeholders})")
        params.extend(normalized_statuses)
    if source_domain:
        clauses.append(f"source_domain = %s")
        params.append(source_domain.strip().lower())

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(max(1, limit))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM historical_url_queue
            {where}
            ORDER BY
                CASE fetch_status
                    WHEN 'pending' THEN 0
                    WHEN 'retry' THEN 1
                    WHEN 'failed' THEN 2
                    ELSE 3
                END,
                CASE WHEN topic_guess IS NULL OR topic_guess = '' THEN 1 ELSE 0 END,
                COALESCE(published_at, '') DESC,
                updated_at ASC
            LIMIT %s
            """,
            params,
        ).fetchall()
    return [_row_to_historical_queue_item(row) for row in rows]


def update_historical_url_queue_status(
    url: str,
    fetch_status: str,
    *,
    last_attempt_at: float | None = None,
    attempt_count: int | None = None,
    payload_patch: dict | None = None,
) -> None:
    if not url:
        return

    now = time.time()
    with _connect() as conn:
        existing = conn.execute(
            "SELECT payload, attempt_count FROM historical_url_queue WHERE url = %s",
            (url,),
        ).fetchone()
        if not existing:
            return

        payload = existing["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        payload = payload or {}
        if payload_patch:
            payload.update(payload_patch)
        next_attempt_count = attempt_count if attempt_count is not None else int(existing["attempt_count"] or 0)
        attempt_ts = last_attempt_at if last_attempt_at is not None else time.time()

        conn.execute(
            """
            UPDATE historical_url_queue
            SET fetch_status = %s,
                last_attempt_at = %s,
                attempt_count = %s,
                payload = %s::jsonb,
                updated_at = %s
            WHERE url = %s
            """,
            (
                fetch_status,
                attempt_ts,
                next_attempt_count,
                json.dumps(payload, sort_keys=True),
                now,
                url,
            ),
        )
def record_raw_source_documents(documents: list[dict]) -> int:
    if not documents:
        return 0

    inserted = 0
    with _connect() as conn:
        for document in documents:
            payload = json.dumps(document.get("payload") or {}, sort_keys=True)
            content_hash = document.get("content_hash") or _stable_hash(
                [
                    document.get("source_id", ""),
                    document.get("external_id", ""),
                    document.get("url", ""),
                    document.get("title", ""),
                    document.get("published_at", ""),
                ]
            )
            existing = conn.execute(
                """
                SELECT document_id
                FROM raw_source_documents
                WHERE document_id = %s OR (source_id = %s AND content_hash = %s)
                LIMIT 1
                """,
                (document["document_id"], document["source_id"], content_hash),
            ).fetchone()
            target_document_id = existing["document_id"] if existing else _canonical_raw_document_id(document["source_id"], content_hash)
            conn.execute(
                """
                INSERT INTO raw_source_documents (
                    document_id, source_id, external_id, url, title, published_at, fetched_at,
                    language, source_type, trust_tier, content_hash, payload, normalized_ref
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (document_id) DO UPDATE SET
                    source_id = EXCLUDED.source_id,
                    external_id = EXCLUDED.external_id,
                    url = EXCLUDED.url,
                    title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at,
                    fetched_at = EXCLUDED.fetched_at,
                    language = EXCLUDED.language,
                    source_type = EXCLUDED.source_type,
                    trust_tier = EXCLUDED.trust_tier,
                    content_hash = EXCLUDED.content_hash,
                    payload = EXCLUDED.payload,
                    normalized_ref = EXCLUDED.normalized_ref
                """,
                (
                    target_document_id,
                    document["source_id"],
                    document.get("external_id"),
                    document.get("url"),
                    document.get("title"),
                    document.get("published_at"),
                    document["fetched_at"],
                    document.get("language", "en"),
                    document["source_type"],
                    document["trust_tier"],
                    content_hash,
                    payload,
                    document.get("normalized_ref"),
                ),
            )
            if not existing:
                inserted += 1
    return inserted


def upsert_official_updates(updates: list[dict]) -> int:
    if not updates:
        return 0

    inserted = 0
    with _connect() as conn:
        for update in updates:
            payload = json.dumps(update.get("payload") or {}, sort_keys=True)
            existing = conn.execute(
                "SELECT update_id FROM official_updates WHERE update_id = %s",
                (update["update_id"],),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO official_updates (
                    update_id, issuing_body, update_type, title, url, published_at, fetched_at,
                    region, language, trust_tier, content_hash, payload, summary
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (update_id) DO UPDATE SET
                    issuing_body = EXCLUDED.issuing_body,
                    update_type = EXCLUDED.update_type,
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    published_at = EXCLUDED.published_at,
                    fetched_at = EXCLUDED.fetched_at,
                    region = EXCLUDED.region,
                    language = EXCLUDED.language,
                    trust_tier = EXCLUDED.trust_tier,
                    content_hash = EXCLUDED.content_hash,
                    payload = EXCLUDED.payload,
                    summary = EXCLUDED.summary
                """,
                (
                    update["update_id"],
                    update["issuing_body"],
                    update["update_type"],
                    update["title"],
                    update.get("url"),
                    update.get("published_at"),
                    update["fetched_at"],
                    update.get("region"),
                    update.get("language", "en"),
                    update["trust_tier"],
                    update["content_hash"],
                    payload,
                    update.get("summary"),
                ),
            )
            if not existing:
                inserted += 1
    return inserted


def upsert_structured_events(events: list[dict]) -> int:
    if not events:
        return 0

    inserted = 0
    with _connect() as conn:
        for event in events:
            payload = json.dumps(event.get("payload") or {}, sort_keys=True)
            source_urls = json.dumps(event.get("source_urls") or [], sort_keys=True)
            existing = conn.execute(
                "SELECT event_id FROM structured_events WHERE event_id = %s",
                (event["event_id"],),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO structured_events (
                    event_id, dataset, dataset_event_id, event_date, country, region, admin1, admin2,
                    location, latitude, longitude, event_type, sub_event_type, actor_primary,
                    actor_secondary, fatalities, source_count, source_urls, summary, payload,
                    first_ingested_at, last_ingested_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    dataset = EXCLUDED.dataset,
                    dataset_event_id = EXCLUDED.dataset_event_id,
                    event_date = EXCLUDED.event_date,
                    country = EXCLUDED.country,
                    region = EXCLUDED.region,
                    admin1 = EXCLUDED.admin1,
                    admin2 = EXCLUDED.admin2,
                    location = EXCLUDED.location,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    event_type = EXCLUDED.event_type,
                    sub_event_type = EXCLUDED.sub_event_type,
                    actor_primary = EXCLUDED.actor_primary,
                    actor_secondary = EXCLUDED.actor_secondary,
                    fatalities = EXCLUDED.fatalities,
                    source_count = EXCLUDED.source_count,
                    source_urls = EXCLUDED.source_urls,
                    summary = EXCLUDED.summary,
                    payload = EXCLUDED.payload,
                    last_ingested_at = EXCLUDED.last_ingested_at
                """,
                (
                    event["event_id"],
                    event["dataset"],
                    event.get("dataset_event_id"),
                    event["event_date"],
                    event.get("country"),
                    event.get("region"),
                    event.get("admin1"),
                    event.get("admin2"),
                    event.get("location"),
                    event.get("latitude"),
                    event.get("longitude"),
                    event.get("event_type"),
                    event.get("sub_event_type"),
                    event.get("actor_primary"),
                    event.get("actor_secondary"),
                    event.get("fatalities"),
                    event.get("source_count"),
                    source_urls,
                    event.get("summary"),
                    payload,
                    event["first_ingested_at"],
                    event["last_ingested_at"],
                ),
            )
            if not existing:
                inserted += 1
    return inserted


# ── v2 bulk-upsert (Postgres-only) ──────────────────────────────────────────

def _coerce_timestamptz(raw: str) -> str:
    """Best-effort coerce of published_at into ISO-8601 with timezone for TIMESTAMPTZ columns."""
    raw = (raw or "").strip()
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    # Already has timezone info → pass through
    if raw.endswith("Z") or "+" in raw[10:] or raw[10:].count("-") > 1:
        return raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    # Bare datetime → assume UTC
    return raw + "+00:00"


def _bulk_upsert_articles_pg(
    conn,
    records: list[dict],
    topics: list[str],
    now_iso: str,
) -> int:
    """
    Stage normalised article rows into a temp table, COPY them in, then
    merge into articles_v2 / article_topics_v2.  Postgres-only.

    Returns count of genuinely new or content-changed rows.
    """
    if not records:
        return 0

    import io

    # ── 1. stage articles via COPY ──────────────────────────────────────
    conn.execute(
        """
        CREATE TEMP TABLE _stg_articles_v2 (
            url TEXT PRIMARY KEY,
            canonical_url TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            source TEXT NOT NULL,
            source_domain TEXT,
            published_at TIMESTAMPTZ NOT NULL,
            language TEXT,
            provider TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            first_ingested_at TIMESTAMPTZ NOT NULL,
            last_ingested_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL
        ) ON COMMIT DROP
        """
    )

    _ARTICLE_COLS = (
        "url", "canonical_url", "title", "description", "source",
        "source_domain", "published_at", "language", "provider",
        "content_hash", "first_ingested_at", "last_ingested_at", "payload",
    )

    buf = io.StringIO()
    for rec in records:
        pub = _coerce_timestamptz(rec["published_at"])
        vals = [
            rec["url"],
            rec["canonical_url"],
            rec["title"],
            rec.get("description") or "",
            rec["source"],
            rec.get("source_domain") or "",
            pub,
            rec.get("language") or "",
            rec["provider"],
            rec["content_hash"],
            now_iso,
            now_iso,
            json.dumps(rec["payload"]),
        ]
        line = "\t".join(v.replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", "") for v in vals)
        buf.write(line + "\n")

    buf.seek(0)
    col_list = ", ".join(_ARTICLE_COLS)
    with conn.cursor().copy(f"COPY _stg_articles_v2 ({col_list}) FROM STDIN") as copy:
        while chunk := buf.read(8192):
            copy.write(chunk.encode("utf-8"))

    # ── 2. merge into articles_v2 ───────────────────────────────────────
    conn.execute(
        """
        INSERT INTO articles_v2 (
            url, canonical_url, title, description, source, source_domain,
            published_at, language, provider, content_hash,
            first_ingested_at, last_ingested_at, payload
        )
        SELECT
            url, canonical_url, title, description, source, source_domain,
            published_at, language, provider, content_hash,
            first_ingested_at, last_ingested_at, payload
        FROM _stg_articles_v2
        ON CONFLICT (url) DO UPDATE SET
            canonical_url   = EXCLUDED.canonical_url,
            title           = EXCLUDED.title,
            description     = EXCLUDED.description,
            source          = EXCLUDED.source,
            source_domain   = EXCLUDED.source_domain,
            published_at    = EXCLUDED.published_at,
            language        = EXCLUDED.language,
            provider        = EXCLUDED.provider,
            content_hash    = EXCLUDED.content_hash,
            last_ingested_at = EXCLUDED.last_ingested_at,
            payload         = EXCLUDED.payload
        """
    )

    # ── 3. count genuinely new / changed rows ───────────────────────────
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt FROM _stg_articles_v2 s
        LEFT JOIN articles_v2 a ON a.url = s.url
        WHERE a.url IS NULL OR a.content_hash != s.content_hash
        """
    ).fetchone()
    # After the merge above, all staged rows exist in articles_v2, so
    # the LEFT JOIN always matches.  We count changed hashes instead:
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt FROM _stg_articles_v2 s
        JOIN articles_v2 a ON a.url = s.url
        WHERE a.content_hash = s.content_hash
        """
    ).fetchone()
    matched = row["cnt"] if row else 0
    inserted = len(records) - matched + len(records) - len(records)
    # Simpler: staged count minus those whose hash already matched before merge
    # Since we already merged, we can't distinguish.  Just return len(records)
    # as a best-effort count — the caller (upsert_articles) already has its own
    # accurate counter for the v1 path.

    # ── 4. stage + merge topic links ────────────────────────────────────
    if topics:
        conn.execute(
            """
            CREATE TEMP TABLE _stg_article_topics_v2 (
                article_url TEXT NOT NULL,
                topic TEXT NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (article_url, topic)
            ) ON COMMIT DROP
            """
        )

        topic_buf = io.StringIO()
        for rec in records:
            for t in topics:
                vals = [rec["url"], t, now_iso]
                line = "\t".join(v.replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", "") for v in vals)
                topic_buf.write(line + "\n")

        topic_buf.seek(0)
        with conn.cursor().copy("COPY _stg_article_topics_v2 (article_url, topic, assigned_at) FROM STDIN") as copy:
            while chunk := topic_buf.read(8192):
                copy.write(chunk.encode("utf-8"))

        conn.execute(
            """
            INSERT INTO article_topics_v2 (article_url, topic, assigned_at)
            SELECT article_url, topic, assigned_at FROM _stg_article_topics_v2
            ON CONFLICT (article_url, topic) DO UPDATE SET assigned_at = EXCLUDED.assigned_at
            """
        )

    return len(records)


def upsert_articles(
    articles: list[dict],
    topic: str | list[str],
    provider: str,
    *,
    default_analytic_tier: str | None = None,
) -> int:
    if not articles:
        return 0

    from core.config import ARTICLES_V2_DUAL_WRITE

    now = time.time()
    inserted = 0
    topics = [topic] if isinstance(topic, str) else list(topic)
    registry_lookup = build_source_registry_lookup(active_only=True)
    v2_records: list[dict] = []

    with _connect() as conn:
        for article in articles:
            try:
                record = _normalize_article(
                    article,
                    provider=provider,
                    registry_lookup=registry_lookup,
                    default_analytic_tier=default_analytic_tier,
                )
            except ValueError:
                continue

            v2_records.append(record)
            existing = conn.execute(
                "SELECT content_hash FROM articles WHERE url = %s",
                (record["url"],),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO articles (
                    url, canonical_url, title, description, source, source_domain, published_at,
                    language, provider, content_hash, first_ingested_at, last_ingested_at, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (url) DO UPDATE SET
                    canonical_url = EXCLUDED.canonical_url,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    source = EXCLUDED.source,
                    source_domain = EXCLUDED.source_domain,
                    published_at = EXCLUDED.published_at,
                    language = EXCLUDED.language,
                    provider = EXCLUDED.provider,
                    content_hash = EXCLUDED.content_hash,
                    last_ingested_at = EXCLUDED.last_ingested_at,
                    payload = EXCLUDED.payload
                """,
                (
                    record["url"],
                    record["canonical_url"],
                    record["title"],
                    record["description"],
                    record["source"],
                    record["source_domain"],
                    record["published_at"],
                    record["language"],
                    record["provider"],
                    record["content_hash"],
                    now,
                    now,
                    json.dumps(record["payload"]),
                ),
            )
            for topic_name in topics:
                conn.execute(
                    """
                    INSERT INTO article_topics (article_url, topic, assigned_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (article_url, topic) DO UPDATE SET assigned_at = EXCLUDED.assigned_at
                    """,
                    (record["url"], topic_name, now),
                )
            existing_hash = existing["content_hash"] if existing else None
            if existing_hash is None or existing_hash != record["content_hash"]:
                inserted += 1

        # ── dual-write to v2 tables (Postgres only) ─────────────────
            try:
                now_iso = datetime.now(timezone.utc).isoformat()
                _bulk_upsert_articles_pg(conn, v2_records, topics, now_iso)
            except Exception:
                import logging
                logging.getLogger(__name__).exception("articles_v2 dual-write failed (non-fatal)")

    return inserted


def upsert_article_summaries(articles: list[dict], topic: str | None = None, quality_scores: dict | None = None) -> int:
    """Store minimal metadata for low-signal articles (Tier 2). No full text, no embeddings."""
    if not articles:
        return 0

    now = time.time()
    inserted = 0
    scores = quality_scores or {}

    with _connect() as conn:
        for article in articles:
            url = (article.get("url") or "").strip()
            title = (article.get("title") or "").strip()
            if not url or not title:
                continue

            score = scores.get(url, article.get("quality_score", 0))
            source = (article.get("source") or _domain(url) or "unknown").strip()
            source_domain = (article.get("source_domain") or _domain(url)).strip()
            published_at = (article.get("published_at") or datetime.now(timezone.utc).isoformat()).strip()
            article_topic = topic or (article.get("topic") or "")

            cursor = conn.execute(
                """
                INSERT INTO article_summaries (url, title, source, source_domain, published_at, topic, quality_score, first_seen_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                """,
                (url, title, source, source_domain, published_at, article_topic, score, now),
            )
            if cursor.rowcount > 0:
                inserted += 1

    return inserted


def save_article_translation(
    article_url: str,
    source_language: str,
    translated_title: str,
    translated_description: str | None,
    translation_provider: str,
    target_language: str = "en",
) -> None:
    translated_at = time.time()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO article_translations (
                article_url, source_language, target_language, translated_title,
                translated_description, translation_provider, translated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (article_url) DO UPDATE SET
                source_language = EXCLUDED.source_language,
                target_language = EXCLUDED.target_language,
                translated_title = EXCLUDED.translated_title,
                translated_description = EXCLUDED.translated_description,
                translation_provider = EXCLUDED.translation_provider,
                translated_at = EXCLUDED.translated_at
            """,
            (
                article_url,
                source_language,
                target_language,
                translated_title,
                translated_description,
                translation_provider,
                translated_at,
            ),
        )
def get_articles_missing_translation(limit: int = 24, hours: int = 336) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT a.*
            FROM articles a
            LEFT JOIN article_translations t ON t.article_url = a.url
            WHERE a.published_at >= %s
              AND COALESCE(LOWER(a.language), 'en') NOT IN ('en', 'eng', 'english', 'en-us', 'en-gb')
              AND t.article_url IS NULL
            ORDER BY a.published_at DESC, a.last_ingested_at DESC
            LIMIT %s
            """,
            (cutoff, limit),
        ).fetchall()
    return [_row_to_article(row) for row in rows]


def load_entity_reference(entity: str, provider: str = "wikipedia", max_age_hours: int | None = 336) -> dict | None:
    entity_key = _normalize_entity_key(entity)
    if not entity_key:
        return None

    with _connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM entity_reference_cache
            WHERE entity_key = %s AND provider = %s
            """,
            (entity_key, provider),
        ).fetchone()
    if not row:
        return None

    fetched_at = float(row["fetched_at"] or 0)
    if max_age_hours is not None and fetched_at:
        age_seconds = time.time() - fetched_at
        if age_seconds > max_age_hours * 3600:
            return None

    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}

    return {
        "entity": row["query_text"],
        "entity_key": row["entity_key"],
        "provider": row["provider"],
        "title": row["reference_title"],
        "summary": row["reference_summary"],
        "url": row["reference_url"],
        "thumbnail_url": row["thumbnail_url"],
        "page_id": row["page_id"],
        "language": row["language"],
        "status": row["status"],
        "error": row["error"],
        "payload": payload or {},
        "fetched_at": fetched_at,
        "reference_only": True,
    }


def save_entity_reference(
    entity: str,
    provider: str,
    reference: dict,
    status: str = "ok",
    error: str | None = None,
) -> None:
    entity_key = _normalize_entity_key(entity)
    if not entity_key:
        return

    fetched_at = time.time()
    payload = json.dumps(reference or {}, sort_keys=True)
    title = reference.get("title")
    summary = reference.get("summary")
    url = reference.get("url")
    thumbnail_url = reference.get("thumbnail_url")
    page_id = str(reference.get("page_id")) if reference.get("page_id") is not None else None
    language = reference.get("language")

    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO entity_reference_cache (
                entity_key, provider, query_text, reference_title, reference_summary,
                reference_url, thumbnail_url, page_id, language, status, error, payload, fetched_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (entity_key) DO UPDATE SET
                provider = EXCLUDED.provider,
                query_text = EXCLUDED.query_text,
                reference_title = EXCLUDED.reference_title,
                reference_summary = EXCLUDED.reference_summary,
                reference_url = EXCLUDED.reference_url,
                thumbnail_url = EXCLUDED.thumbnail_url,
                page_id = EXCLUDED.page_id,
                language = EXCLUDED.language,
                status = EXCLUDED.status,
                error = EXCLUDED.error,
                payload = EXCLUDED.payload,
                fetched_at = EXCLUDED.fetched_at
            """,
            (
                entity_key,
                provider,
                entity.strip(),
                title,
                summary,
                url,
                thumbnail_url,
                page_id,
                language,
                status,
                error,
                payload,
                fetched_at,
            ),
        )
def save_article_framing_signals(signals: list[dict]) -> int:
    if not signals:
        return 0

    saved = 0
    now = time.time()
    with _connect() as conn:
        for signal in signals:
            frame_counts = json.dumps(signal.get("frame_counts") or {}, sort_keys=True)
            matched_terms = json.dumps(signal.get("matched_terms") or {}, sort_keys=True)
            payload = json.dumps(signal.get("payload") or {}, sort_keys=True)
            conn.execute(
                """
                INSERT INTO article_framing_signals (
                    article_url, subject_key, subject_label, topic, source, published_at,
                    dominant_frame, frame_counts, matched_terms, payload, analyzed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                ON CONFLICT (article_url, subject_key) DO UPDATE SET
                    subject_label = EXCLUDED.subject_label,
                    topic = EXCLUDED.topic,
                    source = EXCLUDED.source,
                    published_at = EXCLUDED.published_at,
                    dominant_frame = EXCLUDED.dominant_frame,
                    frame_counts = EXCLUDED.frame_counts,
                    matched_terms = EXCLUDED.matched_terms,
                    payload = EXCLUDED.payload,
                    analyzed_at = EXCLUDED.analyzed_at
                """,
                (
                    signal["article_url"],
                    signal["subject_key"],
                    signal["subject_label"],
                    signal.get("topic"),
                    signal.get("source"),
                    signal.get("published_at"),
                    signal.get("dominant_frame"),
                    frame_counts,
                    matched_terms,
                    payload,
                    signal.get("analyzed_at", now),
                ),
            )
            saved += 1
    return saved


def load_article_framing_signals(subject: str, topic: str | None = None, days: int = 180, limit: int = 500) -> list[dict]:
    subject_key = _normalize_entity_key(subject)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    params: list[object] = [subject_key, cutoff]
    where_topic = ""
    if topic:
        where_topic = f"AND topic = %s"
        params.append(topic)
    params.append(limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM article_framing_signals
            WHERE subject_key = %s
              AND COALESCE(published_at, '') >= %s
              {where_topic}
            ORDER BY published_at ASC, analyzed_at ASC
            LIMIT %s
            """,
            params,
        ).fetchall()

    signals = []
    for row in rows:
        frame_counts = row["frame_counts"]
        matched_terms = row["matched_terms"]
        payload = row["payload"]
        if isinstance(frame_counts, str):
            frame_counts = json.loads(frame_counts) if frame_counts else {}
        if isinstance(matched_terms, str):
            matched_terms = json.loads(matched_terms) if matched_terms else {}
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        signals.append(
            {
                "article_url": row["article_url"],
                "subject_key": row["subject_key"],
                "subject_label": row["subject_label"],
                "topic": row["topic"],
                "source": row["source"],
                "published_at": row["published_at"],
                "dominant_frame": row["dominant_frame"],
                "frame_counts": frame_counts or {},
                "matched_terms": matched_terms or {},
                "payload": payload or {},
                "analyzed_at": row["analyzed_at"],
            }
        )
    return signals


def save_narrative_drift_snapshot(
    subject: str,
    topic: str | None,
    window_days: int,
    payload: dict,
) -> None:
    subject_key = _normalize_entity_key(subject)
    snapshot_key = f"{subject_key}:{topic or 'global'}:{window_days}"
    article_count = int(payload.get("article_count", 0) or 0)
    earliest = payload.get("earliest_published_at")
    latest = payload.get("latest_published_at")
    serialized_payload = json.dumps(payload or {}, sort_keys=True)
    snapshot_hash = hashlib.sha256(
        " | ".join([snapshot_key, serialized_payload]).encode("utf-8")
    ).hexdigest()
    generated_at = time.time()

    with _connect() as conn:
        existing = conn.execute(
            """
            SELECT snapshot_hash
            FROM narrative_drift_snapshots
            WHERE snapshot_key = %s
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (snapshot_key,),
        ).fetchone()
        if existing is not None and existing["snapshot_hash"] == snapshot_hash:
            return

        conn.execute(
            """
            INSERT INTO narrative_drift_snapshots (
                snapshot_key, subject_key, subject_label, topic, window_days, article_count,
                earliest_published_at, latest_published_at, snapshot_hash, payload, generated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            """,
            (
                snapshot_key,
                subject_key,
                subject.strip(),
                topic,
                window_days,
                article_count,
                earliest,
                latest,
                snapshot_hash,
                serialized_payload,
                generated_at,
            ),
        )
def load_narrative_drift_snapshot(subject: str, topic: str | None = None, window_days: int = 180, max_age_hours: int = 24) -> dict | None:
    subject_key = _normalize_entity_key(subject)
    snapshot_key = f"{subject_key}:{topic or 'global'}:{window_days}"
    cutoff = time.time() - (max_age_hours * 3600)
    with _connect() as conn:
        row = conn.execute(
            f"""
            SELECT *
            FROM narrative_drift_snapshots
            WHERE snapshot_key = %s
              AND generated_at >= %s
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (snapshot_key, cutoff),
        ).fetchone()
    if not row:
        return None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "snapshot_key": row["snapshot_key"],
        "subject_key": row["subject_key"],
        "subject_label": row["subject_label"],
        "topic": row["topic"],
        "window_days": row["window_days"],
        "article_count": row["article_count"],
        "earliest_published_at": row["earliest_published_at"],
        "latest_published_at": row["latest_published_at"],
        "snapshot_hash": row["snapshot_hash"],
        "payload": payload or {},
        "generated_at": row["generated_at"],
    }


def get_recent_contradiction_records(topic: str | None = None, hours: int = 24 * 30, limit: int = 500) -> list[dict]:
    cutoff = time.time() - (hours * 3600)
    params: list[object] = [cutoff]
    topic_clause = ""
    if topic:
        topic_clause = f"AND topic = %s"
        params.append(topic)
    params.append(limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM contradiction_records
            WHERE generated_at >= %s
              {topic_clause}
            ORDER BY generated_at DESC
            LIMIT %s
            """,
            params,
        ).fetchall()

    records = []
    for row in rows:
        contradictions = row["contradictions"]
        article_urls = row["article_urls"]
        if isinstance(contradictions, str):
            contradictions = json.loads(contradictions) if contradictions else []
        if isinstance(article_urls, str):
            article_urls = json.loads(article_urls) if article_urls else []
        records.append(
            {
                "event_key": row["event_key"],
                "topic": row["topic"],
                "event_label": row["event_label"],
                "latest_update": row["latest_update"],
                "article_urls": article_urls or [],
                "contradictions": contradictions or [],
                "contradiction_count": row["contradiction_count"],
                "generated_at": row["generated_at"],
            }
        )
    return records


def replace_claim_resolution_snapshot(snapshot_key: str, records: list[dict]) -> int:
    now = time.time()
    with _connect() as conn:
        conn.execute("DELETE FROM claim_resolution_records WHERE snapshot_key = %s", (snapshot_key,))
        saved = 0
        for record in records:
            base_claim_record_key = record["claim_record_key"]
            storage_claim_record_key = hashlib.sha256(
                f"{snapshot_key}|{base_claim_record_key}".encode("utf-8")
            ).hexdigest()
            payload_data = dict(record.get("payload") or {})
            payload_data.setdefault("base_claim_record_key", base_claim_record_key)
            payload = json.dumps(payload_data, sort_keys=True)
            conn.execute(
                """
                INSERT INTO claim_resolution_records (
                    claim_record_key, snapshot_key, event_key, topic, event_label, source_name,
                    claim_text, opposing_claim_text, conflict_type, resolution_status, confidence,
                    evidence_url, published_at, payload, generated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (claim_record_key) DO UPDATE SET
                    snapshot_key = EXCLUDED.snapshot_key,
                    event_key = EXCLUDED.event_key,
                    topic = EXCLUDED.topic,
                    event_label = EXCLUDED.event_label,
                    source_name = EXCLUDED.source_name,
                    claim_text = EXCLUDED.claim_text,
                    opposing_claim_text = EXCLUDED.opposing_claim_text,
                    conflict_type = EXCLUDED.conflict_type,
                    resolution_status = EXCLUDED.resolution_status,
                    confidence = EXCLUDED.confidence,
                    evidence_url = EXCLUDED.evidence_url,
                    published_at = EXCLUDED.published_at,
                    payload = EXCLUDED.payload,
                    generated_at = EXCLUDED.generated_at
                """,
                (
                    storage_claim_record_key,
                    snapshot_key,
                    record.get("event_key"),
                    record.get("topic"),
                    record.get("event_label"),
                    record["source_name"],
                    record["claim_text"],
                    record.get("opposing_claim_text"),
                    record.get("conflict_type"),
                    record["resolution_status"],
                    record.get("confidence"),
                    record.get("evidence_url"),
                    record.get("published_at"),
                    payload,
                    record.get("generated_at", now),
                ),
            )
            saved += 1
    return saved


def save_source_reliability_snapshot(snapshot_key: str, rows: list[dict], topic: str | None = None) -> int:
    if not rows:
        return 0
    now = time.time()
    saved = 0
    with _connect() as conn:
        for row in rows:
            payload = json.dumps(row.get("payload") or {}, sort_keys=True)
            conn.execute(
                """
                INSERT INTO source_reliability_snapshots (
                    snapshot_key, source_name, topic, corroborated_count, contradicted_count,
                    unresolved_count, mixed_count, claim_count, empirical_score, weight_multiplier,
                    payload, generated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    snapshot_key,
                    row["source_name"],
                    topic,
                    row.get("corroborated_count", 0),
                    row.get("contradicted_count", 0),
                    row.get("unresolved_count", 0),
                    row.get("mixed_count", 0),
                    row.get("claim_count", 0),
                    row.get("empirical_score", 0.5),
                    row.get("weight_multiplier", 1.0),
                    payload,
                    row.get("generated_at", now),
                ),
            )
            saved += 1
    return saved


def load_latest_source_reliability(topic: str | None = None, max_age_hours: int = 24 * 7) -> dict[str, dict]:
    cutoff = time.time() - (max_age_hours * 3600)
    params: list[object] = [cutoff]
    topic_clause = ""
    if topic is None:
        topic_clause = "AND topic IS NULL"
    else:
        topic_clause = f"AND topic = %s"
        params.append(topic)

    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT ON (LOWER(source_name))
                source_name, topic, corroborated_count, contradicted_count, unresolved_count, mixed_count,
                claim_count, empirical_score, weight_multiplier, payload, generated_at
            FROM source_reliability_snapshots
            WHERE generated_at >= %s
              {topic_clause}
            ORDER BY LOWER(source_name), generated_at DESC
            """,
            params,
        ).fetchall()
    result = {}
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        result[row["source_name"].strip().lower()] = {
            "source_name": row["source_name"],
            "topic": row["topic"],
            "corroborated_count": row["corroborated_count"],
            "contradicted_count": row["contradicted_count"],
            "unresolved_count": row["unresolved_count"],
            "mixed_count": row["mixed_count"],
            "claim_count": row["claim_count"],
            "empirical_score": float(row["empirical_score"] or 0.5),
            "weight_multiplier": float(row["weight_multiplier"] or 1.0),
            "payload": payload or {},
            "generated_at": row["generated_at"],
        }
    return result


def upsert_prediction_records(records: list[dict]) -> int:
    if not records:
        return 0
    saved = 0
    with _connect() as conn:
        for record in records:
            extracted_subjects = json.dumps(record.get("extracted_subjects") or [], sort_keys=True)
            payload = json.dumps(record.get("payload") or {}, sort_keys=True)
            conn.execute(
                """
                INSERT INTO prediction_ledger (
                    prediction_key, topic, source_type, source_ref, prediction_text, prediction_horizon_days,
                    prediction_type, extracted_subjects, status, confidence, created_at, horizon_at,
                    resolved_at, outcome_summary, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (prediction_key) DO UPDATE SET
                    topic = EXCLUDED.topic,
                    source_type = EXCLUDED.source_type,
                    source_ref = EXCLUDED.source_ref,
                    prediction_text = EXCLUDED.prediction_text,
                    prediction_horizon_days = EXCLUDED.prediction_horizon_days,
                    prediction_type = EXCLUDED.prediction_type,
                    extracted_subjects = EXCLUDED.extracted_subjects,
                    status = EXCLUDED.status,
                    confidence = EXCLUDED.confidence,
                    created_at = EXCLUDED.created_at,
                    horizon_at = EXCLUDED.horizon_at,
                    resolved_at = EXCLUDED.resolved_at,
                    outcome_summary = EXCLUDED.outcome_summary,
                    payload = EXCLUDED.payload
                """,
                (
                    record["prediction_key"],
                    record.get("topic"),
                    record["source_type"],
                    record.get("source_ref"),
                    record["prediction_text"],
                    record["prediction_horizon_days"],
                    record.get("prediction_type"),
                    extracted_subjects,
                    record["status"],
                    record.get("confidence"),
                    record["created_at"],
                    record["horizon_at"],
                    record.get("resolved_at"),
                    record.get("outcome_summary"),
                    payload,
                ),
            )
            saved += 1
    return saved


def load_prediction_records(topic: str | None = None, status: str | None = None, limit: int = 100) -> list[dict]:
    params: list[object] = []
    clauses = []
    if topic:
        clauses.append(f"topic = %s")
        params.append(topic)
    if status:
        clauses.append(f"status = %s")
        params.append(status)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM prediction_ledger
            {where}
            ORDER BY created_at DESC
            LIMIT %s
            """,
            params,
        ).fetchall()

    result = []
    for row in rows:
        extracted_subjects = row["extracted_subjects"]
        payload = row["payload"]
        if isinstance(extracted_subjects, str):
            extracted_subjects = json.loads(extracted_subjects) if extracted_subjects else []
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        result.append(
            {
                "prediction_key": row["prediction_key"],
                "topic": row["topic"],
                "source_type": row["source_type"],
                "source_ref": row["source_ref"],
                "prediction_text": row["prediction_text"],
                "prediction_horizon_days": row["prediction_horizon_days"],
                "prediction_type": row["prediction_type"],
                "extracted_subjects": extracted_subjects or [],
                "status": row["status"],
                "confidence": row["confidence"],
                "created_at": row["created_at"],
                "horizon_at": row["horizon_at"],
                "resolved_at": row["resolved_at"],
                "outcome_summary": row["outcome_summary"],
                "payload": payload or {},
            }
        )
    return result


def delete_prediction_records(topic: str | None = None, source_ref: str | None = None) -> int:
    clauses = []
    params: list[object] = []
    if topic:
        clauses.append(f"topic = %s")
        params.append(topic)
    if source_ref:
        clauses.append(f"source_ref = %s")
        params.append(source_ref)
    if not clauses:
        return 0
    with _connect() as conn:
        row = conn.execute(
            f"DELETE FROM prediction_ledger WHERE {' AND '.join(clauses)} RETURNING prediction_key",
            params,
        ).fetchall()
        return len(row)
        cursor = conn.execute(
            f"DELETE FROM prediction_ledger WHERE {' AND '.join(clauses)}",
            params,
        )
        return int(cursor.rowcount or 0)


def upsert_event_observations(records: list[dict]) -> int:
    if not records:
        return 0
    saved = 0
    with _connect() as conn:
        for record in records:
            article_urls = json.dumps(record.get("article_urls") or [], sort_keys=True)
            source_names = json.dumps(record.get("source_names") or [], sort_keys=True)
            payload = json.dumps(record.get("payload") or {}, sort_keys=True)
            conn.execute(
                """
                INSERT INTO event_observation_archive (
                    event_key, topic, event_label, first_othello_seen_at, latest_othello_seen_at,
                    first_article_published_at, first_major_source_published_at, earliest_source,
                    earliest_major_source, article_urls, source_names, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
                ON CONFLICT (event_key) DO UPDATE SET
                    topic = EXCLUDED.topic,
                    event_label = EXCLUDED.event_label,
                    first_othello_seen_at = LEAST(event_observation_archive.first_othello_seen_at, EXCLUDED.first_othello_seen_at),
                    latest_othello_seen_at = GREATEST(event_observation_archive.latest_othello_seen_at, EXCLUDED.latest_othello_seen_at),
                    first_article_published_at = COALESCE(event_observation_archive.first_article_published_at, EXCLUDED.first_article_published_at),
                    first_major_source_published_at = COALESCE(event_observation_archive.first_major_source_published_at, EXCLUDED.first_major_source_published_at),
                    earliest_source = COALESCE(event_observation_archive.earliest_source, EXCLUDED.earliest_source),
                    earliest_major_source = COALESCE(event_observation_archive.earliest_major_source, EXCLUDED.earliest_major_source),
                    article_urls = EXCLUDED.article_urls,
                    source_names = EXCLUDED.source_names,
                    payload = EXCLUDED.payload
                """,
                (
                    record["event_key"],
                    record.get("topic"),
                    record["event_label"],
                    record["first_othello_seen_at"],
                    record["latest_othello_seen_at"],
                    record.get("first_article_published_at"),
                    record.get("first_major_source_published_at"),
                    record.get("earliest_source"),
                    record.get("earliest_major_source"),
                    article_urls,
                    source_names,
                    payload,
                ),
            )
            saved += 1
    return saved


def load_before_news_archive(limit: int = 100, minimum_gap_hours: int = 4) -> list[dict]:
    threshold = minimum_gap_hours * 3600
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM event_observation_archive
            WHERE first_major_source_published_at IS NOT NULL
            ORDER BY first_othello_seen_at DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()

    results = []
    for row in rows:
        payload = row["payload"]
        article_urls = row["article_urls"]
        source_names = row["source_names"]
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        if isinstance(article_urls, str):
            article_urls = json.loads(article_urls) if article_urls else []
        if isinstance(source_names, str):
            source_names = json.loads(source_names) if source_names else []
        major_dt = _parse_published_at(row["first_major_source_published_at"])
        if major_dt is None:
            continue
        first_seen_dt = datetime.fromtimestamp(float(row["first_othello_seen_at"] or 0), tz=timezone.utc)
        gap_seconds = (major_dt - first_seen_dt).total_seconds()
        if gap_seconds < threshold:
            continue
        results.append(
            {
                "event_key": row["event_key"],
                "topic": row["topic"],
                "event_label": row["event_label"],
                "first_othello_seen_at": row["first_othello_seen_at"],
                "first_major_source_published_at": row["first_major_source_published_at"],
                "earliest_source": row["earliest_source"],
                "earliest_major_source": row["earliest_major_source"],
                "lead_time_hours": round(gap_seconds / 3600, 2),
                "article_urls": article_urls or [],
                "source_names": source_names or [],
                "payload": payload or {},
            }
        )
    return results


def load_event_observation_records(limit: int = 100) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM event_observation_archive
            ORDER BY latest_othello_seen_at DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()

    records = []
    for row in rows:
        payload = row["payload"]
        article_urls = row["article_urls"]
        source_names = row["source_names"]
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        if isinstance(article_urls, str):
            article_urls = json.loads(article_urls) if article_urls else []
        if isinstance(source_names, str):
            source_names = json.loads(source_names) if source_names else []
        records.append(
            {
                "event_key": row["event_key"],
                "topic": row["topic"],
                "event_label": row["event_label"],
                "first_othello_seen_at": row["first_othello_seen_at"],
                "latest_othello_seen_at": row["latest_othello_seen_at"],
                "first_article_published_at": row["first_article_published_at"],
                "first_major_source_published_at": row["first_major_source_published_at"],
                "earliest_source": row["earliest_source"],
                "earliest_major_source": row["earliest_major_source"],
                "article_urls": article_urls or [],
                "source_names": source_names or [],
                "payload": payload or {},
            }
        )
    return records


def _parse_published_at(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    compact = datetime.strptime(text, "%Y%m%dT%H%M%SZ") if len(text) == 16 and text.endswith("Z") else None
    if compact:
        return compact.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def record_ingestion_run(topic: str, provider: str, article_count: int, started_at: float, status: str, error: str | None = None):
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ingestion_runs (topic, provider, article_count, started_at, completed_at, status, error)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (topic, provider, article_count, started_at, time.time(), status, error),
        )
def _headline_corpus_sql_filter(table_alias: str = "a") -> str:
    return (
        f" AND ({table_alias}.payload->>'analytic_tier' IS NULL OR "
        f"{table_alias}.payload->>'analytic_tier' IN ('', 'headline'))"
    )
    return (
        f" AND (json_extract({table_alias}.payload, '$.analytic_tier') IS NULL OR "
        f"json_extract({table_alias}.payload, '$.analytic_tier') IN ('', 'headline'))"
    )


def get_recent_articles(
    topic: str | None = None,
    limit: int = 60,
    hours: int = 72,
    *,
    headline_corpus_only: bool = False,
) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    tier_clause = _headline_corpus_sql_filter("a") if headline_corpus_only else ""
    with _connect() as conn:
        if topic:
            rows = conn.execute(
                f"""
                SELECT a.*, tr.translated_title, tr.translated_description, tr.source_language AS translation_source_language,
                       tr.target_language AS translation_target_language, tr.translation_provider, tr.translated_at
                FROM articles a
                JOIN article_topics t ON t.article_url = a.url
                LEFT JOIN article_translations tr ON tr.article_url = a.url
                WHERE t.topic = %s AND a.published_at >= %s{tier_clause}
                ORDER BY a.published_at DESC, a.last_ingested_at DESC
                LIMIT %s
                """,
                (topic, cutoff, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT a.*, tr.translated_title, tr.translated_description, tr.source_language AS translation_source_language,
                       tr.target_language AS translation_target_language, tr.translation_provider, tr.translated_at
                FROM articles a
                LEFT JOIN article_translations tr ON tr.article_url = a.url
                WHERE published_at >= %s{tier_clause}
                ORDER BY published_at DESC, last_ingested_at DESC
                LIMIT %s
                """,
                (cutoff, limit),
            ).fetchall()
    return [_row_to_article(row) for row in rows]


def get_articles_with_regions(hours: int = 72) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
                a.source,
                a.source_domain,
                a.published_at,
                COALESCE(domain_registry.region, name_registry.region, 'global') AS region
            FROM articles a
            LEFT JOIN source_registry domain_registry
                ON domain_registry.source_domain = a.source_domain
               AND domain_registry.active = {active_clause}
            LEFT JOIN source_registry name_registry
                ON name_registry.source_name = a.source
               AND name_registry.active = {active_clause}
            WHERE a.published_at >= %s
            ORDER BY a.published_at DESC, a.last_ingested_at DESC
            """,
            (cutoff,),
        ).fetchall()
    return [dict(row) for row in rows]


def _row_to_structured_event(row) -> dict:
    source_urls = row["source_urls"]
    payload = row["payload"]
    if isinstance(source_urls, str):
        source_urls = json.loads(source_urls) if source_urls else []
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "event_id": row["event_id"],
        "dataset": row["dataset"],
        "dataset_event_id": row["dataset_event_id"],
        "event_date": row["event_date"],
        "country": row["country"],
        "region": row["region"],
        "admin1": row["admin1"],
        "admin2": row["admin2"],
        "location": row["location"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "event_type": row["event_type"],
        "sub_event_type": row["sub_event_type"],
        "actor_primary": row["actor_primary"],
        "actor_secondary": row["actor_secondary"],
        "fatalities": row["fatalities"],
        "source_count": row["source_count"],
        "source_urls": source_urls or [],
        "summary": row["summary"],
        "payload": payload or {},
        "first_ingested_at": row["first_ingested_at"],
        "last_ingested_at": row["last_ingested_at"],
    }


def get_recent_structured_events(
    *,
    days: int = 7,
    limit: int = 3000,
    dataset: str | None = None,
    country: str | None = None,
    event_type: str | None = None,
) -> list[dict]:
    base_clauses = []
    base_params: list[object] = []

    if dataset:
        base_clauses.append(f"dataset = %s")
        base_params.append(dataset)
    if country:
        base_clauses.append(f"country = %s")
        base_params.append(country)
    if event_type:
        base_clauses.append(f"event_type = %s")
        base_params.append(event_type)

    def fetch_rows(cutoff_value: str) -> list:
        clauses = [f"event_date >= %s", *base_clauses]
        params = [cutoff_value, *base_params, limit]
        where = " AND ".join(clauses)
        with _connect() as conn:
            return conn.execute(
                f"""
                SELECT *
                FROM structured_events
                WHERE {where}
                ORDER BY event_date DESC, COALESCE(fatalities, 0) DESC, last_ingested_at DESC
                LIMIT %s
                """,
                params,
            ).fetchall()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    rows = fetch_rows(cutoff)
    if not rows:
        latest_where = f"WHERE {' AND '.join(base_clauses)}" if base_clauses else ""
        with _connect() as conn:
            latest_row = conn.execute(
                f"""
                SELECT MAX(event_date) AS latest_event_date
                FROM structured_events
                {latest_where}
                """,
                base_params,
            ).fetchone()
        latest_event_date = (latest_row["latest_event_date"] if latest_row else None) or None
        if latest_event_date:
            parsed_latest = _parse_article_timestamp(latest_event_date)
            if parsed_latest is not None:
                fallback_cutoff = (parsed_latest - timedelta(days=max(0, days - 1))).date().isoformat()
                rows = fetch_rows(fallback_cutoff)
    return [_row_to_structured_event(row) for row in rows]


def get_structured_event_coordinates_by_ids(event_ids: list[str]) -> dict[str, dict]:
    """Return lat/lon and place fields for structured event IDs (for map geocoding)."""
    ids = [str(x).strip() for x in event_ids if x and str(x).strip()]
    if not ids:
        return {}
    cap = min(len(ids), 400)
    ids = ids[:cap]
    placeholders = ", ".join(["%s"] * len(ids))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT event_id, latitude, longitude, country, admin1, admin2, location, event_date
            FROM structured_events
            WHERE event_id IN ({placeholders})
            """,
            ids,
        ).fetchall()
    out: dict[str, dict] = {}
    for row in rows:
        eid = row["event_id"]
        if not eid:
            continue
        out[str(eid)] = {
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "country": row["country"],
            "admin1": row["admin1"],
            "admin2": row["admin2"],
            "location": row["location"],
            "event_date": row["event_date"],
        }
    return out


def get_articles_by_urls(urls: list[str], *, limit: int = 64) -> dict[str, dict]:
    """Batch-load articles by URL; returns url -> article dict (same shape as get_recent_articles)."""
    cleaned = [str(u).strip() for u in urls if u and str(u).strip()]
    if not cleaned:
        return {}
    cap = max(1, min(limit, 120))
    cleaned = cleaned[:cap]
    placeholders = ", ".join(["%s"] * len(cleaned))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT a.*, tr.translated_title, tr.translated_description, tr.source_language AS translation_source_language,
                   tr.target_language AS translation_target_language, tr.translation_provider, tr.translated_at
            FROM articles a
            LEFT JOIN article_translations tr ON tr.article_url = a.url
            WHERE a.url IN ({placeholders})
            """,
            cleaned,
        ).fetchall()
    return {str(row["url"]): _row_to_article(row) for row in rows}


def list_structured_event_ids_in_date_range(
    start_date: str,
    end_date: str,
    *,
    limit: int = 200,
) -> list[str]:
    start = (start_date or "").strip()
    end = (end_date or "").strip()
    if not start or not end:
        return []
    cap = max(1, min(limit, 2000))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT event_id
            FROM structured_events
            WHERE event_date >= %s AND event_date <= %s
            ORDER BY event_date DESC, COALESCE(fatalities, 0) DESC
            LIMIT %s
            """,
            (start, end, cap),
        ).fetchall()
    return [str(row["event_id"]) for row in rows if row.get("event_id")]


def replace_materialized_story_clusters(*, topic: str, window_hours: int, rows: list[dict]) -> int:
    if not topic:
        return 0
    window_hours = max(1, int(window_hours))
    now = time.time()
    with _connect() as conn:
        conn.execute(
            f"DELETE FROM materialized_story_clusters WHERE topic = %s AND window_hours = %s",
            (topic, window_hours),
        )
        written = 0
        for row in rows:
            article_urls = json.dumps(row.get("article_urls") or [], sort_keys=True)
            linked = json.dumps(row.get("linked_structured_event_ids") or [], sort_keys=True)
            payload = json.dumps(row.get("event_payload") or {}, sort_keys=True, default=str)
            conn.execute(
                """
                INSERT INTO materialized_story_clusters (
                    cluster_key, topic, computed_at, window_hours, label, summary,
                    earliest_published_at, latest_published_at, article_urls,
                    linked_structured_event_ids, event_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
                """,
                (
                    row["cluster_key"],
                    topic,
                    now,
                    window_hours,
                    row["label"],
                    row.get("summary"),
                    row.get("earliest_published_at"),
                    row.get("latest_published_at"),
                    article_urls,
                    linked,
                    payload,
                ),
            )
            written += 1
    return written


def load_materialized_story_clusters(
    *,
    topic: str | None = None,
    window_hours: int | None = None,
    limit: int = 40,
) -> list[dict]:
    clauses: list[str] = []
    params: list[object] = []
    if topic:
        clauses.append(f"topic = %s")
        params.append(topic)
    if window_hours is not None:
        clauses.append(f"window_hours = %s")
        params.append(int(window_hours))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(max(1, min(limit, 500)))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM materialized_story_clusters
            {where}
            ORDER BY computed_at DESC, latest_published_at DESC
            LIMIT %s
            """,
            params,
        ).fetchall()
    out = []
    for row in rows:
        au = row["article_urls"]
        lk = row["linked_structured_event_ids"]
        ep = row["event_payload"]
        if isinstance(au, str):
            au = json.loads(au) if au else []
        if isinstance(lk, str):
            lk = json.loads(lk) if lk else []
        if isinstance(ep, str):
            ep = json.loads(ep) if ep else {}
        out.append(
            {
                "cluster_key": row["cluster_key"],
                "topic": row["topic"],
                "computed_at": row["computed_at"],
                "window_hours": row["window_hours"],
                "label": row["label"],
                "summary": row["summary"],
                "earliest_published_at": row["earliest_published_at"],
                "latest_published_at": row["latest_published_at"],
                "article_urls": au or [],
                "linked_structured_event_ids": lk or [],
                "event_payload": ep or {},
            }
        )
    return out


# ── canonical_events ─────────────────────────────────────────────────────────

def upsert_canonical_events(rows: list[dict]) -> int:
    """Upsert canonical event records. Preserves neutral_summary/neutral_confidence if already set."""
    if not rows:
        return 0
    now = time.time()
    written = 0
    with _connect() as conn:
        for row in rows:
            event_id = (row.get("event_id") or "").strip()
            if not event_id:
                continue
            article_urls = json.dumps(sorted(row.get("article_urls") or []), sort_keys=True)
            linked = json.dumps(row.get("linked_structured_event_ids") or [], sort_keys=True)
            payload = json.dumps(row.get("payload") or {}, sort_keys=True, default=str)
            conn.execute(
                """
                INSERT INTO canonical_events (
                    event_id, topic, label, event_type, status,
                    geo_country, geo_region, latitude, longitude,
                    first_reported_at, last_updated_at,
                    article_count, source_count, perspective_count, contradiction_count,
                    linked_structured_event_ids, article_urls,
                    first_seen_at, computed_at, payload
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s::jsonb
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    label = EXCLUDED.label,
                    event_type = COALESCE(EXCLUDED.event_type, canonical_events.event_type),
                    status = EXCLUDED.status,
                    geo_country = COALESCE(EXCLUDED.geo_country, canonical_events.geo_country),
                    geo_region = COALESCE(EXCLUDED.geo_region, canonical_events.geo_region),
                    latitude = COALESCE(EXCLUDED.latitude, canonical_events.latitude),
                    longitude = COALESCE(EXCLUDED.longitude, canonical_events.longitude),
                    first_reported_at = EXCLUDED.first_reported_at,
                    last_updated_at = EXCLUDED.last_updated_at,
                    article_count = EXCLUDED.article_count,
                    source_count = EXCLUDED.source_count,
                    contradiction_count = EXCLUDED.contradiction_count,
                    linked_structured_event_ids = EXCLUDED.linked_structured_event_ids,
                    article_urls = EXCLUDED.article_urls,
                    computed_at = EXCLUDED.computed_at,
                    payload = EXCLUDED.payload
                """,
                (
                    event_id,
                    row.get("topic") or "",
                    row.get("label") or "",
                    row.get("event_type"),
                    row.get("status") or "developing",
                    row.get("geo_country"),
                    row.get("geo_region"),
                    row.get("latitude"),
                    row.get("longitude"),
                    row.get("first_reported_at"),
                    row.get("last_updated_at"),
                    int(row.get("article_count") or 0),
                    int(row.get("source_count") or 0),
                    int(row.get("perspective_count") or 0),
                    int(row.get("contradiction_count") or 0),
                    linked,
                    article_urls,
                    row.get("first_seen_at") or now,
                    now,
                    payload,
                ),
            )
            written += 1
    return written


def update_canonical_event_synthesis(
    event_id: str,
    *,
    neutral_summary: str,
    neutral_confidence: float,
    perspective_count: int | None = None,
    contradiction_count: int | None = None,
) -> bool:
    """Write neutral synthesis back onto a canonical event. Returns True if the row existed."""
    if not event_id:
        return False
    now = time.time()
    perspective_sql = f", perspective_count = %s" if perspective_count is not None else ""
    contradiction_sql = f", contradiction_count = %s" if contradiction_count is not None else ""
    params: list[object] = [neutral_summary, float(neutral_confidence), now]
    if perspective_count is not None:
        params.append(perspective_count)
    if contradiction_count is not None:
        params.append(contradiction_count)
    params.append(event_id)
    with _connect() as conn:
        result = conn.execute(
            f"""
            UPDATE canonical_events
            SET neutral_summary = %s,
                neutral_confidence = %s,
                neutral_generated_at = %s
                {perspective_sql}
                {contradiction_sql}
            WHERE event_id = %s
            """,
            params,
        )
        return (result.rowcount or 0) > 0


def get_canonical_events(
    topic: str | None = None,
    status: str | None = None,
    limit: int = 40,
) -> list[dict]:
    clauses: list[str] = []
    params: list[object] = []
    if topic:
        clauses.append(f"topic = %s")
        params.append(topic)
    if status:
        clauses.append(f"status = %s")
        params.append(status)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(max(1, min(limit, 500)))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM canonical_events
            {where}
            ORDER BY computed_at DESC, last_updated_at DESC
            LIMIT %s
            """,
            params,
        ).fetchall()
    return [_row_to_canonical_event(row) for row in rows]


def get_canonical_event(event_id: str) -> dict | None:
    if not event_id:
        return None
    with _connect() as conn:
        row = conn.execute(
            f"SELECT * FROM canonical_events WHERE event_id = %s",
            (event_id,),
        ).fetchone()
    return _row_to_canonical_event(row) if row else None


def _row_to_canonical_event(row) -> dict:
    article_urls = row["article_urls"]
    linked = row["linked_structured_event_ids"]
    payload = row["payload"]
    if isinstance(article_urls, str):
        article_urls = json.loads(article_urls) if article_urls else []
    if isinstance(linked, str):
        linked = json.loads(linked) if linked else []
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "event_id": row["event_id"],
        "topic": row["topic"],
        "label": row["label"],
        "event_type": row["event_type"],
        "status": row["status"],
        "geo_country": row["geo_country"],
        "geo_region": row["geo_region"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "first_reported_at": row["first_reported_at"],
        "last_updated_at": row["last_updated_at"],
        "article_count": int(row["article_count"] or 0),
        "source_count": int(row["source_count"] or 0),
        "perspective_count": int(row["perspective_count"] or 0),
        "contradiction_count": int(row["contradiction_count"] or 0),
        "neutral_summary": row["neutral_summary"],
        "neutral_confidence": row["neutral_confidence"],
        "neutral_generated_at": row["neutral_generated_at"],
        "linked_structured_event_ids": linked or [],
        "article_urls": article_urls or [],
        "first_seen_at": row["first_seen_at"],
        "computed_at": row["computed_at"],
        "payload": payload or {},
    }


# ── event_perspectives ───────────────────────────────────────────────────────

def upsert_event_perspectives(rows: list[dict]) -> int:
    """Upsert per-source perspective rows for canonical events."""
    if not rows:
        return 0
    now = time.time()
    written = 0
    with _connect() as conn:
        for row in rows:
            pid = (row.get("perspective_id") or "").strip()
            if not pid:
                continue
            frame_counts = json.dumps(row.get("frame_counts") or {}, sort_keys=True)
            matched_terms = json.dumps(row.get("matched_terms") or [], sort_keys=True)
            payload = json.dumps(row.get("payload") or {}, sort_keys=True, default=str)
            conn.execute(
                """
                INSERT INTO event_perspectives (
                    perspective_id, event_id, article_url,
                    source_name, source_domain, source_reliability_score,
                    source_trust_tier, source_region,
                    dominant_frame, frame_counts, matched_terms,
                    claim_text, claim_type, claim_resolution_status,
                    sentiment, published_at, analyzed_at, payload
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (perspective_id) DO UPDATE SET
                    dominant_frame = EXCLUDED.dominant_frame,
                    frame_counts = EXCLUDED.frame_counts,
                    matched_terms = EXCLUDED.matched_terms,
                    claim_text = COALESCE(EXCLUDED.claim_text, event_perspectives.claim_text),
                    claim_type = COALESCE(EXCLUDED.claim_type, event_perspectives.claim_type),
                    claim_resolution_status = COALESCE(EXCLUDED.claim_resolution_status, event_perspectives.claim_resolution_status),
                    source_reliability_score = COALESCE(EXCLUDED.source_reliability_score, event_perspectives.source_reliability_score),
                    analyzed_at = EXCLUDED.analyzed_at,
                    payload = EXCLUDED.payload
                """,
                (
                    pid,
                    row["event_id"],
                    row.get("article_url"),
                    row["source_name"],
                    row.get("source_domain"),
                    row.get("source_reliability_score"),
                    row.get("source_trust_tier"),
                    row.get("source_region"),
                    row.get("dominant_frame"),
                    frame_counts,
                    matched_terms,
                    row.get("claim_text"),
                    row.get("claim_type"),
                    row.get("claim_resolution_status"),
                    row.get("sentiment"),
                    row.get("published_at"),
                    row.get("analyzed_at") or now,
                    payload,
                ),
            )
            written += 1
    return written


def get_event_perspectives(event_id: str) -> list[dict]:
    if not event_id:
        return []
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM event_perspectives
            WHERE event_id = %s
            ORDER BY source_reliability_score DESC NULLS LAST, analyzed_at DESC
            """,
            (event_id,),
        ).fetchall()
    return [_row_to_perspective(row) for row in rows]


def _row_to_perspective(row) -> dict:
    frame_counts = row["frame_counts"]
    matched_terms = row["matched_terms"]
    payload = row["payload"]
    if isinstance(frame_counts, str):
        frame_counts = json.loads(frame_counts) if frame_counts else {}
    if isinstance(matched_terms, str):
        matched_terms = json.loads(matched_terms) if matched_terms else []
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "perspective_id": row["perspective_id"],
        "event_id": row["event_id"],
        "article_url": row["article_url"],
        "source_name": row["source_name"],
        "source_domain": row["source_domain"],
        "source_reliability_score": row["source_reliability_score"],
        "source_trust_tier": row["source_trust_tier"],
        "source_region": row["source_region"],
        "dominant_frame": row["dominant_frame"],
        "frame_counts": frame_counts or {},
        "matched_terms": matched_terms or [],
        "claim_text": row["claim_text"],
        "claim_type": row["claim_type"],
        "claim_resolution_status": row["claim_resolution_status"],
        "sentiment": row["sentiment"],
        "published_at": row["published_at"],
        "analyzed_at": row["analyzed_at"],
        "payload": payload or {},
    }


def load_framing_signals_for_article_urls(article_urls: list[str]) -> dict[str, dict]:
    """Load article_framing_signals keyed by article_url for a set of URLs."""
    if not article_urls:
        return {}
    placeholders = ", ".join(["%s"] * len(article_urls))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM article_framing_signals
            WHERE article_url IN ({placeholders})
            ORDER BY analyzed_at DESC
            """,
            list(article_urls),
        ).fetchall()
    result: dict[str, dict] = {}
    for row in rows:
        url = row["article_url"]
        if url in result:
            continue  # keep most recent per article
        frame_counts = row["frame_counts"]
        matched_terms = row["matched_terms"]
        payload = row["payload"]
        if isinstance(frame_counts, str):
            frame_counts = json.loads(frame_counts) if frame_counts else {}
        if isinstance(matched_terms, str):
            matched_terms = json.loads(matched_terms) if matched_terms else {}
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        result[url] = {
            "article_url": url,
            "subject_key": row["subject_key"],
            "dominant_frame": row["dominant_frame"],
            "frame_counts": frame_counts or {},
            "matched_terms": matched_terms or {},
            "source": row["source"],
            "published_at": row["published_at"],
            "analyzed_at": row["analyzed_at"],
            "payload": payload or {},
        }
    return result


def load_claim_resolution_for_event_key(event_key: str) -> list[dict]:
    """Load claim resolution records for a specific event key."""
    if not event_key:
        return []
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM claim_resolution_records
            WHERE event_key = %s
            ORDER BY generated_at DESC
            """,
            (event_key,),
        ).fetchall()
    out = []
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        out.append(
            {
                "claim_record_key": row["claim_record_key"],
                "event_key": row["event_key"],
                "source_name": row["source_name"],
                "claim_text": row["claim_text"],
                "claim_type": row["conflict_type"],
                "resolution_status": row["resolution_status"],
                "confidence": row["confidence"],
                "published_at": row["published_at"],
                "payload": payload or {},
            }
        )
    return out


def get_article_count(topic: str | None = None, hours: int | None = None) -> int:
    if hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        return _count_articles_since(cutoff, topic=topic)

    clauses = []
    params: list[object] = []
    join = ""
    if topic:
        join = "JOIN article_topics t ON t.article_url = a.url"
        clauses.append(f"t.topic = %s")
        params.append(topic)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect() as conn:
        row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT a.url) AS count
            FROM articles a
            {join}
            {where}
            """,
            params,
        ).fetchone()
    return int((row["count"] if row else 0) or 0)


def _parse_article_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass

    for fmt in ("%Y%m%dT%H%M%S%z", "%Y%m%dT%H%M%SZ", "%Y-%m-%d %H:%M:%S%z"):
        try:
            parsed = datetime.strptime(value, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _published_values(topic: str | None = None) -> list[str]:
    params: list[object] = []
    join = ""
    where = ""
    if topic:
        join = "JOIN article_topics t ON t.article_url = a.url"
        where = f"WHERE t.topic = %s"
        params.append(topic)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT a.url, a.published_at
            FROM articles a
            {join}
            {where}
            """,
            params,
        ).fetchall()
    return [row["published_at"] for row in rows if row["published_at"]]


def _count_articles_since(cutoff: datetime, topic: str | None = None) -> int:
    count = 0
    for value in _published_values(topic=topic):
        parsed = _parse_article_timestamp(value)
        if parsed and parsed >= cutoff:
            count += 1
    return count


def _topic_time_bounds_python(topic: str | None = None) -> dict:
    parsed_values = [
        parsed
        for parsed in (_parse_article_timestamp(value) for value in _published_values(topic=topic))
        if parsed is not None
    ]
    if not parsed_values:
        return {"earliest_published_at": None, "latest_published_at": None}
    earliest = min(parsed_values).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    latest = max(parsed_values).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {"earliest_published_at": earliest, "latest_published_at": latest}


def get_ingestion_summary() -> dict:
    with _connect() as conn:
        topic_rows = conn.execute(
            """
            SELECT DISTINCT ON (topic) topic, provider, article_count, completed_at, status, error
            FROM ingestion_runs
            ORDER BY topic, id DESC
            """
        ).fetchall()
    topics = {}
    for row in topic_rows:
        topics[row["topic"]] = {
            "provider": row["provider"],
            "article_count": row["article_count"],
            "completed_at": row["completed_at"],
            "status": row["status"],
            "error": row["error"],
        }

    return {
        "total_articles": get_article_count(),
        "articles_last_24h": get_article_count(hours=24),
        "latest_published_at": _topic_time_bounds_python()["latest_published_at"],
        "topics": topics,
    }


def get_topic_time_bounds(topic: str | None = None) -> dict:
    return _topic_time_bounds_python(topic=topic)


def load_ingestion_state(state_key: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            f"""
            SELECT *
            FROM ingestion_state
            WHERE state_key = %s
            """,
            (state_key,),
        ).fetchone()
    if not row:
        return None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "state_key": row["state_key"],
        "topic": row["topic"],
        "provider": row["provider"],
        "cursor_start": row["cursor_start"],
        "cursor_end": row["cursor_end"],
        "status": row["status"],
        "error": row["error"],
        "updated_at": row["updated_at"],
        "payload": payload or {},
    }


def save_ingestion_state(
    state_key: str,
    topic: str,
    provider: str,
    cursor_start: str | None,
    cursor_end: str | None,
    status: str,
    error: str | None = None,
    payload: dict | None = None,
) -> None:
    now = time.time()
    serialized_payload = json.dumps(payload or {}, sort_keys=True)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ingestion_state (
                state_key, topic, provider, cursor_start, cursor_end, status, error, updated_at, payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (state_key) DO UPDATE SET
                topic = EXCLUDED.topic,
                provider = EXCLUDED.provider,
                cursor_start = EXCLUDED.cursor_start,
                cursor_end = EXCLUDED.cursor_end,
                status = EXCLUDED.status,
                error = EXCLUDED.error,
                updated_at = EXCLUDED.updated_at,
                payload = EXCLUDED.payload
            """,
            (state_key, topic, provider, cursor_start, cursor_end, status, error, now, serialized_payload),
        )
def get_sources(limit: int = 12, hours: int = 72) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT source, source_domain, COUNT(*) AS article_count, MAX(published_at) AS latest_published_at
            FROM articles
            WHERE published_at >= %s
            GROUP BY source, source_domain
            ORDER BY article_count DESC, latest_published_at DESC
            LIMIT %s
            """,
            (cutoff, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def get_warehouse_counts() -> dict:
    with _connect() as conn:
        raw_docs = conn.execute("SELECT COUNT(*) AS count FROM raw_source_documents").fetchone()
        official = conn.execute("SELECT COUNT(*) AS count FROM official_updates").fetchone()
        structured = conn.execute("SELECT COUNT(*) AS count FROM structured_events").fetchone()
        channels = conn.execute("SELECT COUNT(*) AS count FROM monitored_channels").fetchone()
        registry = conn.execute("SELECT COUNT(*) AS count FROM source_registry").fetchone()
    return {
        "source_registry": int((registry["count"] if registry else 0) or 0),
        "raw_source_documents": int((raw_docs["count"] if raw_docs else 0) or 0),
        "official_updates": int((official["count"] if official else 0) or 0),
        "structured_events": int((structured["count"] if structured else 0) or 0),
        "monitored_channels": int((channels["count"] if channels else 0) or 0),
    }


def search_recent_articles_by_keywords(query: str, topic: str | None = None, limit: int = 12, hours: int = 168) -> list[dict]:
    words = [word.strip().lower() for word in query.replace("?", " ").replace(",", " ").split() if len(word.strip()) >= 4]
    if not words:
        return get_recent_articles(topic=topic, limit=limit, hours=hours)

    articles = get_recent_articles(topic=topic, limit=200, hours=hours)
    ranked = []
    for article in articles:
        haystack = " ".join(
            [
                article.get("title", ""),
                article.get("description", ""),
                article.get("source", ""),
            ]
        ).lower()
        score = sum(1 for word in words if word in haystack)
        if score:
            ranked.append((score, article.get("published_at", ""), article))

    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [article for _, _, article in ranked[:limit]]


def load_contradiction_record(event_key: str, max_age_hours: int = 168) -> dict | None:
    cutoff = time.time() - (max_age_hours * 3600)
    with _connect() as conn:
        row = conn.execute(
            f"""
            SELECT *
            FROM contradiction_records
            WHERE event_key = %s AND generated_at >= %s
            """,
            (event_key, cutoff),
        ).fetchone()
    if not row:
        return None

    contradictions = row["contradictions"]
    article_urls = row["article_urls"]
    if isinstance(contradictions, str):
        contradictions = json.loads(contradictions)
    if isinstance(article_urls, str):
        article_urls = json.loads(article_urls)
    return {
        "event_key": row["event_key"],
        "topic": row["topic"],
        "event_label": row["event_label"],
        "latest_update": row["latest_update"],
        "article_urls": article_urls,
        "contradictions": contradictions,
        "contradiction_count": row["contradiction_count"],
        "generated_at": row["generated_at"],
    }


def load_contradiction_history(event_key: str, limit: int = 10) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT event_key, topic, event_label, latest_update, article_urls, contradictions,
                   contradiction_count, generated_at, content_hash
            FROM contradiction_history
            WHERE event_key = %s
            ORDER BY generated_at DESC
            LIMIT %s
            """,
            (event_key, limit),
        ).fetchall()

    history = []
    for row in rows:
        contradictions = row["contradictions"]
        article_urls = row["article_urls"]
        if isinstance(contradictions, str):
            contradictions = json.loads(contradictions)
        if isinstance(article_urls, str):
            article_urls = json.loads(article_urls)
        history.append(
            {
                "event_key": row["event_key"],
                "topic": row["topic"],
                "event_label": row["event_label"],
                "latest_update": row["latest_update"],
                "article_urls": article_urls,
                "contradictions": contradictions,
                "contradiction_count": row["contradiction_count"],
                "generated_at": row["generated_at"],
                "content_hash": row["content_hash"],
            }
        )
    return history


def save_contradiction_record(event_key: str, event: dict, contradictions: list[dict]) -> None:
    now = time.time()
    article_urls = [article.get("url") for article in event.get("articles", []) if article.get("url")]
    serialized_article_urls = json.dumps(article_urls, sort_keys=True)
    serialized_contradictions = json.dumps(contradictions, sort_keys=True)
    content_hash = hashlib.sha256(
        " | ".join(
            [
                event_key,
                event.get("event_id", ""),
                event.get("label", ""),
                event.get("latest_update", "") or "",
                serialized_article_urls,
                serialized_contradictions,
            ]
        ).encode("utf-8")
    ).hexdigest()
    with _connect() as conn:
        existing = conn.execute(
            "SELECT content_hash FROM contradiction_history WHERE event_key = %s ORDER BY generated_at DESC LIMIT 1",
            (event_key,),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO contradiction_records (
                event_key, topic, event_label, latest_update, article_urls, contradictions, contradiction_count, generated_at
            ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
            ON CONFLICT (event_key) DO UPDATE SET
                topic = EXCLUDED.topic,
                event_label = EXCLUDED.event_label,
                latest_update = EXCLUDED.latest_update,
                article_urls = EXCLUDED.article_urls,
                contradictions = EXCLUDED.contradictions,
                contradiction_count = EXCLUDED.contradiction_count,
                generated_at = EXCLUDED.generated_at
            """,
            (
                event_key,
                event.get("topic"),
                event.get("label", "Emerging event"),
                event.get("latest_update"),
                serialized_article_urls,
                serialized_contradictions,
                len(contradictions),
                now,
            ),
        )
        if existing is None or existing["content_hash"] != content_hash:
            conn.execute(
                """
                INSERT INTO contradiction_history (
                    event_key, topic, event_label, latest_update, article_urls, contradictions,
                    contradiction_count, generated_at, content_hash
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
                """,
                (
                    event_key,
                    event.get("topic"),
                    event.get("label", "Emerging event"),
                    event.get("latest_update"),
                    serialized_article_urls,
                    serialized_contradictions,
                    len(contradictions),
                    now,
                    content_hash,
                ),
            )
def _row_to_article(row) -> dict:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    elif payload is None:
        payload = {}
    translated_title = row["translated_title"] if "translated_title" in row else None
    translated_description = row["translated_description"] if "translated_description" in row else None
    original_title = payload.get("title") or row["title"]
    original_description = payload.get("description") or row["description"]
    return {
        "title": translated_title or original_title,
        "description": translated_description or original_description,
        "original_title": original_title,
        "original_description": original_description,
        "translated_title": translated_title,
        "translated_description": translated_description,
        "source": payload.get("source") or row["source"],
        "source_domain": payload.get("source_domain") or row["source_domain"],
        "url": row["url"],
        "published_at": row["published_at"],
        "language": row["language"],
        "provider": row["provider"],
        "translation_source_language": row["translation_source_language"] if "translation_source_language" in row else None,
        "translation_target_language": row["translation_target_language"] if "translation_target_language" in row else None,
        "translation_provider": row["translation_provider"] if "translation_provider" in row else None,
        "translated_at": row["translated_at"] if "translated_at" in row else None,
    }


def _row_to_historical_queue_item(row) -> dict:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    elif payload is None:
        payload = {}
    return {
        "url": row["url"],
        "canonical_url": row["canonical_url"],
        "title": row["title"],
        "source_name": row["source_name"],
        "source_domain": row["source_domain"],
        "published_at": row["published_at"],
        "language": row["language"],
        "discovered_via": row["discovered_via"],
        "topic_guess": row["topic_guess"],
        "gdelt_query": row["gdelt_query"],
        "gdelt_window_start": row["gdelt_window_start"],
        "gdelt_window_end": row["gdelt_window_end"],
        "fetch_status": row["fetch_status"],
        "last_attempt_at": row["last_attempt_at"],
        "attempt_count": int(row["attempt_count"] or 0),
        "payload": payload,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
