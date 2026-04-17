"""Database schema / migrations helpers.

Contains `init_db()` to create necessary tables and indexes.
This is extracted from the original `backend/corpus.py` to isolate schema
creation logic.
"""

from db.common import _connect


def init_db():
    with _connect() as conn:
        conn.execute("""
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
            """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS article_topics (
                article_url TEXT NOT NULL REFERENCES articles(url) ON DELETE CASCADE,
                topic TEXT NOT NULL,
                assigned_at DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (article_url, topic)
            )
            """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS article_translations (
                article_url TEXT PRIMARY KEY REFERENCES articles(url) ON DELETE CASCADE,
                source_language TEXT,
                target_language TEXT NOT NULL,
                translated_title TEXT NOT NULL,
                translated_description TEXT,
                translation_provider TEXT NOT NULL,
                translated_at DOUBLE PRECISION NOT NULL
            )
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        # Mark this table as materialized / derived from canonical_events
        conn.execute(
            "COMMENT ON TABLE materialized_story_clusters IS 'MATERIALIZED: derived from canonical_events; refresh via story_materialization pipeline'"
        )
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        # Contradiction records are derived snapshots for analytics; refresh via analytics pipeline
        conn.execute(
            "COMMENT ON TABLE contradiction_records IS 'DERIVED: contradiction snapshots computed from perspectives and articles; refresh via analytics pipeline'"
        )
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        # Claim-resolution records are derived materializations
        conn.execute(
            "COMMENT ON TABLE claim_resolution_records IS 'DERIVED: claim resolution snapshots; refresh via claim-resolution pipeline'"
        )
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        # Event observation archive is a derived materialization of observed evidence
        conn.execute(
            "COMMENT ON TABLE event_observation_archive IS 'DERIVED: observation archive derived from ingestion evidence; refresh via ingestion/observation pipeline'"
        )
        conn.execute("""
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
            """)
        conn.execute("""
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
            """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entity_mentions (
                id          BIGSERIAL PRIMARY KEY,
                entity      TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                topic       TEXT NOT NULL,
                article_url TEXT NOT NULL,
                mentioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (topic, article_url, entity)
            )
            """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entity_mentions_entity "
            "ON entity_mentions (entity)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entity_mentions_mentioned_at "
            "ON entity_mentions (mentioned_at)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS entity_cooccurrences (
                id          BIGSERIAL PRIMARY KEY,
                entity_a    TEXT NOT NULL,
                entity_b    TEXT NOT NULL,
                topic       TEXT NOT NULL,
                article_url TEXT NOT NULL,
                mentioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (topic, article_url, entity_a, entity_b)
            )
            """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entity_cooc_a "
            "ON entity_cooccurrences (entity_a)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entity_cooc_b "
            "ON entity_cooccurrences (entity_b)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS entity_links (
                entity       TEXT NOT NULL,
                qid          TEXT NOT NULL,
                label        TEXT,
                description  TEXT,
                source       TEXT,
                retrieved_at TIMESTAMPTZ,
                PRIMARY KEY (entity, qid)
            )
            """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entity_links_entity "
            "ON entity_links (entity)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_article_summaries_topic ON article_summaries (topic, published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles (published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_last_ingested_at ON articles (last_ingested_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_article_topics_topic ON article_topics (topic)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_article_translations_target ON article_translations (target_language, translated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_historical_url_queue_status ON historical_url_queue (fetch_status, published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_historical_url_queue_domain ON historical_url_queue (source_domain, published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ingestion_state_provider_topic ON ingestion_state (provider, topic)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_source_registry_type ON source_registry (source_type, trust_tier)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_raw_source_documents_source ON raw_source_documents (source_id, published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_structured_events_date ON structured_events (event_date DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_structured_events_dataset ON structured_events (dataset, event_date DESC)"
        )
        conn.execute(
            "ALTER TABLE structured_events "
            "ADD COLUMN IF NOT EXISTS superseded_by TEXT REFERENCES structured_events(event_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_structured_events_superseded "
            "ON structured_events (superseded_by) WHERE superseded_by IS NOT NULL"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_materialized_story_clusters_topic ON materialized_story_clusters (topic, computed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_domain_published ON articles (source_domain, published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_official_updates_body ON official_updates (issuing_body, published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitored_channels_key ON monitored_channels (channel_key, posted_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_evidence_links_topic ON evidence_links (topic, linked_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_contradiction_topic ON contradiction_records (topic)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_contradiction_history_event_key ON contradiction_history (event_key, generated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_article_framing_subject ON article_framing_signals (subject_key, published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_narrative_drift_subject ON narrative_drift_snapshots (subject_key, generated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_claim_resolution_source ON claim_resolution_records (source_name, generated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_source_reliability_topic ON source_reliability_snapshots (topic, generated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_prediction_status ON prediction_ledger (status, created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_observation_topic ON event_observation_archive (topic, first_othello_seen_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_entity_reference_provider ON entity_reference_cache (provider, fetched_at DESC)"
        )

        # ── canonical event model ─────────────────────────────────────
        conn.execute("""
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
                importance_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                importance_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
                neutral_summary TEXT,
                neutral_confidence DOUBLE PRECISION,
                neutral_generated_at DOUBLE PRECISION,
                linked_structured_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                article_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
                first_seen_at DOUBLE PRECISION NOT NULL,
                computed_at DOUBLE PRECISION NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """)
        conn.execute(
            "ALTER TABLE canonical_events ADD COLUMN IF NOT EXISTS importance_score DOUBLE PRECISION NOT NULL DEFAULT 0"
        )
        conn.execute(
            "ALTER TABLE canonical_events ADD COLUMN IF NOT EXISTS importance_reasons JSONB NOT NULL DEFAULT '[]'::jsonb"
        )
        conn.execute("""
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
            """)
        conn.execute("""
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
            )
            """)
        conn.execute("""
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
            )
            """)
        conn.execute(
            "COMMENT ON TABLE cluster_assignment_evidence IS 'DERIVED: per-article assignment evidence for volatile observation clusters; refresh via story_materialization pipeline'"
        )

        # ── event identity resolution ─────────────────────────────────
        # Map volatile observation keys (current clustering hashes) onto
        # stable canonical event IDs.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS event_identity_map (
                observation_key TEXT PRIMARY KEY,
                event_id TEXT NOT NULL REFERENCES canonical_events(event_id) ON DELETE CASCADE,
                topic TEXT,
                first_mapped_at DOUBLE PRECISION NOT NULL,
                last_seen_at DOUBLE PRECISION NOT NULL,
                identity_confidence DOUBLE PRECISION,
                identity_reasons JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS event_identity_events (
                id BIGSERIAL PRIMARY KEY,
                observation_key TEXT NOT NULL,
                event_id TEXT NOT NULL REFERENCES canonical_events(event_id) ON DELETE CASCADE,
                action TEXT NOT NULL,
                confidence DOUBLE PRECISION,
                reasons JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at DOUBLE PRECISION NOT NULL
            )
            """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_canonical_events_topic ON canonical_events (topic, computed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_canonical_events_status ON canonical_events (status, last_updated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_perspectives_event ON event_perspectives (event_id, analyzed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_perspectives_source ON event_perspectives (source_name, analyzed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_canonical_event_obs_event ON canonical_event_observations (event_id, observed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_canonical_event_obs_topic ON canonical_event_observations (topic, observed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cluster_assignment_obs ON cluster_assignment_evidence (observation_key, computed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cluster_assignment_event ON cluster_assignment_evidence (event_id, computed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cluster_assignment_topic ON cluster_assignment_evidence (topic, computed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_identity_map_event ON event_identity_map (event_id, last_seen_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_identity_map_topic ON event_identity_map (topic, last_seen_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_identity_events_event ON event_identity_events (event_id, created_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_event_identity_events_obs ON event_identity_events (observation_key, created_at DESC)"
        )

        # ── analyst corrections feedback loop ─────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analyst_corrections (
                id BIGSERIAL PRIMARY KEY,
                correction_type TEXT NOT NULL,
                event_a_id TEXT NOT NULL REFERENCES canonical_events(event_id) ON DELETE CASCADE,
                event_b_id TEXT REFERENCES canonical_events(event_id) ON DELETE CASCADE,
                article_url TEXT,
                created_at DOUBLE PRECISION NOT NULL,
                applied BOOLEAN NOT NULL DEFAULT FALSE
            )
            """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_analyst_corrections_event_a ON analyst_corrections (event_a_id, applied DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_analyst_corrections_applied ON analyst_corrections (applied, created_at DESC)"
        )

        # ── v2 tables (typed timestamps, Postgres-only) ──────────────
        conn.execute("""
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
            """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS article_topics_v2 (
                article_url TEXT NOT NULL REFERENCES articles_v2(url) ON DELETE CASCADE,
                topic TEXT NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (article_url, topic)
            )
            """)
        conn.execute("""
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
            """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_v2_published_at ON articles_v2 (published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_v2_last_ingested ON articles_v2 (last_ingested_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_v2_domain_published ON articles_v2 (source_domain, published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_article_topics_v2_topic ON article_topics_v2 (topic)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_article_summaries_v2_topic ON article_summaries_v2 (topic, published_at DESC)"
        )
        return
