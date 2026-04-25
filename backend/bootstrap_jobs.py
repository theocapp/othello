"""
Bootstrap job orchestration for worker initialization.

Assembles the list of startup jobs based on WORKER_* configuration flags.
"""

from core.config import (
    WORKER_BOOTSTRAP_MODE,
    WORKER_ENABLE_ANALYTICS,
    WORKER_ENABLE_INGESTION,
    WORKER_ENABLE_TRANSLATIONS,
)
from services.ingest_service import (
    ingest_all_topics,
    refresh_acled_events,
    refresh_direct_feed_layer,
    refresh_official_updates,
    refresh_recent_translations,
    run_incremental_gdelt_backfill,
    sync_registry_mirror,
)
from services.article_event_pipeline import populate_canonical_events_from_articles


def build_bootstrap_jobs():
    """
    Build list of bootstrap jobs based on worker configuration.

    Returns:
        List of (job_label, job_function) tuples to execute during worker startup.
    """
    bootstrap_jobs = []

    # Core ingestion jobs for ingest and full modes
    if WORKER_ENABLE_INGESTION and WORKER_BOOTSTRAP_MODE in {"ingest", "full"}:
        bootstrap_jobs.extend(
            [
                ("ingest_all_topics", ingest_all_topics),
                ("gdelt_backfill", run_incremental_gdelt_backfill),
                ("refresh_direct_feed_layer", refresh_direct_feed_layer),
            ]
        )

    # Extended jobs for full bootstrap mode
    if WORKER_ENABLE_INGESTION and WORKER_BOOTSTRAP_MODE == "full":
        bootstrap_jobs.extend(
            [
                ("sync_registry_mirror", sync_registry_mirror),
                ("refresh_official_updates", refresh_official_updates),
                ("refresh_acled_events", refresh_acled_events),
            ]
        )
        if WORKER_ENABLE_TRANSLATIONS:
            bootstrap_jobs.append(
                ("refresh_recent_translations", refresh_recent_translations)
            )

    if WORKER_ENABLE_ANALYTICS:
        bootstrap_jobs.append(
            ("article_event_pipeline", lambda: populate_canonical_events_from_articles(days=3, limit=2000))
        )

    return bootstrap_jobs
