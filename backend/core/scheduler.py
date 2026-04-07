"""Scheduler construction and lifecycle helpers."""

from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler

from core.config import (
    ACLED_REFRESH_MINUTES,
    ANALYTICS_WARM_DELAY_SECONDS,
    ARTICLE_FALLBACK_REFRESH_MINUTES,
    DIRECT_FEED_REFRESH_MINUTES,
    FORESIGHT_REFRESH_MINUTES,
    GDELT_GKG_REFRESH_MINUTES,
    HISTORICAL_FETCH_REFRESH_MINUTES,
    INTERNAL_SCHEDULER_ENABLED,
    NARRATIVE_DRIFT_REFRESH_MINUTES,
    OFFICIAL_UPDATE_REFRESH_MINUTES,
    SOURCE_RELIABILITY_REFRESH_MINUTES,
    STORY_MATERIALIZATION_REFRESH_MINUTES,
    WORKER_ENABLE_ANALYTICS,
    WORKER_ENABLE_INGESTION,
    WORKER_ENABLE_TRANSLATIONS,
)

_scheduler: BackgroundScheduler | None = None


def get_scheduler() -> BackgroundScheduler | None:
    return _scheduler


def build_scheduler(
    include_ingestion: bool = True,
    include_translations: bool = True,
    include_analytics: bool = True,
) -> BackgroundScheduler:
    from services.ingest_service import (
        ingest_article_fallback,
        refresh_acled_events,
        refresh_direct_feed_layer,
        refresh_gdelt_gkg_events,
        refresh_official_updates,
        refresh_recent_translations,
        refresh_source_reliability,
        refresh_foresight_layer,
        refresh_narrative_drift_layer,
        run_scheduled_gdelt_backfill,
        run_scheduled_historical_queue_fetch,
        run_scheduled_ingest_cycle,
        run_scheduled_story_materialization,
        sync_registry_mirror,
    )
    from services.briefing_service import refresh_snapshot_layer

    scheduler = BackgroundScheduler(
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 300,
        }
    )
    if include_ingestion:
        scheduler.add_job(
            run_scheduled_ingest_cycle, "interval", minutes=15, id="refresh_corpus"
        )
        scheduler.add_job(
            run_scheduled_gdelt_backfill, "interval", minutes=10, id="gdelt_backfill"
        )
        scheduler.add_job(
            refresh_direct_feed_layer,
            "interval",
            minutes=DIRECT_FEED_REFRESH_MINUTES,
            id="refresh_directfeeds",
        )
        scheduler.add_job(
            run_scheduled_historical_queue_fetch,
            "interval",
            minutes=HISTORICAL_FETCH_REFRESH_MINUTES,
            id="historical_queue_fetch",
        )
        scheduler.add_job(
            ingest_article_fallback,
            "interval",
            minutes=ARTICLE_FALLBACK_REFRESH_MINUTES,
            id="article_fallback",
        )
        scheduler.add_job(
            sync_registry_mirror, "interval", hours=6, id="mirror_registry_articles"
        )
        scheduler.add_job(
            refresh_official_updates,
            "interval",
            minutes=OFFICIAL_UPDATE_REFRESH_MINUTES,
            id="refresh_official_updates",
        )
        scheduler.add_job(
            refresh_acled_events,
            "interval",
            minutes=ACLED_REFRESH_MINUTES,
            id="refresh_acled_events",
        )
        scheduler.add_job(
            refresh_gdelt_gkg_events,
            "interval",
            minutes=GDELT_GKG_REFRESH_MINUTES,
            id="refresh_gdelt_gkg_events",
        )
        scheduler.add_job(
            run_scheduled_story_materialization,
            "interval",
            minutes=STORY_MATERIALIZATION_REFRESH_MINUTES,
            id="materialize_story_clusters",
        )
    if include_translations:
        scheduler.add_job(
            refresh_recent_translations,
            "interval",
            minutes=30,
            id="refresh_translations",
        )
    if include_analytics:
        scheduler.add_job(
            refresh_source_reliability,
            "interval",
            minutes=SOURCE_RELIABILITY_REFRESH_MINUTES,
            id="refresh_source_reliability",
        )
        scheduler.add_job(
            refresh_foresight_layer,
            "interval",
            minutes=FORESIGHT_REFRESH_MINUTES,
            id="refresh_foresight_layer",
        )
        scheduler.add_job(
            refresh_narrative_drift_layer,
            "interval",
            minutes=NARRATIVE_DRIFT_REFRESH_MINUTES,
            id="refresh_narrative_drift_layer",
        )
        scheduler.add_job(
            refresh_snapshot_layer, "interval", hours=1, id="refresh_snapshots"
        )

    global _scheduler
    _scheduler = scheduler
    return scheduler


def build_worker_scheduler() -> BackgroundScheduler:
    return build_scheduler(
        include_ingestion=WORKER_ENABLE_INGESTION,
        include_translations=WORKER_ENABLE_TRANSLATIONS,
        include_analytics=WORKER_ENABLE_ANALYTICS,
    )


def schedule_initial_analytics_warm(scheduler: BackgroundScheduler) -> None:
    if not INTERNAL_SCHEDULER_ENABLED:
        return

    from services.ingest_service import (
        refresh_acled_events,
        refresh_gdelt_gkg_events,
        refresh_source_reliability,
        refresh_foresight_layer,
        refresh_narrative_drift_layer,
    )
    from services.briefing_service import refresh_snapshot_layer

    warm_jobs = [
        (
            "refresh_snapshots_initial",
            refresh_snapshot_layer,
            ANALYTICS_WARM_DELAY_SECONDS,
        ),
        (
            "refresh_source_reliability_initial",
            refresh_source_reliability,
            ANALYTICS_WARM_DELAY_SECONDS + 30,
        ),
        (
            "refresh_foresight_initial",
            refresh_foresight_layer,
            ANALYTICS_WARM_DELAY_SECONDS + 60,
        ),
        (
            "refresh_narrative_drift_initial",
            refresh_narrative_drift_layer,
            ANALYTICS_WARM_DELAY_SECONDS + 90,
        ),
        (
            "refresh_acled_initial",
            refresh_acled_events,
            ANALYTICS_WARM_DELAY_SECONDS + 120,
        ),
        (
            "refresh_gdelt_gkg_initial",
            refresh_gdelt_gkg_events,
            ANALYTICS_WARM_DELAY_SECONDS + 150,
        ),
    ]
    for job_id, job_func, delay_seconds in warm_jobs:
        scheduler.add_job(
            job_func,
            "date",
            run_date=datetime.now(timezone.utc)
            + timedelta(seconds=max(0, delay_seconds)),
            id=job_id,
            replace_existing=True,
        )
