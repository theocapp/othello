import signal
import sys
import time

from main import (
    build_worker_scheduler,
    bootstrap_from_legacy_cache,
    ingest_all_topics,
    init_cache_db,
    init_corpus_db,
    refresh_acled_events,
    refresh_direct_feed_layer,
    refresh_recent_translations,
    refresh_official_updates,
    run_incremental_gdelt_backfill,
    runtime_status,
    seed_sources,
    sync_registry_mirror,
    WORKER_BOOTSTRAP_MODE,
    WORKER_ENABLE_INGESTION,
    WORKER_ENABLE_TRANSLATIONS,
)


def initialize_worker_state() -> None:
    init_cache_db()
    init_corpus_db()
    seed_sources()
    state = runtime_status()
    if state["corpus"]["total_articles"] == 0:
        bootstrap_from_legacy_cache()


def main() -> int:
    initialize_worker_state()

    # Keep launch memory predictable: do the minimum needed to keep ingestion warm,
    # and let the scheduler handle the rest over time.
    bootstrap_jobs = []
    if WORKER_ENABLE_INGESTION and WORKER_BOOTSTRAP_MODE in {"ingest", "full"}:
        bootstrap_jobs.extend(
            [
                ("ingest_all_topics", ingest_all_topics),
                ("gdelt_backfill", run_incremental_gdelt_backfill),
                ("refresh_direct_feed_layer", refresh_direct_feed_layer),
            ]
        )
    if WORKER_ENABLE_INGESTION and WORKER_BOOTSTRAP_MODE == "full":
        bootstrap_jobs.extend(
            [
                ("sync_registry_mirror", sync_registry_mirror),
                ("refresh_official_updates", refresh_official_updates),
                ("refresh_acled_events", refresh_acled_events),
            ]
        )
        if WORKER_ENABLE_TRANSLATIONS:
            bootstrap_jobs.append(("refresh_recent_translations", refresh_recent_translations))

    for label, job in bootstrap_jobs:
        try:
            result = job()
            print(f"[worker] Startup job '{label}' completed: {result}")
        except Exception as exc:
            print(f"[worker] Startup job '{label}' failed: {exc}")

    scheduler = build_worker_scheduler()
    scheduler.start()
    print(
        "[worker] Othello worker started "
        f"(ingestion={WORKER_ENABLE_INGESTION}, translations={WORKER_ENABLE_TRANSLATIONS}, bootstrap={WORKER_BOOTSTRAP_MODE})"
    )

    def handle_shutdown(signum, frame):
        print(f"[worker] Shutting down on signal {signum}")
        if scheduler.running:
            scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        while True:
            time.sleep(30)
    except KeyboardInterrupt:
        handle_shutdown(signal.SIGINT, None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
