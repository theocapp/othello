from fastapi import HTTPException


def trigger_ingest_payload(topic: str | None = None):
    from main import INGEST_JOB_LOCK, TOPICS, _run_exclusive, ingest_all_topics, ingest_topic
    def run():
        if topic:
            if topic not in TOPICS:
                raise HTTPException(status_code=400, detail=f"Topic must be one of {TOPICS}")
            return {"results": [ingest_topic(topic)]}
        return {"results": ingest_all_topics()}
    return _run_exclusive(INGEST_JOB_LOCK, "Ingest", run)


def trigger_backfill_payload(topic: str | None = None):
    from main import BACKFILL_JOB_LOCK, TOPICS, _run_exclusive, run_incremental_gdelt_backfill
    def run():
        if topic:
            if topic not in TOPICS:
                raise HTTPException(status_code=400, detail=f"Topic must be one of {TOPICS}")
            return {"results": run_incremental_gdelt_backfill([topic])}
        return {"results": run_incremental_gdelt_backfill()}
    return _run_exclusive(BACKFILL_JOB_LOCK, "GDELT backfill", run)


def source_registry_payload():
    from source_ingestion import registry_sources_with_feed_status
    sources = registry_sources_with_feed_status()
    return {"count": len(sources), "sources": sources}


def source_refresh_payload():
    from main import refresh_registry_sources, sync_registry_mirror
    return {"refresh": refresh_registry_sources(), "mirror": sync_registry_mirror()}


def official_refresh_payload():
    from main import refresh_official_updates
    return refresh_official_updates()


def acled_refresh_payload():
    from main import refresh_acled_events
    return refresh_acled_events()


def gdelt_gkg_refresh_payload():
    from main import refresh_gdelt_gkg_events
    return refresh_gdelt_gkg_events()


def trigger_local_seed_payload():
    from main import seed_local_corpus
    return seed_local_corpus()


def source_probe_payload(query: str = "Iran OR Israel OR war", page_size: int = 10):
    from news import probe_sources
    return probe_sources(query, page_size=max(1, min(page_size, 25)))
