from fastapi import APIRouter, Depends

from api.deps import require_write_access_dep
from services.analytics_service import (
    evaluation_scorecard_payload,
    narrative_drift_payload,
    source_reliability_payload,
)
from services.article_event_pipeline import populate_canonical_events_from_articles
from services.ingest_service import (
    acled_refresh_payload,
    gdelt_gkg_refresh_payload,
    official_refresh_payload,
    source_probe_payload,
    source_refresh_payload,
    source_registry_payload,
    trigger_backfill_payload,
    trigger_ingest_payload,
    trigger_local_seed_payload,
)

router = APIRouter()


@router.post("/ingest")
def trigger_ingest(
    topic: str | None = None, _: None = Depends(require_write_access_dep)
):
    return trigger_ingest_payload(topic)


@router.post("/ingest/backfill")
def trigger_gdelt_backfill(
    topic: str | None = None, _: None = Depends(require_write_access_dep)
):
    return trigger_backfill_payload(topic)


@router.post("/ingest/acled")
def trigger_acled_ingest(_: None = Depends(require_write_access_dep)):
    from ingestion.acled_ingestion import ingest_acled_recent
    from services.canonical_events_pipeline import populate_canonical_events

    result = ingest_acled_recent()
    canonical = populate_canonical_events(days=7, limit=500)
    return {"acled": result, "canonical": canonical}


@router.post("/ingest/canonical")
def trigger_canonical_refresh(_: None = Depends(require_write_access_dep)):
    from services.canonical_events_pipeline import populate_canonical_events

    return populate_canonical_events(days=7, limit=500)


@router.post("/ingest/article-events")
def trigger_article_event_pipeline(
    days: int = 3, limit: int = 2000, _: None = Depends(require_write_access_dep)
):
    return populate_canonical_events_from_articles(days=days, limit=limit)


@router.get("/sources/registry")
def source_registry():
    return source_registry_payload()


@router.post("/sources/refresh")
def source_refresh(_: None = Depends(require_write_access_dep)):
    return source_refresh_payload()


@router.post("/official/refresh")
def official_refresh(_: None = Depends(require_write_access_dep)):
    return official_refresh_payload()


@router.post("/acled/refresh")
def acled_refresh(_: None = Depends(require_write_access_dep)):
    return acled_refresh_payload()


@router.post("/gdelt-gkg/refresh")
def gdelt_gkg_refresh(_: None = Depends(require_write_access_dep)):
    return gdelt_gkg_refresh_payload()


@router.post("/seed/local")
def trigger_local_seed(_: None = Depends(require_write_access_dep)):
    return trigger_local_seed_payload()


@router.get("/sources/probe")
def sources_probe(query: str = "Iran OR Israel OR war", page_size: int = 10):
    return source_probe_payload(query, page_size)


@router.get("/sources/reliability")
def source_reliability(
    topic: str | None = None, days: int = 180, refresh: bool = False
):
    return source_reliability_payload(topic, days, refresh)


@router.get("/narratives/drift/{subject}")
def narrative_drift(
    subject: str, topic: str | None = None, days: int = 180, refresh: bool = False
):
    return narrative_drift_payload(subject, topic, days, refresh)


@router.get("/evaluation/scorecard")
def evaluation_scorecard(
    kind: str | None = None,
    topic: str | None = None,
    limit_files: int = 80,
    include_error_samples: bool = False,
):
    return evaluation_scorecard_payload(
        kind=kind,
        topic=topic,
        limit_files=limit_files,
        include_error_samples=include_error_samples,
    )
