import time
from fastapi import APIRouter
from pydantic import BaseModel

from db.common import _connect
from services.analytics_service import (
    get_correlations_payload,
    get_instability_detail_payload,
    get_instability_payload,
)
from services.events_service import (
    get_canonical_map_payload,
    get_canonical_event_debug_payload,
    get_canonical_event_payload,
    get_canonical_events_payload,
    get_event_perspectives_payload,
    get_events_payload,
    get_materialized_story_clusters_payload,
    get_structured_events_payload,
    get_topic_events_payload,
)
from services.map_service import (
    get_hotspot_attention_map_payload,
    get_region_attention_payload,
)

router = APIRouter()


# ── Request models for analyst corrections ────────────────────────
class MergeEventRequest(BaseModel):
    merge_with_event_id: str


class SplitArticleRequest(BaseModel):
    article_url: str


@router.post("/events/{event_id}/merge")
def merge_events(event_id: str, body: MergeEventRequest):
    """Queue a merge correction: combine event_id and body.merge_with_event_id."""
    with _connect() as conn:
        conn.execute("""
            INSERT INTO analyst_corrections (correction_type, event_a_id, event_b_id, created_at, applied)
            VALUES (%s, %s, %s, %s, %s)
        """, ("merge", event_id, body.merge_with_event_id, time.time(), False))
    return {"status": "correction_queued"}


@router.post("/events/{event_id}/split-article")
def split_article(event_id: str, body: SplitArticleRequest):
    """Queue a split correction: remove article_url from event and create standalone event."""
    with _connect() as conn:
        conn.execute("""
            INSERT INTO analyst_corrections (correction_type, event_a_id, article_url, created_at, applied)
            VALUES (%s, %s, %s, %s, %s)
        """, ("split", event_id, body.article_url, time.time(), False))
    return {"status": "correction_queued"}


@router.get("/events")
def get_events(limit: int = 12, include_factual: bool = False):
    payload = get_events_payload(limit=limit)
    filtered_events = []
    for event in payload.get("events", []):
        all_contradictions = event.get("contradictions") or []
        contradictions = (
            all_contradictions
            if include_factual
            else [
                c
                for c in all_contradictions
                if c.get("contradiction_class") != "factual_claim"
            ]
        )
        filtered_events.append(
            {
                **event,
                "contradictions": contradictions,
                "contradiction_count": len(contradictions),
            }
        )
    return {
        **payload,
        "events": filtered_events,
        "count": len(filtered_events),
    }


@router.get("/events/structured")
def get_structured_events(
    days: int = 3,
    limit: int = 12,
    country: str | None = None,
    event_type: str | None = None,
):
    return get_structured_events_payload(
        days=days, limit=limit, country=country, event_type=event_type
    )


@router.get("/coverage/regions")
def get_region_attention(window: str = "24h"):
    return get_region_attention_payload(window)


@router.get("/coverage/map")
def get_hotspot_attention_map(window: str = "24h", start: str | None = None, end: str | None = None, days: int | None = None):
    return get_hotspot_attention_map_payload(window, start=start, end=end, days=days)


@router.get("/instability")
def get_country_instability(days: int = 3):
    return get_instability_payload(days)


@router.get("/instability/{country}")
def get_country_instability_detail(country: str, days: int = 3):
    return get_instability_detail_payload(country, days)


@router.get("/correlations")
def get_correlations(days: int = 3):
    return get_correlations_payload(days)


@router.get("/events/materialized")
def get_materialized_story_clusters(
    topic: str | None = None, window_hours: int | None = None, limit: int = 40
):
    return get_materialized_story_clusters_payload(
        topic=topic, window_hours=window_hours, limit=limit
    )


@router.get("/events/canonical")
def get_canonical_events_route(
    topic: str | None = None, status: str | None = None, limit: int = 40
):
    return get_canonical_events_payload(topic=topic, status=status, limit=limit)


@router.get("/events/canonical/map")
def get_canonical_events_map_route(days: int = 7, limit: int = 500):
    return get_canonical_map_payload(days=days, limit=limit)


@router.get("/events/canonical/{event_id}/perspectives")
def get_event_perspectives_route(event_id: str):
    return get_event_perspectives_payload(event_id)


@router.get("/events/canonical/{event_id}")
def get_canonical_event_route(event_id: str):
    return get_canonical_event_payload(event_id)


@router.get("/events/canonical/{event_id}/debug")
def get_canonical_event_debug_route(event_id: str):
    return get_canonical_event_debug_payload(event_id)


@router.get("/events/{topic}")
def get_events_for_topic(topic: str, limit: int = 8):
    return get_topic_events_payload(topic, limit=limit)
