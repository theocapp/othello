from fastapi import APIRouter

from services.analytics_service import get_correlations_payload, get_instability_detail_payload, get_instability_payload
from services.map_service import get_hotspot_attention_map_payload, get_region_attention_payload

router = APIRouter()


@router.get("/events")
def get_events(limit: int = 12):
    from main import _build_global_events
    events = _build_global_events(limit=max(limit, 1))
    return {"events": events[:limit], "count": len(events)}


@router.get("/events/structured")
def get_structured_events(days: int = 3, limit: int = 12, country: str | None = None, event_type: str | None = None):
    from structured_story_rollups import build_structured_story_clusters
    clusters = build_structured_story_clusters(days=max(1, min(days, 30)), limit=max(1, min(limit, 30)), country=country, event_type=event_type)
    return {"dataset": "acled", "days": max(1, min(days, 30)), "country": country, "event_type": event_type, "clusters": clusters, "count": len(clusters)}


@router.get("/coverage/regions")
def get_region_attention(window: str = "24h"):
    return get_region_attention_payload(window)


@router.get("/coverage/map")
def get_hotspot_attention_map(window: str = "24h"):
    return get_hotspot_attention_map_payload(window)


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
def get_materialized_story_clusters(topic: str | None = None, window_hours: int | None = None, limit: int = 40):
    from corpus import load_materialized_story_clusters
    wh = int(window_hours) if window_hours is not None else None
    rows = load_materialized_story_clusters(topic=topic, window_hours=wh, limit=max(1, min(limit, 200)))
    return {"topic": topic, "window_hours": wh, "clusters": rows, "count": len(rows)}


@router.get("/events/{topic}")
def get_events_for_topic(topic: str, limit: int = 8):
    from main import TOPICS, _build_topic_events
    from fastapi import HTTPException
    if topic not in TOPICS:
        raise HTTPException(status_code=400, detail=f"Topic must be one of {TOPICS}")
    events = _build_topic_events(topic, limit=max(limit, 1))
    return {"topic": topic, "events": events[:limit], "count": len(events)}
