from fastapi import HTTPException

from core.config import TOPICS
from corpus import get_canonical_event, get_canonical_events, get_event_perspectives, load_materialized_story_clusters
from services.headlines_service import _build_global_events, _build_topic_events
from structured_story_rollups import build_structured_story_clusters


def get_events_payload(limit: int = 12) -> dict:
    safe_limit = max(limit, 1)
    events = _build_global_events(limit=safe_limit)
    return {"events": events[:safe_limit], "count": len(events)}


def get_structured_events_payload(
    days: int = 3,
    limit: int = 12,
    country: str | None = None,
    event_type: str | None = None,
) -> dict:
    safe_days = max(1, min(days, 30))
    safe_limit = max(1, min(limit, 30))
    clusters = build_structured_story_clusters(
        days=safe_days,
        limit=safe_limit,
        country=country,
        event_type=event_type,
    )
    return {
        "dataset": "acled",
        "days": safe_days,
        "country": country,
        "event_type": event_type,
        "clusters": clusters,
        "count": len(clusters),
    }


def get_materialized_story_clusters_payload(
    topic: str | None = None,
    window_hours: int | None = None,
    limit: int = 40,
) -> dict:
    wh = int(window_hours) if window_hours is not None else None
    rows = load_materialized_story_clusters(
        topic=topic,
        window_hours=wh,
        limit=max(1, min(limit, 200)),
    )
    return {"topic": topic, "window_hours": wh, "clusters": rows, "count": len(rows)}


def get_topic_events_payload(topic: str, limit: int = 8) -> dict:
    if topic not in TOPICS:
        raise HTTPException(status_code=400, detail=f"Topic must be one of {TOPICS}")
    safe_limit = max(limit, 1)
    events = _build_topic_events(topic, limit=safe_limit)
    return {"topic": topic, "events": events[:safe_limit], "count": len(events)}


def get_canonical_events_payload(
    topic: str | None = None,
    status: str | None = None,
    limit: int = 40,
) -> dict:
    events = get_canonical_events(
        topic=topic,
        status=status,
        limit=max(1, min(limit, 200)),
    )
    return {"topic": topic, "status": status, "events": events, "count": len(events)}


def get_canonical_event_payload(event_id: str) -> dict:
    event = get_canonical_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    perspectives = get_event_perspectives(event_id)
    # Summarize perspective diversity for the response
    frames = [p["dominant_frame"] for p in perspectives if p.get("dominant_frame")]
    frame_distribution: dict[str, int] = {}
    for f in frames:
        frame_distribution[f] = frame_distribution.get(f, 0) + 1
    sources_agreeing = [p["source_name"] for p in perspectives if p.get("claim_resolution_status") == "corroborated"]
    sources_dissenting = [p["source_name"] for p in perspectives if p.get("claim_resolution_status") == "contradicted"]
    return {
        **event,
        "perspectives": perspectives,
        "frame_distribution": frame_distribution,
        "sources_agreeing": sources_agreeing,
        "sources_dissenting": sources_dissenting,
    }


def get_event_perspectives_payload(event_id: str) -> dict:
    event = get_canonical_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    perspectives = get_event_perspectives(event_id)
    return {
        "event_id": event_id,
        "event_label": event["label"],
        "topic": event["topic"],
        "perspectives": perspectives,
        "count": len(perspectives),
    }
