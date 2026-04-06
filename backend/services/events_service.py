from fastapi import HTTPException

from core.config import TOPICS
from corpus import load_materialized_story_clusters
from services.headlines_service import _build_global_events, _build_topic_events
from structured_story_rollups import build_structured_story_clusters


def _bucket_structured_clusters(clusters: list[dict]) -> dict:
    top = []
    contested = []
    radar = []
    for cluster in clusters:
        status = (cluster.get("status") or "").strip().lower()
        importance = float(cluster.get("importance_score", 0) or 0)
        confidence = float(cluster.get("confidence_score", 0) or 0)
        divergence = float(cluster.get("narrative_divergence_score", 0) or 0)
        if importance >= 72 and confidence >= 58:
            top.append(cluster)
        elif status == "contested" or divergence >= 34:
            contested.append(cluster)
        else:
            radar.append(cluster)
    return {"top": top, "contested": contested, "radar": radar}



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


def get_event_intelligence_payload(
    days: int = 3,
    limit: int = 24,
    country: str | None = None,
    event_type: str | None = None,
) -> dict:
    safe_days = max(1, min(days, 30))
    safe_limit = max(1, min(limit, 60))
    clusters = build_structured_story_clusters(
        days=safe_days,
        limit=safe_limit,
        country=country,
        event_type=event_type,
    )
    buckets = _bucket_structured_clusters(clusters)
    return {
        "dataset": "acled",
        "days": safe_days,
        "country": country,
        "event_type": event_type,
        "count": len(clusters),
        "clusters": clusters,
        "top": buckets["top"],
        "contested": buckets["contested"],
        "radar": buckets["radar"],
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
