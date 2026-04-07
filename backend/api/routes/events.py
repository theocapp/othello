from fastapi import APIRouter

from services.analytics_service import (
    get_correlations_payload,
    get_instability_detail_payload,
    get_instability_payload,
)
from services.events_service import (
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


@router.get("/events")
def get_events(limit: int = 12):
    return get_events_payload(limit=limit)


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


@router.get("/events/canonical/{event_id}/perspectives")
def get_event_perspectives_route(event_id: str):
    return get_event_perspectives_payload(event_id)


@router.get("/events/canonical/{event_id}")
def get_canonical_event_route(event_id: str):
    return get_canonical_event_payload(event_id)


@router.get("/events/{topic}")
def get_events_for_topic(topic: str, limit: int = 8):
    return get_topic_events_payload(topic, limit=limit)
