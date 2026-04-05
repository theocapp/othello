from fastapi import HTTPException

from core.config import INTERNAL_SCHEDULER_ENABLED, TOPICS
from core.runtime import latest_entity_telemetry, runtime_status, topic_counts, topic_summary


def get_root_payload():
    from chroma import get_collection_stats
    return {"status": "Othello V2 API is running", "runtime": runtime_status(), "collection": get_collection_stats()}


def get_health_payload():
    from corpus import get_source_registry, get_warehouse_counts, load_ingestion_state
    from entities import get_entity_model_capabilities
    from core.scheduler import get_scheduler
    scheduler = get_scheduler()
    promotion_states = {
        "analytic_global": load_ingestion_state("analytic-ingest-global"),
        "analytic_geopolitics": load_ingestion_state("analytic-ingest-geopolitics"),
        "analytic_economics": load_ingestion_state("analytic-ingest-economics"),
        "analytic_fallback": load_ingestion_state("analytic-ingest-fallback"),
        "direct_feed_layer_refresh": load_ingestion_state("direct-feed-layer-refresh"),
        "source_registry_refresh": load_ingestion_state("source-registry-refresh"),
        "source_registry_mirror": load_ingestion_state("source-registry-mirror"),
        "official_updates_refresh": load_ingestion_state("official-updates-refresh"),
    }
    return {
        "runtime": runtime_status(),
        "scheduler_running": scheduler.running if scheduler else False,
        "internal_scheduler_enabled": INTERNAL_SCHEDULER_ENABLED,
        "topic_counts": topic_counts(),
        "registry_sources": len(get_source_registry()),
        "warehouse": get_warehouse_counts(),
        "promotion_pipeline": promotion_states,
        "analytics_pipeline": {
            "structured_events": load_ingestion_state("structured-events-refresh"),
            "source_reliability": load_ingestion_state("source-reliability-refresh"),
            "foresight": load_ingestion_state("foresight-layer-refresh"),
            "narrative_drift": load_ingestion_state("narrative-drift-refresh"),
        },
        "entity_pipeline": {
            "installed_models": get_entity_model_capabilities(),
            "latest_usage": latest_entity_telemetry(),
        },
    }


def get_system_overview_payload():
    from chroma import get_collection_stats
    from corpus import get_source_registry, get_sources, get_warehouse_counts
    from datetime import datetime, timezone
    return {
        "runtime": runtime_status(),
        "sources": get_sources(),
        "source_registry": get_source_registry(),
        "warehouse": get_warehouse_counts(),
        "topic_counts": topic_counts(),
        "topics": [topic_summary(topic) for topic in TOPICS],
        "collection": get_collection_stats(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def get_instability_payload(days: int = 3):
    from country_instability import compute_country_instability
    from tiered_cache import TTL_MEDIUM, cache
    return cache.get(f"cii:{days}", lambda: compute_country_instability(days=days), ttl=TTL_MEDIUM)


def get_instability_detail_payload(country: str, days: int = 3):
    result = get_instability_payload(days)
    normalized = country.strip().lower()
    for entry in result.get("countries", []):
        if entry["country"] == normalized or (entry.get("label") or "").lower() == normalized:
            return entry
    raise HTTPException(status_code=404, detail=f"No instability data for '{country}'")


def get_correlations_payload(days: int = 3):
    from correlation_engine import compute_correlations
    return compute_correlations(days=days)


def source_reliability_payload(topic: str | None = None, days: int = 180, refresh: bool = False):
    from claim_resolution import get_source_reliability
    return get_source_reliability(topic=topic, days=max(30, min(days, 365)), refresh=refresh)


def narrative_drift_payload(subject: str, topic: str | None = None, days: int = 180, refresh: bool = False):
    from narrative_drift import analyze_narrative_drift
    return analyze_narrative_drift(subject, topic=topic, days=max(14, min(days, 365)), refresh=refresh)
