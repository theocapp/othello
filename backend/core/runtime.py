import os
import time
from datetime import datetime, timedelta, timezone

from cache import load_briefing
from corpus import (
    get_article_count,
    get_ingestion_summary,
    get_recent_articles,
    load_ingestion_state,
)
from entities import get_entity_model_capabilities
from news import source_status

from core.config import BRIEFING_TTL, CORPUS_WINDOW_HOURS, TOPICS


def runtime_status() -> dict:
    llm_ready = bool(os.getenv("GROQ_API_KEY"))
    contradiction_ready = bool(os.getenv("ANTHROPIC_API_KEY"))
    sources = source_status()
    corpus = get_ingestion_summary()
    return {
        "llm_ready": llm_ready,
        "contradiction_ready": contradiction_ready,
        "sources": sources,
        "corpus": corpus,
        "entity_models": get_entity_model_capabilities(),
        "ready": corpus["total_articles"] > 0 or sources["gdelt"]["enabled"] or sources["directfeeds"]["enabled"],
    }


def topic_counts(hours: int = 72) -> dict[str, int]:
    return {topic: get_article_count(topic=topic, hours=hours) for topic in TOPICS}


def topic_summary(topic: str) -> dict:
    cached = load_briefing(topic, ttl=BRIEFING_TTL)
    recent_articles = get_recent_articles(topic=topic, limit=1, hours=CORPUS_WINDOW_HOURS)
    latest_article = recent_articles[0] if recent_articles else None
    return {
        "topic": topic,
        "corpus_articles_72h": get_article_count(topic=topic, hours=72),
        "briefing_ready": cached is not None,
        "briefing_age_minutes": int((time.time() - cached["generated_at"]) / 60) if cached else None,
        "briefing_event_count": len(cached.get("events", [])) if cached else 0,
        "latest_article_title": latest_article.get("title") if latest_article else None,
        "latest_published_at": latest_article.get("published_at") if latest_article else None,
    }


def latest_entity_telemetry() -> dict:
    tracked_state_keys = [
        "analytic-ingest-global",
        "analytic-ingest-geopolitics",
        "analytic-ingest-economics",
        "analytic-ingest-fallback",
    ]
    latest = None
    for state_key in tracked_state_keys:
        state = load_ingestion_state(state_key)
        if not state:
            continue
        payload = state.get("payload") or {}
        if "entity_extraction" not in payload:
            continue
        updated_at = state.get("updated_at") or 0
        if latest is None or updated_at > latest.get("updated_at", 0):
            latest = {
                "state_key": state_key,
                "topic": state.get("topic"),
                "provider": state.get("provider"),
                "status": state.get("status"),
                "updated_at": updated_at,
                "entity_extraction": payload.get("entity_extraction"),
            }
    return latest or {}


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in ("%Y%m%dT%H%M%S%z", "%Y%m%dT%H%M%SZ", "%Y-%m-%d %H:%M:%S%z"):
        try:
            parsed = datetime.strptime(value, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return None
