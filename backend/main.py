import os
import re
import sqlite3
import time
import hashlib
import ipaddress
import math
from collections import defaultdict
from threading import Lock
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from analyst import answer_query, build_headlines_from_events, build_timeline, generate_briefing, translate_article
from cache import (
    clear_headlines,
    init_db as init_cache_db,
    load_briefing,
    load_headlines,
    save_briefing,
    save_headlines,
)
from chroma import get_collection_stats, search_articles, store_articles
from bootstrap_sources import seed_sources
from contradictions import cluster_articles, enrich_events, format_contradictions_for_briefing, format_event_brief
from corpus import (
    get_article_count,
    get_articles_by_urls,
    get_articles_with_regions,
    get_ingestion_summary,
    get_recent_structured_events,
    get_structured_event_coordinates_by_ids,
    get_topic_time_bounds,
    get_recent_articles,
    get_warehouse_counts,
    get_source_registry,
    load_materialized_story_clusters,
    search_recent_articles_by_keywords,
    delete_prediction_records,
    get_sources,
    init_db as init_corpus_db,
    load_ingestion_state,
    get_articles_missing_translation,
    load_entity_reference,
    record_ingestion_run,
    save_article_translation,
    save_entity_reference,
    save_ingestion_state,
    upsert_article_summaries,
    upsert_prediction_records,
    upsert_articles,
)
from entities import (
    extract_entities,
    get_entity_model_capabilities,
    format_signals_for_briefing,
    get_entity_frequencies,
    get_relationship_graph,
    get_top_entities,
    get_entity_relationships,
    store_entity_mentions,
)
from news import fetch_articles, fetch_articles_for_query, probe_sources, source_status
from news import article_quality_score, fetch_articles_from_provider, fetch_global_articles, fetch_global_articles_from_provider, fetch_gdelt_historic_articles, infer_article_topics, is_english_article, normalize_article_description, normalize_article_title, should_promote_article
from official_ingestion import ingest_official_updates
from source_ingestion import archive_provider_articles, ingest_direct_feed_layer, ingest_registry_sources, mirror_corpus_articles_into_registry, registry_sources_with_feed_status
from acled_ingestion import ingest_acled_recent
from gdelt_gkg_ingestion import ingest_gdelt_gkg_recent
from structured_story_rollups import build_map_structured_story_clusters, build_structured_story_clusters
from claim_resolution import build_claim_resolution_snapshot, get_source_reliability
from narrative_drift import analyze_narrative_drift
from wikipedia_reference import fetch_wikipedia_reference
from foresight import extract_predictions_from_briefing, load_early_signal_archive, load_prediction_ledger, observe_events
from fetch_historical_queue import fetch_historical_queue
from geo_constants import COUNTRY_CENTROIDS, STORY_REGION_CENTROIDS
from story_materialization import rebuild_materialized_story_clusters
from country_instability import compute_country_instability
from correlation_engine import compute_correlations
from tiered_cache import cache as tiered_cache, TTL_FAST, TTL_MEDIUM, TTL_SLOW
from api.routes.analytics import router as analytics_router
from api.routes.briefings import router as briefings_router
from api.routes.entities import router as entities_router
from api.routes.events import router as events_router
from api.routes.headlines import router as headlines_router
from api.routes.health import router as health_router
from api.routes.query import router as query_router

load_dotenv(Path(__file__).with_name(".env"), override=True)


def _split_csv_env(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


DEFAULT_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:5175",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5174",
    "http://127.0.0.1:5175",
    "http://127.0.0.1:5176",
]
CORS_ORIGINS = _split_csv_env(os.getenv("OTHELLO_CORS_ORIGINS")) or DEFAULT_CORS_ORIGINS

app = FastAPI(title="Othello V2 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

TOPICS = ["geopolitics", "economics"]
BRIEFING_TOPICS = ["geopolitics", "economics", "conflict"]
BRIEFING_TTL = 3600


def _corpus_topic_for_briefing(briefing_topic: str) -> str:
    """Conflict briefings use the geopolitics article corpus (no separate conflict topic lane)."""
    if briefing_topic == "conflict":
        return "geopolitics"
    return briefing_topic
HEADLINES_TTL = 1200
CORPUS_WINDOW_HOURS = 96
MIN_TOPIC_ARTICLES = 12
ATTENTION_WINDOW_HOURS = {
    "24h": 24,
    "7d": 24 * 7,
    "30d": 24 * 30,
    "90d": 24 * 90,
    "180d": 24 * 180,
    "365d": 24 * 365,
    "1825d": 24 * 365 * 5,
}
MAP_CACHE_TTL_SECONDS = 600
_MAP_ATTENTION_CACHE: dict[str, tuple[float, dict]] = {}
MAX_MAP_STRUCTURED_DAYS = 60
MAX_MAP_STRUCTURED_LIMIT = 3000
MAX_MAP_STORY_HOURS = 24 * 90
CONFLICT_TEXT_PATTERNS = (
    "airstrike", "missile", "drone", "shelling", "artillery", "troops",
    "battle", "fighting", "attack", "offensive", "raid", "explosion",
    "bomb", "clash", "frontline", "insurgent", "rocket fire", "drone strike",
    "missile strike", "military operation", "killed in", "struck",
)
POLITICAL_TEXT_PATTERNS = (
    "election", "government", "parliament", "diplom", "minister", "president",
    "policy", "sanction", "coalition", "cabinet", "vote", "protest",
)
ECONOMIC_TEXT_PATTERNS = (
    "market", "econom", "trade", "tariff", "inflation", "gdp", "oil", "gas",
    "supply chain", "rate", "bank", "currency", "debt", "investment",
)
_STORY_LOCATION_INDEX_CACHE: dict[int, tuple[float, dict[str, dict]]] = {}
DATELINE_RE = re.compile(
    r"^\s*(?:[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){0,4}|[A-Z]{2,}(?:\s+[A-Z]{2,}){0,4})(?:,\s*[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){0,3})?\s*\((?:AP|Reuters|AFP)\)\s*[—-]\s*"
)
HOTSPOT_EVENT_TYPE_WEIGHTS = {
    "Battles": 4.8,
    "Violence against civilians": 4.4,
    "Explosions/Remote violence": 4.1,
    "Riots": 2.8,
    "Strategic developments": 2.4,
    "Protests": 1.9,
}
GDELT_BACKFILL_WINDOW_HOURS = int(os.getenv("OTHELLO_GDELT_BACKFILL_WINDOW_HOURS", "3"))
GDELT_BACKFILL_LAG_MINUTES = int(os.getenv("OTHELLO_GDELT_BACKFILL_LAG_MINUTES", "90"))
GDELT_BACKFILL_PAGE_SIZE = int(os.getenv("OTHELLO_GDELT_BACKFILL_PAGE_SIZE", "12"))
GDELT_BACKFILL_MIN_WINDOW_HOURS = int(os.getenv("OTHELLO_GDELT_BACKFILL_MIN_WINDOW_HOURS", "1"))
GDELT_BACKFILL_CHROMA = os.getenv("OTHELLO_GDELT_BACKFILL_CHROMA", "false").lower() == "true"
GDELT_BACKFILL_START = os.getenv("OTHELLO_GDELT_BACKFILL_START")
GDELT_BACKFILL_RETRY_MINUTES = int(os.getenv("OTHELLO_GDELT_BACKFILL_RETRY_MINUTES", "45"))
GDELT_BACKFILL_RATE_LIMIT_RETRY_MINUTES = int(os.getenv("OTHELLO_GDELT_BACKFILL_RATE_LIMIT_RETRY_MINUTES", "180"))
SOURCE_REGISTRY_REFRESH_MINUTES = int(os.getenv("OTHELLO_SOURCE_REGISTRY_REFRESH_MINUTES", "60"))
DIRECT_FEED_REFRESH_MINUTES = int(os.getenv("OTHELLO_DIRECT_FEED_REFRESH_MINUTES", "20"))
SOURCE_REGISTRY_MIRROR_HOURS = int(os.getenv("OTHELLO_SOURCE_REGISTRY_MIRROR_HOURS", "336"))
OFFICIAL_UPDATE_REFRESH_MINUTES = int(os.getenv("OTHELLO_OFFICIAL_UPDATE_REFRESH_MINUTES", "180"))
ACLED_REFRESH_MINUTES = int(os.getenv("OTHELLO_ACLED_REFRESH_MINUTES", "240"))
GDELT_GKG_REFRESH_MINUTES = int(os.getenv("OTHELLO_GDELT_GKG_REFRESH_MINUTES", "60"))
GDELT_GKG_REFRESH_HOURS = int(os.getenv("OTHELLO_GDELT_GKG_REFRESH_HOURS", "24"))
ARTICLE_FALLBACK_REFRESH_MINUTES = int(os.getenv("OTHELLO_ARTICLE_FALLBACK_REFRESH_MINUTES", "45"))
SOURCE_RELIABILITY_REFRESH_MINUTES = int(os.getenv("OTHELLO_SOURCE_RELIABILITY_REFRESH_MINUTES", "120"))
FORESIGHT_REFRESH_MINUTES = int(os.getenv("OTHELLO_FORESIGHT_REFRESH_MINUTES", "180"))
NARRATIVE_DRIFT_REFRESH_MINUTES = int(os.getenv("OTHELLO_NARRATIVE_DRIFT_REFRESH_MINUTES", "180"))
NARRATIVE_DRIFT_TOP_SUBJECTS = int(os.getenv("OTHELLO_NARRATIVE_DRIFT_TOP_SUBJECTS", "3"))
ANALYTICS_WARM_DELAY_SECONDS = int(os.getenv("OTHELLO_ANALYTICS_WARM_DELAY_SECONDS", "5"))
REQUEST_ENABLE_VECTOR_SEARCH = os.getenv("OTHELLO_ENABLE_VECTOR_SEARCH", "false").lower() == "true"
REQUEST_ENABLE_CHROMA_INGEST = os.getenv("OTHELLO_ENABLE_CHROMA_INGEST", "false").lower() == "true"
REQUEST_ENABLE_LIVE_FETCH = os.getenv("OTHELLO_ENABLE_LIVE_FETCH", "false").lower() == "true"
REQUEST_ENABLE_LLM_RESPONSES = os.getenv("OTHELLO_ENABLE_LLM_RESPONSES", "false").lower() == "true"
REQUEST_ENABLE_TRANSLATION = os.getenv("OTHELLO_ENABLE_TRANSLATION", "true").lower() == "true"
TRANSLATION_MIN_SCORE = int(os.getenv("OTHELLO_TRANSLATION_MIN_SCORE", "7"))
TRANSLATION_REMOTE_FALLBACK_SCORE = int(os.getenv("OTHELLO_TRANSLATION_REMOTE_FALLBACK_SCORE", "9"))
INTERNAL_SCHEDULER_ENABLED = os.getenv("OTHELLO_INTERNAL_SCHEDULER", "false").lower() == "true"
WORKER_BOOTSTRAP_MODE = os.getenv("OTHELLO_WORKER_BOOTSTRAP_MODE", "ingest").strip().lower()
WORKER_ENABLE_INGESTION = os.getenv("OTHELLO_WORKER_ENABLE_INGESTION", "true").lower() == "true"
WORKER_ENABLE_TRANSLATIONS = os.getenv("OTHELLO_WORKER_ENABLE_TRANSLATIONS", "false").lower() == "true"
WORKER_ENABLE_ANALYTICS = os.getenv("OTHELLO_WORKER_ENABLE_ANALYTICS", "false").lower() == "true"
HISTORICAL_FETCH_REFRESH_MINUTES = int(os.getenv("OTHELLO_HISTORICAL_FETCH_REFRESH_MINUTES", "5"))
HISTORICAL_FETCH_BATCH_LIMIT = int(os.getenv("OTHELLO_HISTORICAL_FETCH_BATCH_LIMIT", "30"))
HISTORICAL_FETCH_WRITE_BATCH_SIZE = int(os.getenv("OTHELLO_HISTORICAL_FETCH_WRITE_BATCH_SIZE", "15"))
HISTORICAL_FETCH_DOMAIN_INTERVAL_SECONDS = float(os.getenv("OTHELLO_HISTORICAL_FETCH_DOMAIN_INTERVAL_SECONDS", "2.0"))
HISTORICAL_FETCH_MAX_ATTEMPTS = int(os.getenv("OTHELLO_HISTORICAL_FETCH_MAX_ATTEMPTS", "3"))
STORY_MATERIALIZATION_REFRESH_MINUTES = int(os.getenv("OTHELLO_STORY_MATERIALIZATION_REFRESH_MINUTES", "45"))
ADMIN_API_KEY = os.getenv("OTHELLO_ADMIN_API_KEY", "").strip()
INGEST_JOB_LOCK = Lock()
BACKFILL_JOB_LOCK = Lock()
HISTORICAL_FETCH_JOB_LOCK = Lock()
STORY_MATERIALIZATION_JOB_LOCK = Lock()

QUERY_STOPWORDS = {
    "about", "across", "actually", "analysis", "analytical", "analyst", "and", "are", "article",
    "articles", "background", "beyond", "be", "brief", "briefing", "coming", "comprehensive",
    "conflict", "cover", "current", "currently", "deep", "dive", "direct", "events", "expect",
    "from", "give", "happening", "historical", "implications", "intelligence", "into", "key",
    "mainstream", "media", "missing", "motivations", "news", "of", "on", "or", "playing",
    "probability", "probabilitys", "precise", "role", "surface", "specific", "story", "talks",
    "that", "the", "their", "them", "they", "this", "underreporting", "what", "weeks", "who",
    "why", "with", "world", "would",
}


class QueryRequest(BaseModel):
    question: str
    topic: str | None = None
    region_context: str | None = None
    hotspot_id: str | None = None
    story_event_id: str | None = None
    source_urls: list[str] | None = None
    attention_window: str | None = None


def _request_is_internal(request: Request) -> bool:
    client_host = (request.client.host if request.client else "") or ""
    if not client_host:
        return False
    if client_host == "localhost":
        return True
    try:
        parsed = ipaddress.ip_address(client_host)
    except ValueError:
        return False
    return parsed.is_loopback


def require_write_access(request: Request, x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if _request_is_internal(request):
        return
    if ADMIN_API_KEY and x_api_key == ADMIN_API_KEY:
        return
    detail = "Write access requires an internal client or a valid X-API-Key."
    if not ADMIN_API_KEY:
        detail = f"{detail} Set OTHELLO_ADMIN_API_KEY to enable authenticated remote access."
    raise HTTPException(status_code=403, detail=detail)


def _run_exclusive(lock: Lock, label: str, fn):
    if not lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail=f"{label} already in progress.")
    try:
        return fn()
    finally:
        lock.release()


def _run_exclusive_or_skip(lock: Lock, label: str, fn):
    if not lock.acquire(blocking=False):
        print(f"[{label}] Skipping scheduled run because another {label} is already in progress.")
        return {"status": "skipped", "reason": f"{label} already in progress"}
    try:
        return fn()
    finally:
        lock.release()


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


def _topic_counts(hours: int = 72) -> dict[str, int]:
    return {topic: get_article_count(topic=topic, hours=hours) for topic in TOPICS}


def _topic_summary(topic: str) -> dict:
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


def _parse_timestamp(value: str | None) -> datetime | None:
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


def _humanize_region(region: str | None) -> str:
    text = (region or "").strip().lower()
    if not text:
        return "Global"
    return " ".join(part.capitalize() for part in text.split("-"))


def _attention_window_hours(window: str) -> int:
    normalized = (window or "24h").strip().lower()
    if normalized not in ATTENTION_WINDOW_HOURS:
        raise HTTPException(status_code=400, detail=f"window must be one of {sorted(ATTENTION_WINDOW_HOURS)}")
    return ATTENTION_WINDOW_HOURS[normalized]


def _build_region_attention_map(window: str = "24h") -> dict:
    normalized_window = (window or "24h").strip().lower()
    hours = _attention_window_hours(normalized_window)
    now = datetime.now(timezone.utc)
    rows = get_articles_with_regions(hours=hours)

    region_stats: dict[str, dict] = {}
    total_attention = 0.0
    global_article_count = 0
    global_attention_score = 0.0

    for row in rows:
        region = (row.get("region") or "global").strip().lower() or "global"
        published_at = _parse_timestamp(row.get("published_at"))
        source_key = (row.get("source_domain") or row.get("source") or "unknown").strip().lower()
        age_hours = max(0.0, (now - published_at).total_seconds() / 3600) if published_at else float(hours)
        recency_ratio = max(0.0, 1.0 - min(age_hours / max(hours, 1), 1.0))
        attention_increment = 1.0 + (recency_ratio * 1.75)

        if region == "global":
            global_article_count += 1
            global_attention_score += attention_increment
            continue

        entry = region_stats.setdefault(
            region,
            {
                "region": region,
                "label": _humanize_region(region),
                "article_count": 0,
                "source_keys": set(),
                "latest_published_at": None,
                "attention_score": 0.0,
            },
        )
        entry["article_count"] += 1
        entry["attention_score"] += attention_increment
        if source_key:
            entry["source_keys"].add(source_key)
        latest = entry["latest_published_at"]
        if published_at and (latest is None or published_at > latest):
            entry["latest_published_at"] = published_at
        total_attention += attention_increment

    ranked = sorted(
        region_stats.values(),
        key=lambda item: (
            -(item["attention_score"]),
            -(item["article_count"]),
            item["region"],
        ),
    )
    max_attention = max((item["attention_score"] for item in ranked), default=0.0)

    regions = []
    for item in ranked:
        latest = item["latest_published_at"]
        latest_text = latest.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if latest else None
        source_count = len(item["source_keys"])
        attention_score = round(float(item["attention_score"]), 2)
        attention_share = round((item["attention_score"] / total_attention), 4) if total_attention else 0.0
        cloud_size = round(0.32 + ((item["attention_score"] / max_attention) * 0.68), 3) if max_attention else 0.32
        regions.append(
            {
                "region": item["region"],
                "label": item["label"],
                "article_count": int(item["article_count"]),
                "source_count": source_count,
                "latest_published_at": latest_text,
                "attention_score": attention_score,
                "attention_share": attention_share,
                "cloud_size": cloud_size,
            }
        )

    return {
        "window": normalized_window,
        "hours": hours,
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "total_articles": len(rows),
        "global_article_count": global_article_count,
        "global_attention_score": round(global_attention_score, 2),
        "regions": regions,
        "available_windows": list(ATTENTION_WINDOW_HOURS.keys()),
    }


def _window_days(window: str) -> int:
    return max(1, math.ceil(_attention_window_hours(window) / 24))


def _event_datetime_for_hotspot(value: str | None) -> datetime | None:
    parsed = _parse_timestamp(value)
    if parsed is not None:
        return parsed.astimezone(timezone.utc)
    if value:
        try:
            return datetime.fromisoformat(str(value)).replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _haversine_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    lat1 = math.radians(lat_a)
    lon1 = math.radians(lon_a)
    lat2 = math.radians(lat_b)
    lon2 = math.radians(lon_b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    arc = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * radius_km * math.asin(min(1.0, math.sqrt(max(0.0, arc))))


def _world_region_for_coordinates(lat: float, lon: float) -> str:
    if lon < -30:
        if lat >= 50:
            return "united-states"
        return "united-states"
    if -30 <= lon < 20:
        if lat >= 50:
            return "united-kingdom" if lon < -2 and lat >= 50 else "europe"
        return "africa"
    if 20 <= lon < 42 and 12 <= lat < 45:
        return "middle-east"
    if 20 <= lon < 65:
        return "eurasia" if lat >= 45 else "middle-east"
    if 65 <= lon < 95:
        return "south-asia"
    if 95 <= lon < 180:
        return "asia-pacific"
    return "global"


def _hotspot_event_weight(event: dict, now: datetime, window_hours: int) -> tuple[float, datetime | None]:
    event_dt = _event_datetime_for_hotspot(event.get("event_date"))
    age_hours = max(0.0, (now - event_dt).total_seconds() / 3600) if event_dt else float(window_hours)
    recency = max(0.0, 1.0 - min(age_hours / max(window_hours, 1), 1.0))
    fatalities = int(event.get("fatalities") or 0)
    source_count = int(event.get("source_count") or 0)
    event_type = (event.get("event_type") or "").strip()
    weight = 1.0
    weight += HOTSPOT_EVENT_TYPE_WEIGHTS.get(event_type, 2.1)
    weight += min(fatalities, 40) * 0.18
    weight += min(source_count, 20) * 0.16
    weight += recency * 2.8
    return round(weight, 4), event_dt


def _dedup_location_string(text: str) -> str:
    """Remove redundant segments and resolve raw codes from GDELT-style location strings.
    e.g. 'Tehran, Tehran, Iran' → 'Tehran, Iran'
         'California, United States, United States' → 'California, United States'
         'IR, Iran' → 'Iran'
    """
    from gdelt_gkg_ingestion import COUNTRY_CODE_TO_NAME as _CC
    parts = [p.strip() for p in text.split(",") if p.strip()]
    # Resolve 2-letter country codes in individual segments
    resolved = []
    for p in parts:
        if len(p) == 2 and p.isupper() and p in _CC:
            resolved.append(_CC[p])
        else:
            resolved.append(p)
    deduped = []
    seen_lower: set[str] = set()
    for part in resolved:
        key = part.lower()
        if key not in seen_lower:
            deduped.append(part)
            seen_lower.add(key)
    return ", ".join(deduped)


def _acled_hotspot_event_copy(event: dict) -> dict:
    """Human-readable summary/title for map tooltips when DB summary is empty."""
    raw_summary = (event.get("summary") or "").strip()
    et = (event.get("event_type") or "Incident").strip()
    sub_raw = (event.get("sub_event_type") or "").strip()
    # Ignore raw CAMEO numeric codes (e.g. "190", "172") and GDELT admin codes as sub-types
    sub = sub_raw if sub_raw and not sub_raw.isdigit() and not re.match(r"^[A-Z]{2}[A-Z0-9]{0,4}$", sub_raw) else ""
    loc = _dedup_location_string(re.sub(r"\s*\(general\)", "", (event.get("location") or "").strip()))
    admin1 = (event.get("admin1") or "").strip()
    # Skip raw GDELT admin/country codes (e.g. "IS00", "USCA", "IS", "UK", "AS")
    if admin1 and re.match(r"^[A-Z]{2}[A-Z0-9]{0,4}$", admin1):
        admin1 = ""
    country = (event.get("country") or "").strip()
    # Resolve 2-letter GDELT/FIPS country codes to full names
    if country and len(country) <= 2 and country.isupper():
        from gdelt_gkg_ingestion import COUNTRY_CODE_TO_NAME
        country = COUNTRY_CODE_TO_NAME.get(country, country)
    a1 = (event.get("actor_primary") or "").strip()
    a2 = (event.get("actor_secondary") or "").strip()
    payload = event.get("payload") or {}
    fatal = int(event.get("fatalities") or 0)
    event_date = (event.get("event_date") or "").strip()

    # Strip CAMEO jargon and GDELT artifacts from old summaries
    if raw_summary:
        raw_summary = re.sub(r"\s*\(CAMEO\s+\d+,?\s*root\s+\d+\)", "", raw_summary).strip()
        raw_summary = re.sub(r"\s*\[?\d{3}\]?\s*$", "", raw_summary).strip()  # trailing CAMEO codes
        raw_summary = re.sub(r"\s*\(general\)", "", raw_summary).strip()  # GDELT qualifier
        raw_summary = raw_summary.rstrip(",").strip()
        raw_summary = _dedup_location_string(raw_summary)  # deduplicate location parts
    # Check if summary has actual narrative content (not just a location string)
    _summary_has_narrative = (
        raw_summary and len(raw_summary) > 30
        and any(kw in raw_summary.lower() for kw in (
            "reported", "involving", "attack", "strike", "killed", "injured", "protest",
            "clash", "fighting", "bomb", "explosion", "arrest", "ceasefire", "sanction",
            "election", "diplomat", "military", "troops", "forces", "violence",
            "offensive", "defensive", "siege", "raid", "detain", "fatalities",
            "incident", "development", "escalat", "de-escalat", "tension",
        ))
    )
    if _summary_has_narrative:
        summary = raw_summary
    else:
        # Build a full narrative sentence instead of terse fragments
        action = sub if sub and sub.lower() != et.lower() else et

        # Place detail — avoid redundant country when already in location string
        if loc and loc.lower() != country.lower():
            if country and country.lower() not in loc.lower():
                place_detail = f"in {loc}, {country}"
            else:
                place_detail = f"in {loc}"
        elif admin1 and admin1.lower() != country.lower():
            place_detail = f"in {admin1}, {country}" if country else f"in {admin1}"
        elif country:
            place_detail = f"in {country}"
        else:
            place_detail = "at an unspecified location"

        # Actor detail
        if a1 and a2:
            actor_detail = f" involving {a1} and {a2}"
        elif a1:
            actor_detail = f" involving {a1}"
        else:
            actor_detail = ""

        # Assemble narrative
        summary = f"{action} reported {place_detail}{actor_detail}"
        if event_date:
            summary += f" on {event_date}"
        summary += "."
        if fatal:
            summary += f" {fatal} fatalities reported."
        # Append raw_summary if it exists but was too short to use alone
        if raw_summary and raw_summary not in summary:
            summary += f" {raw_summary}"

    # Build a descriptive title
    place_for_title = loc or admin1 or country or "Unknown"
    if a1 and a2:
        title = f"{et}: {a1} vs {a2} — {place_for_title}"
    elif a1:
        title = f"{et} involving {a1} — {place_for_title}"
    else:
        detail = sub if sub and sub != et else None
        if detail:
            title = f"{et} ({detail}) — {place_for_title}"
        else:
            title = f"{et} — {place_for_title}"
    if fatal and "fatal" not in title.lower():
        title = f"{title} · {fatal} fatalities"
    return {"summary": summary, "title": title[:220]}


def _map_headline_for_structured_cluster(cluster: dict, primary_country: str) -> str:
    """Headline for map dots: narrative first, not 'EventType in Country: City' (place tail)."""
    summary = " ".join(str(cluster.get("summary") or "").split()).strip()
    if summary:
        first = summary.split(".")[0].strip()
        if len(first) < 20 and summary.count(".") >= 1:
            chunks = summary.split(".")
            first = ".".join(chunks[:2]).strip()
            if chunks[2:]:
                first += "."
        if len(first) > 168:
            first = first[:165].rsplit(" ", 1)[0] + "…"
        return first or summary[:168]

    # Fallback: build a meaningful narrative headline from cluster metadata
    pet = (cluster.get("primary_event_type") or "").strip() or "Incident"
    sub = (cluster.get("primary_sub_event_type") or "").strip()
    base = sub if sub and sub.lower() != pet.lower() else pet
    actors = [str(a).strip() for a in (cluster.get("entity_focus") or []) if str(a).strip()]
    locations = [str(loc).strip() for loc in (cluster.get("location_focus") or []) if str(loc).strip()]
    event_count = int(cluster.get("structured_event_count") or 0)
    fatalities = int(cluster.get("fatality_total") or 0)

    # Build place detail
    if locations:
        place = locations[0] if locations[0].lower() != primary_country.lower() else primary_country
        if len(locations) >= 2 and locations[0].lower() != primary_country.lower():
            place = f"{locations[0]} and nearby areas"
    else:
        place = primary_country

    # Build the headline with as much context as possible
    parts = [base]
    if actors and len(actors) >= 2:
        parts.append(f"involving {actors[0]} and {actors[1]}")
    elif actors:
        parts.append(f"involving {actors[0]}")
    elif event_count > 1:
        parts.append(f"({event_count} incidents)")
    parts.append(f"in {place}")
    if fatalities:
        parts.append(f"— {fatalities} fatalities reported")

    headline = " ".join(parts)
    if len(headline) > 168:
        headline = headline[:165].rsplit(" ", 1)[0] + "…"
    return headline


def _development_aspect_from_structured_cluster(cluster: dict) -> str:
    """Bucket incident semantics into political | conflict | economic (structured events are rarely economic)."""
    et = (cluster.get("primary_event_type") or "").strip()
    conflict_types = {"Battles", "Violence against civilians", "Explosions/Remote violence"}
    political_types = {"Protests", "Riots", "Strategic developments"}
    if et in conflict_types:
        return "conflict"
    if et in political_types:
        return "political"
    el = et.lower()
    if any(token in el for token in ("battle", "violence", "explosion", "clash", "airstrike", "armed attack")):
        return "conflict"
    if any(token in el for token in ("protest", "riot", "strategic", "coup", "election", "sanction", "diplom")):
        return "political"
    return "political"


def _hotspot_recency_factor(event_dt: datetime | None, now: datetime, window_hours: int) -> float:
    if event_dt is None:
        return 0.45
    age_hours = max(0.0, (now - event_dt).total_seconds() / 3600)
    return max(0.0, 1.0 - min(age_hours / max(window_hours, 1), 1.0))


def _incident_hotspots_from_semantic_clusters(
    semantic_clusters: list[dict],
    now: datetime,
    hours: int,
    cutoff: datetime,
) -> tuple[list[dict], int, int]:
    """Turn semantic structured clusters into geocoded map hotspots (one dot per development)."""
    hotspots: list[dict] = []
    candidate_events = 0
    window_fatalities = 0

    for cluster in semantic_clusters:
        window_events: list[dict] = []
        for raw in cluster.get("events") or []:
            ev = dict(raw)
            event_dt = _event_datetime_for_hotspot(ev.get("event_date"))
            if event_dt is None or event_dt < cutoff:
                continue
            window_events.append(ev)

        if not window_events:
            continue

        candidate_events += len(window_events)
        window_fatalities += sum(int(e.get("fatalities") or 0) for e in window_events)

        geocoded: list[dict] = []
        for ev in window_events:
            lat_v, lon_v = ev.get("latitude"), ev.get("longitude")
            if lat_v is None or lon_v is None:
                continue
            try:
                lat_f = float(lat_v)
                lon_f = float(lon_v)
            except (TypeError, ValueError):
                continue
            geocoded.append({**ev, "latitude": lat_f, "longitude": lon_f})

        if not geocoded:
            continue

        total_w = 0.0
        lat_acc = 0.0
        lon_acc = 0.0
        recency_vals: list[float] = []
        location_counts: dict[str, int] = defaultdict(int)
        country_counts: dict[str, int] = defaultdict(int)
        admin1_counts: dict[str, int] = defaultdict(int)
        event_type_counts: dict[str, int] = defaultdict(int)

        for ev in geocoded:
            w, _ = _hotspot_event_weight(ev, now, hours)
            total_w += w
            lat_acc += float(ev["latitude"]) * w
            lon_acc += float(ev["longitude"]) * w
            edt = _event_datetime_for_hotspot(ev.get("event_date"))
            recency_vals.append(_hotspot_recency_factor(edt, now, hours))

            # Resolve country codes and filter out raw admin codes for display
            ev_country = " ".join(str(ev.get("country") or "").split()).strip()
            if ev_country and len(ev_country) == 2 and ev_country.isupper():
                from gdelt_gkg_ingestion import COUNTRY_CODE_TO_NAME
                ev_country = COUNTRY_CODE_TO_NAME.get(ev_country, ev_country)
            ev_admin1 = " ".join(str(ev.get("admin1") or "").split()).strip()
            if ev_admin1 and re.match(r"^[A-Z]{2}[A-Z0-9]{0,4}$", ev_admin1):
                ev_admin1 = ""  # Skip raw GDELT admin/country codes like IS00, USCA, IS, UK

            ev_location = _dedup_location_string(
                re.sub(r"\s*\(general\)", "", " ".join(str(ev.get("location") or "").split()).strip())
            )
            for label, counter in (
                (ev_location, location_counts),
                (ev_country, country_counts),
                (ev_admin1, admin1_counts),
                (ev.get("event_type"), event_type_counts),
            ):
                clean = " ".join(str(label or "").split()).strip()
                if clean:
                    counter[clean] += 1

        centroid_lat = lat_acc / max(total_w, 1e-9)
        centroid_lon = lon_acc / max(total_w, 1e-9)
        avg_recency = sum(recency_vals) / max(len(recency_vals), 1)
        base_priority = float(cluster.get("analysis_priority") or 4.0)
        attention_score = round(base_priority * (0.32 + 0.68 * avg_recency), 2)

        primary_country = max(country_counts.items(), key=lambda item: (item[1], item[0]))[0] if country_counts else "Unknown country"
        # Resolve any remaining 2-letter country codes
        if primary_country and len(primary_country) == 2 and primary_country.isupper():
            from gdelt_gkg_ingestion import COUNTRY_CODE_TO_NAME
            primary_country = COUNTRY_CODE_TO_NAME.get(primary_country, primary_country)
        primary_location = (
            max(location_counts.items(), key=lambda item: (item[1], item[0]))[0]
            if location_counts
            else (max(admin1_counts.items(), key=lambda item: (item[1], item[0]))[0] if admin1_counts else primary_country)
        )
        primary_admin1 = max(admin1_counts.items(), key=lambda item: (item[1], item[0]))[0] if admin1_counts else None
        latest_dt = max((e for e in (_event_datetime_for_hotspot(ev.get("event_date")) for ev in geocoded) if e is not None), default=None)
        fatality_total = sum(int(ev.get("fatalities") or 0) for ev in geocoded)
        source_total = sum(int(ev.get("source_count") or 0) for ev in geocoded)

        headline = _map_headline_for_structured_cluster(cluster, primary_country)
        aspect = _development_aspect_from_structured_cluster(cluster)

        ranked_samples = sorted(
            geocoded,
            key=lambda ev: (
                _hotspot_event_weight(ev, now, hours)[0],
                int(ev.get("fatalities") or 0),
                int(ev.get("source_count") or 0),
                ev.get("event_date") or "",
            ),
            reverse=True,
        )[:4]
        sample_events = []
        for ev in ranked_samples:
            copy = _acled_hotspot_event_copy(ev)
            sample_events.append(
                {
                    "event_id": ev.get("event_id"),
                    "event_date": ev.get("event_date"),
                    "country": ev.get("country"),
                    "admin1": ev.get("admin1"),
                    "location": ev.get("location"),
                    "event_type": ev.get("event_type"),
                    "fatalities": int(ev.get("fatalities") or 0),
                    "source_count": int(ev.get("source_count") or 0),
                    "source_urls": list(ev.get("source_urls") or []),
                    "summary": copy["summary"],
                    "title": copy["title"],
                }
            )

        event_count = len(geocoded)
        material = f"sem|{headline}|{centroid_lat:.4f}|{centroid_lon:.4f}|{cluster.get('event_id')}"
        hotspot_id = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

        hotspots.append(
            {
                "hotspot_id": hotspot_id,
                "label": headline,
                "headline": headline,
                "cluster_label": (cluster.get("label") or "").strip() or headline,
                "country": primary_country,
                "admin1": primary_admin1,
                "location": primary_location,
                "latitude": round(centroid_lat, 4),
                "longitude": round(centroid_lon, 4),
                "event_count": int(event_count),
                "fatality_total": int(fatality_total),
                "source_count": int(source_total),
                "attention_score": attention_score,
                "attention_share": 0.0,
                "intensity": 0.0,
                "event_density": 0.0,
                "fatality_density": 0.0,
                "cloud_radius": 0.0,
                "cloud_density": 0.0,
                "latest_event_date": latest_dt.isoformat().replace("+00:00", "Z") if latest_dt else None,
                "event_types": [name for name, _ in sorted(event_type_counts.items(), key=lambda item: (-item[1], item[0]))[:4]],
                "aspect": aspect,
                "sample_locations": [name for name, _ in sorted(location_counts.items(), key=lambda item: (-item[1], item[0]))[:6]],
                "story_region": _world_region_for_coordinates(centroid_lat, centroid_lon),
                "sample_events": sample_events,
                "source_kind": "structured",
                "topic": "geopolitics",
            }
        )

    max_weight = max((float(h["attention_score"]) for h in hotspots), default=0.0)
    max_events = max((int(h["event_count"]) for h in hotspots), default=0)
    max_fatalities = max((int(h["fatality_total"]) for h in hotspots), default=0)
    total_cluster_weight = sum(float(h["attention_score"]) for h in hotspots)

    for h in hotspots:
        intensity = round((float(h["attention_score"]) / max_weight), 4) if max_weight else 0.0
        event_density = round((int(h["event_count"]) / max(max_events, 1)), 4)
        fatality_density = round((int(h["fatality_total"]) / max(max_fatalities, 1)), 4) if max_fatalities else 0.0
        cloud_radius = round(34.0 + (intensity * 42.0) + (event_density * 10.0), 2)
        cloud_density = round(min(1.0, 0.3 + (intensity * 0.45) + (event_density * 0.35)), 3)
        share = round((float(h["attention_score"]) / total_cluster_weight), 4) if total_cluster_weight else 0.0
        h["intensity"] = intensity
        h["event_density"] = event_density
        h["fatality_density"] = fatality_density
        h["cloud_radius"] = cloud_radius
        h["cloud_density"] = cloud_density
        h["attention_share"] = share

    hotspots.sort(
        key=lambda item: (
            float(item.get("attention_score") or 0.0),
            int(item.get("fatality_total") or 0),
            int(item.get("event_count") or 0),
        ),
        reverse=True,
    )
    hotspots = hotspots[:22]

    return hotspots, candidate_events, window_fatalities


def _pick_materialized_rows_for_map(
    rows: list[dict],
    *,
    target_hours: int,
    cutoff: datetime,
) -> list[dict]:
    """Deduplicate materialized clusters and keep rows relevant to the map window."""
    eligible: list[dict] = []
    for row in rows:
        latest = _parse_timestamp(row.get("latest_published_at") or "")
        latest_dt = latest.astimezone(timezone.utc) if latest is not None else None
        if latest_dt is None or latest_dt < cutoff:
            continue
        eligible.append(row)

    best: dict[str, tuple[int, dict]] = {}
    for row in eligible:
        key = str(row.get("cluster_key") or "")
        if not key:
            continue
        wh = int(row.get("window_hours") or 0) or target_hours
        dist = abs(wh - target_hours)
        prev = best.get(key)
        if prev is None or dist < prev[0] or (dist == prev[0] and (row.get("computed_at") or 0) > (prev[1].get("computed_at") or 0)):
            best[key] = (dist, row)
    return [pair[1] for pair in best.values()]


def _story_latest_datetime(story: dict) -> datetime | None:
    latest = _parse_timestamp(story.get("latest_update") or "")
    if latest is not None:
        return latest.astimezone(timezone.utc)
    source_dates = [
        _parse_timestamp(article.get("published_at") or "")
        for article in (story.get("sources") or [])
        if article.get("published_at")
    ]
    source_dates = [value.astimezone(timezone.utc) for value in source_dates if value is not None]
    return max(source_dates) if source_dates else None


def _story_hotspot_type(story: dict) -> str:
    topic = (story.get("topic") or "").strip().lower()
    text = " ".join(
        str(value or "")
        for value in (
            story.get("headline"),
            story.get("label"),
            story.get("title"),
            story.get("summary"),
            story.get("description"),
        )
    ).lower()
    event_types = " ".join(str(value or "") for value in (story.get("event_types") or [])).lower()
    if any(token in text or token in event_types for token in CONFLICT_TEXT_PATTERNS):
        return "conflict"
    if topic == "geopolitics":
        return "political"
    if topic == "economics":
        return "economic"
    if any(token in text for token in POLITICAL_TEXT_PATTERNS):
        return "political"
    if any(token in text for token in ECONOMIC_TEXT_PATTERNS):
        return "economic"
    return "story"


def _strip_story_dateline(text: str) -> str:
    clean = str(text or "").strip()
    previous = None
    while clean and clean != previous:
        previous = clean
        clean = DATELINE_RE.sub("", clean).strip()
    return clean


def _normalize_place_key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def _register_place_candidate(index: dict[str, dict], label: str | None, payload: dict) -> None:
    key = _normalize_place_key(label)
    if not key:
        return
    current = index.get(key)
    if current is None or float(payload.get("weight") or 0.0) > float(current.get("weight") or 0.0):
        index[key] = payload


def _build_story_location_index(days: int) -> dict[str, dict]:
    scope_days = max(14, min(days, 60))
    cached = _STORY_LOCATION_INDEX_CACHE.get(scope_days)
    now_ts = time.time()
    if cached and (now_ts - cached[0]) < MAP_CACHE_TTL_SECONDS:
        return cached[1]

    index: dict[str, dict] = {}
    structured = get_recent_structured_events(days=scope_days, limit=max(1800, scope_days * 40))
    grouped: dict[str, dict] = {}
    for event in structured:
        latitude = event.get("latitude")
        longitude = event.get("longitude")
        if latitude is None or longitude is None:
            continue
        try:
            lat = float(latitude)
            lon = float(longitude)
        except (TypeError, ValueError):
            continue
        location = " ".join(str(event.get("location") or "").split()).strip()
        country = " ".join(str(event.get("country") or "").split()).strip()
        admin1 = " ".join(str(event.get("admin1") or "").split()).strip() or None
        if not location and not country:
            continue
        canonical_key = _normalize_place_key(location or country)
        record = grouped.setdefault(
            canonical_key,
            {
                "label": location or country,
                "country": country or location or "Unknown",
                "admin1": admin1,
                "latitude_sum": 0.0,
                "longitude_sum": 0.0,
                "weight": 0.0,
            },
        )
        weight = 1.0 + min(int(event.get("source_count") or 0), 8) * 0.2 + min(int(event.get("fatalities") or 0), 25) * 0.05
        record["latitude_sum"] += lat * weight
        record["longitude_sum"] += lon * weight
        record["weight"] += weight

    for record in grouped.values():
        weight = max(float(record.get("weight") or 0.0), 1e-9)
        payload = {
            "label": record["label"],
            "country": record["country"],
            "admin1": record["admin1"],
            "latitude": record["latitude_sum"] / weight,
            "longitude": record["longitude_sum"] / weight,
            "weight": weight,
        }
        _register_place_candidate(index, record["label"], payload)
        _register_place_candidate(index, record["country"], payload)
        _register_place_candidate(index, record["admin1"], payload)

    for key, payload in COUNTRY_CENTROIDS.items():
        merged = {**payload, "weight": 0.5}
        _register_place_candidate(index, key, merged)
        _register_place_candidate(index, payload.get("label"), merged)
        _register_place_candidate(index, payload.get("country"), merged)

    _STORY_LOCATION_INDEX_CACHE[scope_days] = (now_ts, index)
    return index


def _story_article_text(article: dict) -> str:
    title = article.get("translated_title") or article.get("title") or article.get("original_title") or ""
    description = article.get("translated_description") or article.get("description") or article.get("original_description") or ""
    clean_title = _strip_story_dateline(title)
    clean_description = _strip_story_dateline(str(description)[:320])
    return f"{clean_title}. {clean_description}".strip()


def _resolve_story_place(entity_name: str, article_text: str, location_index: dict[str, dict]) -> dict | None:
    key = _normalize_place_key(entity_name)
    if not key:
        return None
    text = article_text.lower()
    if key == "washington":
        if any(token in text for token in ("washington dc", "white house", "state department", "pentagon", "capitol hill", "federal government")):
            return {
                "label": "Washington, DC",
                "country": "United States",
                "admin1": "District of Columbia",
                "latitude": 38.9072,
                "longitude": -77.0369,
                "weight": 3.0,
            }
        if any(token in text for token in ("washington state", "seattle", "spokane", "olympia")):
            return {
                "label": "Washington State",
                "country": "United States",
                "admin1": "Washington",
                "latitude": 47.7511,
                "longitude": -120.7401,
                "weight": 1.6,
            }
        return None
    return location_index.get(key)


def _story_article_language(article: dict) -> str | None:
    return article.get("translation_source_language") or article.get("language")


def _build_story_hotspots(window: str, now: datetime) -> tuple[list[dict], int]:
    """Story-layer dots from materialized clusters, geocoded via linked structured IDs (not 180 km article merge)."""
    normalized_window = (window or "24h").strip().lower()
    hours = _attention_window_hours(normalized_window)
    days = _window_days(normalized_window)
    cutoff = now - timedelta(hours=hours)
    location_index = _build_story_location_index(days)

    raw_rows = load_materialized_story_clusters(limit=220)
    picked = _pick_materialized_rows_for_map(raw_rows, target_hours=hours, cutoff=cutoff)
    picked.sort(key=lambda row: (row.get("latest_published_at") or "", row.get("computed_at") or 0), reverse=True)
    picked = picked[:26]

    story_candidates = 0
    hotspots: list[dict] = []
    for index, row in enumerate(picked, 1):
        urls = [str(u).strip() for u in (row.get("article_urls") or []) if u and str(u).strip()]
        linked = [str(x).strip() for x in (row.get("linked_structured_event_ids") or []) if x and str(x).strip()]
        story_candidates += max(len(urls), len(linked), 1)

        articles_map = get_articles_by_urls(urls, limit=48)
        coord_by_id = get_structured_event_coordinates_by_ids(linked)

        lat_sum = 0.0
        lon_sum = 0.0
        n_geo = 0
        location_counts: dict[str, int] = defaultdict(int)
        country_counts: dict[str, int] = defaultdict(int)
        admin1_counts: dict[str, int] = defaultdict(int)
        for meta in coord_by_id.values():
            lat_v, lon_v = meta.get("latitude"), meta.get("longitude")
            if lat_v is None or lon_v is None:
                continue
            try:
                lat_f = float(lat_v)
                lon_f = float(lon_v)
            except (TypeError, ValueError):
                continue
            lat_sum += lat_f
            lon_sum += lon_f
            n_geo += 1
            for label, bucket in (
                (meta.get("location"), location_counts),
                (meta.get("country"), country_counts),
                (meta.get("admin1"), admin1_counts),
            ):
                clean = " ".join(str(label or "").split()).strip()
                if clean:
                    bucket[clean] += 1

        centroid_lat: float | None = None
        centroid_lon: float | None = None
        if n_geo:
            centroid_lat = lat_sum / n_geo
            centroid_lon = lon_sum / n_geo

        primary_country = "Unknown country"
        primary_admin1 = None
        primary_location = ""

        if n_geo:
            primary_country = max(country_counts.items(), key=lambda item: (item[1], item[0]))[0] if country_counts else primary_country
            primary_location = (
                max(location_counts.items(), key=lambda item: (item[1], item[0]))[0]
                if location_counts
                else (max(admin1_counts.items(), key=lambda item: (item[1], item[0]))[0] if admin1_counts else primary_country)
            )
            primary_admin1 = max(admin1_counts.items(), key=lambda item: (item[1], item[0]))[0] if admin1_counts else None
        else:
            place_payload = None
            for url in urls[:12]:
                article = articles_map.get(url)
                if not article:
                    continue
                text = _story_article_text(article)
                if not text.strip():
                    continue
                try:
                    entities = extract_entities(text, language=_story_article_language(article))
                except Exception:
                    continue
                for entity in entities:
                    if entity.get("type") != "GPE":
                        continue
                    place = _resolve_story_place(entity.get("entity") or "", text, location_index)
                    if place:
                        place_payload = place
                        break
                if place_payload:
                    break
            if not place_payload:
                continue
            centroid_lat = float(place_payload["latitude"])
            centroid_lon = float(place_payload["longitude"])
            primary_country = str(place_payload.get("country") or "Unknown country")
            primary_admin1 = place_payload.get("admin1")
            primary_location = str(place_payload.get("label") or primary_country)

        if centroid_lat is None or centroid_lon is None:
            continue

        raw_label = " ".join(str(row.get("label") or "").split()).strip()
        summary_text = " ".join(str(row.get("summary") or "").split()).strip()

        # Try to extract a meaningful headline from the summary first
        primary_topic = (row.get("topic") or "").strip().lower() or None
        headline = ""
        if summary_text:
            first_sent = summary_text.split(".")[0].strip()
            if len(first_sent) >= 28:
                if len(first_sent) > 168:
                    first_sent = first_sent[:165].rsplit(" ", 1)[0] + "…"
                headline = first_sent
        if not headline:
            headline = raw_label
        if not headline or headline == primary_location or headline == primary_country:
            # Last resort: build a descriptive headline from the topic and location
            topic_label = "Economic development" if primary_topic == "economics" else "Political development" if primary_topic == "geopolitics" else "Development"
            article_count = len(urls)
            if article_count > 1:
                headline = f"{topic_label} in {primary_location or primary_country} ({article_count} sources)"
            else:
                headline = f"{topic_label} reported in {primary_location or primary_country}"
        aspect = _story_hotspot_type(
            {
                "topic": primary_topic,
                "label": headline,
                "headline": headline,
                "title": headline,
                "summary": summary_text or row.get("summary"),
                "description": summary_text or row.get("summary"),
            }
        )
        if aspect == "story":
            aspect = "economic" if primary_topic == "economics" else "political"

        latest = _parse_timestamp(row.get("latest_published_at") or "")
        latest_dt = latest.astimezone(timezone.utc) if latest is not None else None
        recency = _hotspot_recency_factor(latest_dt, now, hours)
        base = 11.0 + min(len(urls), 20) * 1.05 + len(linked) * 0.4 + n_geo * 0.55
        attention_score = round(base * (0.3 + 0.7 * recency), 2)

        arts_sorted = sorted(
            articles_map.values(),
            key=lambda a: (a.get("published_at") or ""),
            reverse=True,
        )
        sample_events = []
        for art in arts_sorted[:4]:
            title = (art.get("title") or "").strip()[:220]
            body = (art.get("description") or art.get("translated_description") or title or "").strip()
            sample_events.append(
                {
                    "event_id": art.get("url"),
                    "event_date": art.get("published_at"),
                    "country": primary_country,
                    "admin1": primary_admin1,
                    "location": primary_location,
                    "event_type": aspect,
                    "fatalities": 0,
                    "source_count": 1,
                    "source_urls": [art.get("url")] if art.get("url") else [],
                    "title": title or None,
                    "summary": body or None,
                }
            )

        if not sample_events and urls:
            fallback_summary = (row.get("summary") or headline)[:280] or headline
            for url in urls[:4]:
                sample_events.append(
                    {
                        "event_id": url,
                        "event_date": row.get("latest_published_at"),
                        "country": primary_country,
                        "admin1": primary_admin1,
                        "location": primary_location,
                        "event_type": aspect,
                        "fatalities": 0,
                        "source_count": 1,
                        "source_urls": [url] if url else [],
                        "title": headline,
                        "summary": fallback_summary,
                    }
                )

        latest_out = latest_dt.isoformat().replace("+00:00", "Z") if latest_dt else None
        material = f"mstory|{row.get('cluster_key')}|{centroid_lat:.4f}|{centroid_lon:.4f}|{index}"
        hotspot_id = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

        hotspots.append(
            {
                "hotspot_id": hotspot_id,
                "label": headline,
                "headline": headline,
                "country": primary_country,
                "admin1": primary_admin1,
                "location": primary_location,
                "latitude": round(centroid_lat, 4),
                "longitude": round(centroid_lon, 4),
                "event_count": max(len(urls), 1),
                "fatality_total": 0,
                "source_count": max(len({(a.get("source") or "").strip().lower() for a in arts_sorted}), 1),
                "attention_score": attention_score,
                "attention_share": 0.0,
                "intensity": 0.0,
                "event_density": 0.0,
                "fatality_density": 0.0,
                "cloud_radius": 0.0,
                "cloud_density": 0.0,
                "latest_event_date": latest_out,
                "event_types": [aspect],
                "aspect": aspect,
                "sample_locations": [name for name, _ in sorted(location_counts.items(), key=lambda item: (-item[1], item[0]))[:6]],
                "story_region": _world_region_for_coordinates(centroid_lat, centroid_lon),
                "sample_events": sample_events,
                "source_kind": "story",
                "topic": primary_topic,
            }
        )

    max_weight = max((float(h["attention_score"]) for h in hotspots), default=0.0)
    max_events = max((int(h["event_count"]) for h in hotspots), default=0)
    total_cluster_weight = sum(float(h["attention_score"]) for h in hotspots) or 1.0

    for h in hotspots:
        intensity = round((float(h["attention_score"]) / (max_weight + 6.0)), 4) if max_weight else 0.0
        event_density = round((int(h["event_count"]) / max(max_events, 1)), 4)
        coverage_density = round(min(1.0, int(h["source_count"]) / 6.0), 4)
        share = round((float(h["attention_score"]) / (total_cluster_weight + max(len(hotspots), 1) * 4.0)), 4)
        cloud_radius = round(20.0 + (intensity * 24.0) + (event_density * 18.0) + (coverage_density * 14.0), 2)
        cloud_density = round(min(0.92, 0.22 + (intensity * 0.36) + (event_density * 0.18) + (coverage_density * 0.16)), 3)
        h["intensity"] = intensity
        h["event_density"] = event_density
        h["fatality_density"] = 0.0
        h["cloud_radius"] = cloud_radius
        h["cloud_density"] = cloud_density
        h["attention_share"] = share

    return hotspots, story_candidates


def _build_hotspot_attention_map(window: str = "24h") -> dict:
    normalized_window = (window or "24h").strip().lower()
    cached = _MAP_ATTENTION_CACHE.get(normalized_window)
    now_ts = time.time()
    if cached and (now_ts - cached[0]) < MAP_CACHE_TTL_SECONDS:
        return cached[1]

    hours = _attention_window_hours(normalized_window)
    days = _window_days(normalized_window)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    structured_days = min(days, MAX_MAP_STRUCTURED_DAYS)
    semantic_clusters = build_map_structured_story_clusters(
        structured_days=structured_days,
        limit=40,
        dataset=None,
    )
    hotspots, structured_candidate_count, window_fatalities = _incident_hotspots_from_semantic_clusters(
        semantic_clusters,
        now,
        hours,
        cutoff,
    )

    story_hotspots, total_story_candidates = _build_story_hotspots(normalized_window, now)
    combined = hotspots + story_hotspots
    combined.sort(
        key=lambda item: (
            float(item.get("attention_score") or 0.0),
            int(item.get("source_count") or 0),
            int(item.get("event_count") or 0),
        ),
        reverse=True,
    )
    combined = combined[:32]
    total_attention = sum(float(item.get("attention_score") or 0.0) for item in combined) or 1.0
    max_attention = max((float(item.get("attention_score") or 0.0) for item in combined), default=1.0)
    for item in combined:
        item["attention_share"] = round(float(item.get("attention_score") or 0.0) / total_attention, 4)
        if item.get("source_kind") == "story":
            item["intensity"] = round(float(item.get("attention_score") or 0.0) / max_attention, 4) if max_attention else 0.0

    payload = {
        "window": normalized_window,
        "hours": hours,
        "days": days,
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "total_events": structured_candidate_count + total_story_candidates,
        "hotspot_count": len(combined),
        "total_fatalities": int(window_fatalities),
        "available_windows": list(ATTENTION_WINDOW_HOURS.keys()),
        "hotspots": combined,
    }
    _MAP_ATTENTION_CACHE[normalized_window] = (now_ts, payload)
    return payload


def _extract_search_focus(question: str) -> str:
    quoted = [match.group(1).strip() for match in re.finditer(r'"([^"]{4,})"', question or "")]
    if quoted:
        return quoted[0]

    cleaned = re.sub(r"[^A-Za-z0-9\s-]", " ", question or "")
    words = [
        word
        for word in cleaned.split()
        if len(word) >= 3 and word.lower() not in QUERY_STOPWORDS
    ]
    if not words:
        return (question or "").strip()
    return " ".join(words[:8])


def _compose_query_search_seed(
    question: str,
    region_context: str | None = None,
    source_urls: list[str] | None = None,
) -> str:
    parts: list[str] = []
    if region_context and str(region_context).strip():
        parts.append(str(region_context).strip())
    q = (question or "").strip()
    if not parts:
        return q
    return " ".join(parts) + (" " + q if q else "")


def _clean_source_urls(source_urls: list[str] | None, *, limit: int = 12) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for url in source_urls or []:
        candidate = str(url or "").strip()
        if not candidate.startswith("http") or candidate in seen:
            continue
        seen.add(candidate)
        cleaned.append(candidate)
        if len(cleaned) >= limit:
            break
    return cleaned


def _resolve_story_source_urls(story_event_id: str | None) -> list[str]:
    event_id = str(story_event_id or "").strip()
    if not event_id:
        return []

    cached = load_headlines(ttl=HEADLINES_TTL) or rebuild_headlines_cache(use_llm=False)
    for story in cached:
        if str(story.get("event_id") or "").strip() != event_id:
            continue
        return _clean_source_urls(
            [source.get("url") for source in (story.get("sources") or []) if isinstance(source, dict)],
            limit=12,
        )
    return []


def _resolve_hotspot_source_urls(hotspot_id: str | None, attention_window: str | None = None) -> list[str]:
    target_id = str(hotspot_id or "").strip()
    if not target_id:
        return []

    payload = _build_hotspot_attention_map(window=attention_window or "24h")
    for hotspot in payload.get("hotspots", []):
        if str(hotspot.get("hotspot_id") or "").strip() != target_id:
            continue
        urls: list[str] = []
        for sample in hotspot.get("sample_events") or []:
            sample_urls = sample.get("source_urls")
            if isinstance(sample_urls, list):
                urls.extend(sample_urls)
            event_id = str(sample.get("event_id") or "").strip()
            if event_id.startswith("http"):
                urls.append(event_id)
        return _clean_source_urls(urls, limit=12)
    return []


def _normalize_query_corpus_topic(topic: str | None) -> str | None:
    if not topic:
        return None
    t = str(topic).strip().lower()
    if t == "conflict":
        return "geopolitics"
    if t in TOPICS:
        return t
    return None


def _infer_query_topic(query: str) -> str | None:
    lowered = (query or "").lower()
    best_topic = None
    best_score = 0
    for topic, keywords in {
        "geopolitics": {
            "iran", "israel", "ukraine", "russia", "china", "taiwan", "war", "military",
            "sanctions", "diplomacy", "missile", "nato", "conflict", "ceasefire", "strike",
        },
        "economics": {
            "inflation", "tariffs", "rates", "markets", "economy", "economic", "fed",
            "reserve", "jobs", "gdp", "oil", "trade", "currency", "bonds", "yields",
        },
    }.items():
        score = sum(1 for keyword in keywords if keyword in lowered)
        if score > best_score:
            best_topic = topic
            best_score = score
    return best_topic if best_score else None


def _append_unique_articles(target: list[dict], seen_urls: set[str], articles: list[dict], limit: int) -> None:
    for article in articles:
        url = article.get("url")
        if not url or url in seen_urls:
            continue
        target.append(article)
        seen_urls.add(url)
        if len(target) >= limit:
            return


def _article_translation_priority(article: dict) -> int:
    topics = infer_article_topics(article)
    score = article_quality_score(article, topics)
    published_at = _parse_timestamp(article.get("published_at"))
    if published_at:
        age_hours = max(0.0, (datetime.now(timezone.utc) - published_at.astimezone(timezone.utc)).total_seconds() / 3600)
        if age_hours <= 24:
            score += 2
        elif age_hours <= 72:
            score += 1
    if article.get("source_domain") in {"reuters.com", "apnews.com", "bbc.com", "ft.com"}:
        score += 1
    return score


def _needs_translation(article: dict, min_priority: int = TRANSLATION_MIN_SCORE) -> bool:
    return (
        REQUEST_ENABLE_TRANSLATION
        and not is_english_article(article)
        and not article.get("translated_title")
        and _article_translation_priority(article) >= min_priority
    )


def ensure_article_translations(articles: list[dict], max_articles: int = 6, min_priority: int = TRANSLATION_MIN_SCORE) -> list[dict]:
    if not REQUEST_ENABLE_TRANSLATION:
        return articles

    candidates = []
    for article in articles:
        if not _needs_translation(article, min_priority=min_priority):
            continue
        candidates.append((_article_translation_priority(article), article))

    translated_count = 0
    for _, article in sorted(candidates, key=lambda item: item[0], reverse=True):
        if translated_count >= max_articles:
            break
        try:
            priority = _article_translation_priority(article)
            translation = translate_article(
                article,
                allow_remote_fallback=priority >= TRANSLATION_REMOTE_FALLBACK_SCORE,
            )
            save_article_translation(
                article_url=article["url"],
                source_language=article.get("language") or "unknown",
                translated_title=translation["translated_title"],
                translated_description=translation.get("translated_description"),
                translation_provider=translation.get("provider", "groq"),
                target_language=translation.get("target_language", "en"),
            )
            article["translated_title"] = translation["translated_title"]
            article["translated_description"] = translation.get("translated_description")
            article["translation_provider"] = translation.get("provider", "translation")
            article["translation_target_language"] = translation.get("target_language", "en")
            article["title"] = translation["translated_title"] or article.get("title")
            article["description"] = translation.get("translated_description") or article.get("description")
            translated_count += 1
        except Exception as exc:
            print(f"[translation] Failed for {article.get('url')}: {exc}")
    return articles


def _store_entity_mentions_with_translation(articles: list[dict], topic: str) -> dict:
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            return store_entity_mentions(articles, topic)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == attempts:
                print(f"[entities] Entity extraction skipped for '{topic}': {exc}")
                return {
                    "topic": topic,
                    "articles_processed": len(articles),
                    "mentions_written": 0,
                    "cooccurrences_written": 0,
                    "error": str(exc),
                    "status": "skipped_locked",
                }
            time.sleep(0.5 * attempt)


def _clear_headlines_resilient() -> None:
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            clear_headlines()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == attempts:
                print(f"[cache] Headline cache clear skipped: {exc}")
                return
            time.sleep(0.5 * attempt)


def _entity_payload(entity_stats: dict | None) -> dict:
    if not entity_stats:
        return {}
    return {"entity_extraction": entity_stats}


def _latest_entity_telemetry() -> dict:
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


def _store_articles_safe(articles: list[dict], topic: str) -> None:
    if not REQUEST_ENABLE_CHROMA_INGEST:
        return
    try:
        store_articles(articles, topic)
    except Exception as exc:
        print(f"[chroma] Ingest store failed for '{topic}': {exc}")


def _gather_query_articles(
    question: str,
    topic: str | None = None,
    limit: int = 12,
    keyword_hours: int = 336,
    *,
    region_context: str | None = None,
    hotspot_id: str | None = None,
    story_event_id: str | None = None,
    source_urls: list[str] | None = None,
    attention_window: str | None = None,
) -> tuple[list[dict], dict]:
    grounding_urls = _clean_source_urls(source_urls, limit=12)
    if not grounding_urls and story_event_id:
        grounding_urls = _resolve_story_source_urls(story_event_id)
    if not grounding_urls and hotspot_id:
        grounding_urls = _resolve_hotspot_source_urls(hotspot_id, attention_window=attention_window)

    search_seed = _compose_query_search_seed(question, region_context, source_urls)
    focus = _extract_search_focus(search_seed)
    resolved_topic = topic or _infer_query_topic(focus or search_seed)
    combined: list[dict] = []
    seen_urls: set[str] = set()
    historical_sources = 0
    live_sources = 0
    grounding_used = False

    if grounding_urls:
        grounded_map = get_articles_by_urls(grounding_urls, limit=max(limit, len(grounding_urls)))
        grounded_articles = [grounded_map[url] for url in grounding_urls if url in grounded_map]
        if grounded_articles:
            _append_unique_articles(combined, seen_urls, grounded_articles, limit)
            historical_sources = len(combined)
            grounding_used = True

    if REQUEST_ENABLE_VECTOR_SEARCH and len(combined) < limit:
        try:
            vector_hits = search_articles(focus or search_seed, n_results=min(limit, 8), topic=resolved_topic)
            _append_unique_articles(combined, seen_urls, vector_hits, limit)
            historical_sources = len(combined)
        except Exception as exc:
            print(f"[query] Chroma search failed, falling back to keyword search: {exc}")

    keyword_queries = []
    if focus:
        keyword_queries.append(focus)
    if search_seed not in keyword_queries:
        keyword_queries.append(search_seed)

    for candidate in keyword_queries:
        keyword_hits = search_recent_articles_by_keywords(candidate, topic=resolved_topic, limit=max(limit * 2, 18), hours=keyword_hours)
        _append_unique_articles(combined, seen_urls, keyword_hits, limit)
        historical_sources = len(combined)
        if len(combined) >= limit:
            break

    if len(combined) < max(6, limit // 2) and resolved_topic:
        recent_topic_articles = get_recent_articles(topic=resolved_topic, limit=limit, hours=keyword_hours)
        _append_unique_articles(combined, seen_urls, recent_topic_articles, limit)
        historical_sources = len(combined)

    if not combined:
        recent_global_articles = get_recent_articles(limit=limit, hours=keyword_hours)
        _append_unique_articles(combined, seen_urls, recent_global_articles, limit)
        historical_sources = len(combined)

    if REQUEST_ENABLE_LIVE_FETCH and REQUEST_ENABLE_LIVE_FETCH and len(combined) < max(6, limit // 2):
        try:
            live_hits = fetch_articles_for_query(focus or search_seed, page_size=min(limit, 8))
            before = len(combined)
            _append_unique_articles(combined, seen_urls, live_hits, limit)
            live_sources = len(combined) - before
        except Exception as exc:
            print(f"[query] Live fetch failed, continuing with stored corpus only: {exc}")

    combined = ensure_article_translations(combined[:limit], max_articles=8)
    return combined, {
        "focus": focus or search_seed,
        "topic": resolved_topic,
        "historical_sources": historical_sources,
        "live_sources": live_sources,
        "region_context": region_context,
        "hotspot_id": hotspot_id,
        "story_event_id": story_event_id,
        "grounding_used": grounding_used,
        "grounding_source_count": len(grounding_urls),
    }


def _event_rank_score(event: dict) -> float:
    base = float(event.get("analysis_priority", 0) or 0)
    source_count = int(event.get("source_count", 0) or 0)
    article_count = int(event.get("article_count", 0) or 0)
    tier_1_source_count = int(event.get("tier_1_source_count", 0) or 0)
    latest = _parse_timestamp(event.get("latest_update"))
    coverage_score = (source_count * 16.0) + (article_count * 4.5) + (tier_1_source_count * 3.0)
    if not latest:
        return round(coverage_score + (base * 0.35), 2)
    age_hours = max(0.0, (datetime.now(timezone.utc) - latest).total_seconds() / 3600)
    if age_hours <= 6:
        freshness_multiplier = 1.9
        freshness_bonus = 28.0
    elif age_hours <= 12:
        freshness_multiplier = 1.7
        freshness_bonus = 22.0
    elif age_hours <= 24:
        freshness_multiplier = 1.5
        freshness_bonus = 15.0
    elif age_hours <= 36:
        freshness_multiplier = 1.25
        freshness_bonus = 9.0
    elif age_hours <= 48:
        freshness_multiplier = 1.12
        freshness_bonus = 3.0
    else:
        freshness_multiplier = 1.0
        freshness_bonus = max(0.0, 72.0 - age_hours) / 18.0

    return round((coverage_score * freshness_multiplier) + freshness_bonus + (base * 0.35), 2)


def _backfill_state_key(topic: str) -> str:
    return f"gdelt-backfill:{topic}"


def _scheduler_state_key() -> str:
    return "gdelt-backfill:scheduler"


def _cursor_start_for_topic(topic: str) -> datetime:
    state = load_ingestion_state(_backfill_state_key(topic))
    if state:
        if state.get("status") == "error" and state.get("cursor_start"):
            return _parse_timestamp(state["cursor_start"]) or datetime.now(timezone.utc) - timedelta(days=1)
        if state.get("cursor_end"):
            return _parse_timestamp(state["cursor_end"]) or datetime.now(timezone.utc) - timedelta(days=1)

    if GDELT_BACKFILL_START:
        configured = _parse_timestamp(GDELT_BACKFILL_START)
        if configured:
            return configured

    bounds = get_topic_time_bounds(topic)
    if bounds.get("earliest_published_at"):
        parsed = _parse_timestamp(bounds["earliest_published_at"])
        if parsed:
            return parsed - timedelta(hours=6)

    return datetime.now(timezone.utc) - timedelta(days=3)


def _backfill_should_wait(state: dict | None) -> tuple[bool, str | None]:
    if not state:
        return False, None
    payload = state.get("payload") or {}
    retry_after = payload.get("retry_after")
    if not retry_after:
        return False, None
    retry_at = _parse_timestamp(retry_after)
    if retry_at and retry_at > datetime.now(timezone.utc):
        return True, retry_after
    return False, None


def _adaptive_backfill_config(state: dict | None) -> tuple[int, int, int]:
    payload = (state or {}).get("payload") or {}
    window_hours = int(payload.get("window_hours") or GDELT_BACKFILL_WINDOW_HOURS)
    page_size = int(payload.get("page_size") or GDELT_BACKFILL_PAGE_SIZE)
    failure_count = int(payload.get("failure_count") or 0)
    return (
        max(GDELT_BACKFILL_MIN_WINDOW_HOURS, window_hours),
        max(4, page_size),
        max(0, failure_count),
    )


def _next_scheduled_topics(topics: list[str] | None) -> list[str]:
    if topics:
        return topics
    state = load_ingestion_state(_scheduler_state_key())
    last_topic = state.get("topic") if state else None
    if last_topic in TOPICS:
        next_index = (TOPICS.index(last_topic) + 1) % len(TOPICS)
    else:
        next_index = 0
    chosen = TOPICS[next_index]
    save_ingestion_state(
        _scheduler_state_key(),
        topic=chosen,
        provider="gdelt-backfill-scheduler",
        cursor_start=None,
        cursor_end=None,
        status="ok",
        payload={"last_topic": chosen},
    )
    return [chosen]


def run_incremental_gdelt_backfill(topics: list[str] | None = None) -> list[dict]:
    target_end = datetime.now(timezone.utc) - timedelta(minutes=GDELT_BACKFILL_LAG_MINUTES)
    selected_topics = _next_scheduled_topics(topics)
    results = []

    for topic in selected_topics:
        started_at = time.time()
        state_key = _backfill_state_key(topic)
        state = load_ingestion_state(state_key)
        window_hours, page_size, failure_count = _adaptive_backfill_config(state)
        should_wait, retry_after = _backfill_should_wait(state)
        if should_wait:
            results.append(
                {
                    "topic": topic,
                    "status": "backoff",
                    "retry_after": retry_after,
                    "window_hours": window_hours,
                    "page_size": page_size,
                    "cursor_start": state.get("cursor_start") if state else None,
                    "cursor_end": state.get("cursor_end") if state else None,
                }
            )
            continue

        window_start = _cursor_start_for_topic(topic)
        if window_start >= target_end:
            results.append(
                {
                    "topic": topic,
                    "status": "idle",
                    "window_hours": window_hours,
                    "page_size": page_size,
                    "cursor_start": window_start.isoformat(),
                    "cursor_end": window_start.isoformat(),
                    "message": "Backfill cursor is caught up to the configured lag window.",
                }
            )
            continue

        window_end = min(window_start + timedelta(hours=window_hours), target_end)

        try:
            articles = fetch_gdelt_historic_articles(
                topic=topic,
                start=window_start,
                end=window_end,
                page_size=page_size,
                min_window_hours=GDELT_BACKFILL_MIN_WINDOW_HOURS,
            )
            if articles:
                inserted = upsert_articles(
                    articles,
                    topic=topic,
                    provider="gdelt",
                    default_analytic_tier="volume",
                )
                if GDELT_BACKFILL_CHROMA:
                    _store_articles_safe(articles, topic)
                entity_stats = _store_entity_mentions_with_translation(articles, topic)
                record_ingestion_run(topic, "gdelt-backfill", len(articles), started_at, "ok")
                save_ingestion_state(
                    state_key,
                    topic=topic,
                    provider="gdelt-backfill",
                    cursor_start=window_start.isoformat(),
                    cursor_end=window_end.isoformat(),
                    status="ok",
                    payload={
                        "fetched": len(articles),
                        "inserted_or_updated": inserted,
                        "retry_after": None,
                        "window_hours": min(
                            GDELT_BACKFILL_WINDOW_HOURS,
                            window_hours * 2 if len(articles) < max(4, page_size // 2) else window_hours,
                        ),
                        "page_size": min(
                            GDELT_BACKFILL_PAGE_SIZE,
                            page_size + 1 if len(articles) >= max(4, page_size // 2) else page_size,
                        ),
                        "failure_count": 0,
                        "entity_extraction": entity_stats,
                    },
                )
                results.append(
                    {
                        "topic": topic,
                        "status": "ok",
                        "cursor_start": window_start.isoformat(),
                        "cursor_end": window_end.isoformat(),
                        "window_hours": window_hours,
                        "page_size": page_size,
                        "fetched": len(articles),
                        "inserted_or_updated": inserted,
                    }
                )
            else:
                message = "No GDELT articles returned for this backfill window."
                record_ingestion_run(topic, "gdelt-backfill", 0, started_at, "empty", error=message)
                save_ingestion_state(
                    state_key,
                    topic=topic,
                    provider="gdelt-backfill",
                    cursor_start=window_start.isoformat(),
                    cursor_end=window_end.isoformat(),
                    status="empty",
                    error=message,
                    payload={
                        "retry_after": None,
                        "window_hours": min(GDELT_BACKFILL_WINDOW_HOURS, window_hours * 2),
                        "page_size": max(4, page_size - 1) if page_size > 4 else page_size,
                        "failure_count": 0,
                    },
                )
                results.append(
                    {
                        "topic": topic,
                        "status": "empty",
                        "cursor_start": window_start.isoformat(),
                        "cursor_end": window_end.isoformat(),
                        "window_hours": window_hours,
                        "page_size": page_size,
                        "fetched": 0,
                        "inserted_or_updated": 0,
                    }
                )
        except Exception as exc:
            error_text = str(exc)
            is_rate_limit = "429" in error_text or "Too Many Requests" in error_text
            is_timeout = "Read timed out" in error_text or "timed out" in error_text.lower()
            retry_delay = GDELT_BACKFILL_RATE_LIMIT_RETRY_MINUTES if is_rate_limit else GDELT_BACKFILL_RETRY_MINUTES
            next_failure_count = failure_count + 1
            retry_delay += min(360, next_failure_count * (45 if is_rate_limit else 20))
            retry_after = (datetime.now(timezone.utc) + timedelta(minutes=retry_delay)).isoformat()
            next_window_hours = max(
                GDELT_BACKFILL_MIN_WINDOW_HOURS,
                max(1, window_hours // 2) if (is_rate_limit or is_timeout) else window_hours,
            )
            next_page_size = max(4, page_size - (4 if is_rate_limit else 2)) if (is_rate_limit or is_timeout) else page_size
            record_ingestion_run(topic, "gdelt-backfill", 0, started_at, "error", error=str(exc))
            save_ingestion_state(
                state_key,
                topic=topic,
                provider="gdelt-backfill",
                cursor_start=window_start.isoformat(),
                cursor_end=window_end.isoformat(),
                status="error",
                error=error_text,
                payload={
                    "retry_after": retry_after,
                    "window_hours": next_window_hours,
                    "page_size": next_page_size,
                    "failure_count": next_failure_count,
                },
            )
            results.append(
                {
                    "topic": topic,
                    "status": "error",
                    "cursor_start": window_start.isoformat(),
                    "cursor_end": window_end.isoformat(),
                    "window_hours": window_hours,
                    "page_size": page_size,
                    "next_window_hours": next_window_hours,
                    "next_page_size": next_page_size,
                    "error": error_text,
                    "retry_after": retry_after,
                }
            )

    return results


def ingest_topic(topic: str, page_size: int = 60) -> dict:
    started_at = time.time()
    state_key = f"analytic-ingest-{topic}"
    try:
        articles = fetch_articles(topic, page_size=page_size)
        provider = articles[0].get("provider", "unknown") if articles else "unknown"
        archive_summary = archive_provider_articles(articles, provider=provider, topic_hint=topic)
        if not articles:
            fallback_provider = "directfeeds"
            fallback_articles = fetch_articles_from_provider(topic, fallback_provider, page_size=max(20, page_size // 2)) if source_status()["directfeeds"]["enabled"] else []
            if fallback_articles:
                articles = fallback_articles
                provider = f"{fallback_provider}-fallback"
                archive_summary = archive_provider_articles(articles, provider=provider, topic_hint=topic)
            else:
                message = f"No fresh articles returned for topic '{topic}' from configured providers."
                record_ingestion_run(topic, provider, 0, started_at, "empty", error=message)
                save_ingestion_state(state_key, topic, provider, None, None, "empty", error=message, payload={"fetched": 0, "promoted": 0, "archived_documents": archive_summary["documents_written"]})
                print(f"[ingest] {message}")
                return {
                    "topic": topic,
                    "provider": provider,
                    "fetched": 0,
                    "promoted": 0,
                    "inserted_or_updated": 0,
                    "existing_or_unchanged": 0,
                    "status": "empty",
                    "error": message,
                }
        quality_scores = {a["url"]: article_quality_score(a, [topic]) for a in articles if a.get("url")}
        promoted = [article for article in articles if should_promote_article(article, [topic])]
        rejected = [article for article in articles if not should_promote_article(article, [topic])]
        if rejected:
            upsert_article_summaries(rejected, topic=topic, quality_scores=quality_scores)
        if not promoted:
            message = f"Fetched {len(articles)} articles for '{topic}', but none passed analytic promotion."
            record_ingestion_run(topic, provider, 0, started_at, "empty", error=message)
            save_ingestion_state(state_key, topic, provider, None, None, "empty", error=message, payload={"fetched": len(articles), "promoted": 0, "rejected": len(articles), "archived_documents": archive_summary["documents_written"]})
            return {
                "topic": topic,
                "provider": provider,
                "fetched": len(articles),
                "promoted": 0,
                "archived_documents": archive_summary["documents_written"],
                "inserted_or_updated": 0,
                "tier2_summaries": len(rejected),
                "existing_or_unchanged": 0,
                "status": "empty",
                "error": message,
            }
        write_provider = "newsapi" if provider == "newsapi-fallback" else ("directfeeds" if provider == "directfeeds-fallback" else provider)
        inserted = upsert_articles(promoted, topic=topic, provider=write_provider)
        existing_or_unchanged = max(len(promoted) - inserted, 0)
        _store_articles_safe(promoted, topic)
        entity_stats = _store_entity_mentions_with_translation(promoted, topic)
        record_ingestion_run(topic, provider, len(promoted), started_at, "ok")
        save_ingestion_state(state_key, topic, provider, None, None, "ok", payload={"fetched": len(articles), "promoted": len(promoted), "rejected": max(len(articles) - len(promoted), 0), "archived_documents": archive_summary["documents_written"], "inserted_or_updated": inserted, "existing_or_unchanged": existing_or_unchanged, "entity_extraction": entity_stats})
        _clear_headlines_resilient()
        return {
            "topic": topic,
            "provider": provider,
            "fetched": len(articles),
            "promoted": len(promoted),
            "archived_documents": archive_summary["documents_written"],
            "inserted_or_updated": inserted,
            "existing_or_unchanged": existing_or_unchanged,
            "status": "ok",
        }
    except Exception as exc:
        record_ingestion_run(topic, "unknown", 0, started_at, "error", error=str(exc))
        save_ingestion_state(state_key, topic, "unknown", None, None, "error", error=str(exc), payload={})
        print(f"[ingest] Topic '{topic}' failed: {exc}")
        return {
            "topic": topic,
            "provider": "unknown",
            "fetched": 0,
            "promoted": 0,
            "inserted_or_updated": 0,
            "existing_or_unchanged": 0,
            "status": "error",
            "error": str(exc),
        }


def ingest_global(page_size: int = 100) -> dict:
    started_at = time.time()
    state_key = "analytic-ingest-global"
    try:
        articles = fetch_global_articles(page_size=page_size)
        provider = articles[0].get("provider", "unknown") if articles else "unknown"
        archive_summary = archive_provider_articles(articles, provider=provider, topic_hint="global")
        if not articles and source_status()["directfeeds"]["enabled"]:
            articles = fetch_global_articles_from_provider("directfeeds", page_size=max(30, page_size // 2))
            provider = "directfeeds-fallback" if articles else provider
            archive_summary = archive_provider_articles(articles, provider=provider, topic_hint="global-fallback") if articles else archive_summary

        if not articles:
            message = "Global ingest returned no fresh articles from configured providers."
            for topic in TOPICS:
                record_ingestion_run(topic, "unknown", 0, started_at, "empty", error=message)
            save_ingestion_state(state_key, "global", "unknown", None, None, "empty", error=message, payload={"fetched": 0, "archived_documents": archive_summary["documents_written"]})
            print(f"[ingest] {message}")
            return {
                "provider": "unknown",
                "fetched": 0,
                "classified": {topic: 0 for topic in TOPICS},
                "inserted_or_updated": 0,
                "unclassified": 0,
                "status": "empty",
                "error": message,
            }
        topic_buckets = {topic: [] for topic in TOPICS}
        tier2_articles = []
        unclassified = 0
        rejected = 0

        for article in articles:
            article_topics = infer_article_topics(article)
            if not article_topics:
                unclassified += 1
                tier2_articles.append(article)
                continue
            if not should_promote_article(article, article_topics):
                rejected += 1
                tier2_articles.append((article, article_topics[0]))
                continue
            topic_buckets[article_topics[0]].append(article)

        if tier2_articles:
            global_quality_scores = {a["url"]: article_quality_score(a) for a in articles if a.get("url")}
            for entry in tier2_articles:
                if isinstance(entry, tuple):
                    art, t2_topic = entry
                    upsert_article_summaries([art], topic=t2_topic, quality_scores=global_quality_scores)
                else:
                    upsert_article_summaries([entry], topic=None, quality_scores=global_quality_scores)

        total_written = 0
        entity_stats_by_topic = {}
        write_provider = "newsapi" if provider == "newsapi-fallback" else ("directfeeds" if provider == "directfeeds-fallback" else provider)
        for topic, topic_articles in topic_buckets.items():
            if not topic_articles:
                record_ingestion_run(topic, provider, 0, started_at, "empty", error=f"No classified articles for topic '{topic}' in this ingest batch.")
                continue
            total_written += upsert_articles(topic_articles, topic=topic, provider=write_provider)
            _store_articles_safe(topic_articles, topic)
            entity_stats_by_topic[topic] = _store_entity_mentions_with_translation(topic_articles, topic)
            record_ingestion_run(topic, provider, len(topic_articles), started_at, "ok")
        classified_total = sum(len(items) for items in topic_buckets.values())
        existing_or_unchanged = max(classified_total - total_written, 0)

        if total_written == 0 and provider != "directfeeds-fallback" and source_status()["directfeeds"]["enabled"]:
            fallback_result = ingest_article_fallback(page_size=max(30, page_size // 2))
            if fallback_result.get("inserted_or_updated", 0) > 0:
                return {
                    "provider": fallback_result.get("provider", "directfeeds-fallback"),
                    "fetched": len(articles),
                    "classified": {topic: len(items) for topic, items in topic_buckets.items()},
                    "rejected": rejected,
                    "archived_documents": archive_summary["documents_written"],
                    "inserted_or_updated": fallback_result["inserted_or_updated"],
                    "existing_or_unchanged": fallback_result.get("existing_or_unchanged", 0),
                    "unclassified": unclassified,
                    "status": "ok",
                    "fallback": fallback_result,
                }

        _clear_headlines_resilient()
        save_ingestion_state(state_key, "global", provider, None, None, "ok" if total_written else "empty", payload={"fetched": len(articles), "classified": {topic: len(items) for topic, items in topic_buckets.items()}, "rejected": rejected, "unclassified": unclassified, "archived_documents": archive_summary["documents_written"], "inserted_or_updated": total_written, "existing_or_unchanged": existing_or_unchanged, "entity_extraction": entity_stats_by_topic})
        return {
            "provider": provider,
            "fetched": len(articles),
            "classified": {topic: len(items) for topic, items in topic_buckets.items()},
            "rejected": rejected,
            "archived_documents": archive_summary["documents_written"],
            "inserted_or_updated": total_written,
            "existing_or_unchanged": existing_or_unchanged,
            "unclassified": unclassified,
            "status": "ok" if total_written else "empty",
        }
    except Exception as exc:
        for topic in TOPICS:
            record_ingestion_run(topic, "unknown", 0, started_at, "error", error=str(exc))
        save_ingestion_state(state_key, "global", "unknown", None, None, "error", error=str(exc), payload={})
        print(f"[ingest] Global ingest failed: {exc}")
        return {
            "provider": "unknown",
            "fetched": 0,
            "classified": {topic: 0 for topic in TOPICS},
            "inserted_or_updated": 0,
            "unclassified": 0,
            "status": "error",
            "error": str(exc),
        }


def ingest_all_topics() -> list[dict]:
    global_result = ingest_global(page_size=100)
    results = [global_result]
    sparse_topics = [topic for topic, count in _topic_counts().items() if count < MIN_TOPIC_ARTICLES]
    for topic in sparse_topics:
        results.append(ingest_topic(topic))
    return results


def run_scheduled_ingest_cycle():
    return _run_exclusive_or_skip(INGEST_JOB_LOCK, "ingest", ingest_all_topics)


def run_scheduled_gdelt_backfill():
    return _run_exclusive_or_skip(BACKFILL_JOB_LOCK, "gdelt backfill", run_incremental_gdelt_backfill)


def run_historical_queue_fetch(
    limit: int = HISTORICAL_FETCH_BATCH_LIMIT,
    batch_size: int = HISTORICAL_FETCH_WRITE_BATCH_SIZE,
    min_domain_interval_seconds: float = HISTORICAL_FETCH_DOMAIN_INTERVAL_SECONDS,
    max_attempts: int = HISTORICAL_FETCH_MAX_ATTEMPTS,
) -> dict:
    started_at = time.time()
    state_key = "historical-fetch-queue"
    try:
        result = fetch_historical_queue(
            limit=max(1, limit),
            batch_size=max(1, batch_size),
            min_domain_interval_seconds=max(0.0, min_domain_interval_seconds),
            max_attempts=max(1, max_attempts),
            dry_run=False,
        )
        status = "ok" if result.get("inserted_or_updated", 0) > 0 else ("empty" if result.get("processed", 0) == 0 else "ok")
        save_ingestion_state(
            state_key,
            "historical",
            "historical-fetch",
            None,
            None,
            status,
            payload=result,
        )
        record_ingestion_run(
            "historical",
            "historical-fetch",
            int(result.get("inserted_or_updated", 0) or 0),
            started_at,
            status,
        )
        return result
    except Exception as exc:
        save_ingestion_state(
            state_key,
            "historical",
            "historical-fetch",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        record_ingestion_run("historical", "historical-fetch", 0, started_at, "error", error=str(exc))
        print(f"[historical-fetch] Historical queue fetch failed: {exc}")
        return {"status": "error", "error": str(exc)}


def run_scheduled_historical_queue_fetch():
    return _run_exclusive_or_skip(HISTORICAL_FETCH_JOB_LOCK, "historical queue fetch", run_historical_queue_fetch)


def run_scheduled_story_materialization():
    def _job():
        return rebuild_materialized_story_clusters(
            topics=TOPICS,
            window_hours=CORPUS_WINDOW_HOURS,
            articles_limit=120,
        )

    return _run_exclusive_or_skip(STORY_MATERIALIZATION_JOB_LOCK, "story materialization", _job)


def _gdelt_unhealthy() -> bool:
    states = [load_ingestion_state(_backfill_state_key(topic)) for topic in TOPICS]
    active_states = [state for state in states if state]
    if not active_states:
        return False
    unhealthy = 0
    for state in active_states:
        if state.get("status") != "error":
            continue
        payload = state.get("payload") or {}
        if payload.get("retry_after") or state.get("error"):
            unhealthy += 1
    return unhealthy >= len(TOPICS)


def _article_corpus_stale(max_age_hours: int = 8) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    global_bounds = get_topic_time_bounds()
    latest_global = _parse_timestamp(global_bounds.get("latest_published_at"))
    if latest_global is None or latest_global < cutoff:
        return True
    sparse_topics = 0
    for topic in TOPICS:
        latest_topic = _parse_timestamp(get_topic_time_bounds(topic).get("latest_published_at"))
        if latest_topic is None or latest_topic < cutoff:
            sparse_topics += 1
    return sparse_topics >= 1


def ingest_article_fallback(page_size: int = 40) -> dict:
    started_at = time.time()
    state_key = "analytic-ingest-fallback"
    fallback_provider = "directfeeds" if source_status()["directfeeds"]["enabled"] else None
    fallback_state_provider = f"{fallback_provider}-fallback" if fallback_provider else "directfeeds-fallback"
    if fallback_provider is None:
        save_ingestion_state(state_key, "global-fallback", fallback_state_provider, None, None, "disabled", error="No direct-feed fallback is configured.", payload={})
        return {"status": "disabled", "reason": "No direct-feed fallback is configured."}
    gdelt_unhealthy = _gdelt_unhealthy()
    corpus_stale = _article_corpus_stale()
    if not gdelt_unhealthy and not corpus_stale:
        reason = "GDELT is not marked unhealthy and the article corpus is still fresh."
        save_ingestion_state(state_key, "global-fallback", fallback_state_provider, None, None, "skipped", error=reason, payload={"gdelt_unhealthy": gdelt_unhealthy, "corpus_stale": corpus_stale})
        return {"status": "skipped", "reason": reason}

    try:
        articles = fetch_global_articles_from_provider(fallback_provider, page_size=page_size)
        archive_summary = archive_provider_articles(articles, provider=fallback_state_provider, topic_hint="global-fallback")
        if not articles:
            message = f"{fallback_provider} fallback did not return any fresh articles."
            for topic in TOPICS:
                record_ingestion_run(topic, fallback_state_provider, 0, started_at, "empty", error=message)
            save_ingestion_state(state_key, "global-fallback", fallback_state_provider, None, None, "empty", error=message, payload={"fetched": 0, "archived_documents": archive_summary["documents_written"]})
            return {"status": "empty", "fetched": 0, "error": message}

        topic_buckets = {topic: [] for topic in TOPICS}
        rejected = 0
        for article in articles:
            article_topics = infer_article_topics(article)
            if not article_topics:
                continue
            if not should_promote_article(article, article_topics):
                rejected += 1
                continue
            topic_buckets[article_topics[0]].append(article)

        total_written = 0
        entity_stats_by_topic = {}
        for topic, topic_articles in topic_buckets.items():
            if not topic_articles:
                record_ingestion_run(topic, fallback_state_provider, 0, started_at, "empty", error=f"No classified fallback articles for '{topic}'.")
                continue
            total_written += upsert_articles(topic_articles, topic=topic, provider=fallback_provider)
            _store_articles_safe(topic_articles, topic)
            entity_stats_by_topic[topic] = _store_entity_mentions_with_translation(topic_articles, topic)
            record_ingestion_run(topic, fallback_state_provider, len(topic_articles), started_at, "ok")
        classified_total = sum(len(rows) for rows in topic_buckets.values())
        existing_or_unchanged = max(classified_total - total_written, 0)

        if total_written:
            _clear_headlines_resilient()

        save_ingestion_state(state_key, "global-fallback", fallback_state_provider, None, None, "ok" if total_written else "empty", payload={"fetched": len(articles), "classified": {topic: len(rows) for topic, rows in topic_buckets.items()}, "rejected": rejected, "archived_documents": archive_summary["documents_written"], "inserted_or_updated": total_written, "existing_or_unchanged": existing_or_unchanged, "entity_extraction": entity_stats_by_topic})
        return {
            "provider": fallback_state_provider,
            "status": "ok" if total_written else "empty",
            "fetched": len(articles),
            "rejected": rejected,
            "archived_documents": archive_summary["documents_written"],
            "inserted_or_updated": total_written,
            "existing_or_unchanged": existing_or_unchanged,
            "classified": {topic: len(rows) for topic, rows in topic_buckets.items()},
        }
    except Exception as exc:
        for topic in TOPICS:
            record_ingestion_run(topic, fallback_state_provider, 0, started_at, "error", error=str(exc))
        save_ingestion_state(state_key, "global-fallback", fallback_state_provider, None, None, "error", error=str(exc), payload={})
        return {"status": "error", "error": str(exc)}


def bootstrap_from_legacy_cache() -> dict:
    backend_dir = Path(__file__).resolve().parent
    legacy_paths = [
        backend_dir / "othello_cache.db",
        backend_dir.parent.parent / "backend" / "othello_cache.db",
    ]
    imported = {topic: 0 for topic in TOPICS}

    for db_path in legacy_paths:
        if not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(db_path)
            rows = conn.execute(
                "SELECT topic, sources FROM briefing_cache"
            ).fetchall()
            conn.close()
        except Exception as exc:
            print(f"[bootstrap] Failed reading legacy cache {db_path}: {exc}")
            continue

        for topic, sources_json in rows:
            if topic not in TOPICS:
                continue
            try:
                articles = json.loads(sources_json or "[]")
            except json.JSONDecodeError:
                continue
            if not articles:
                continue
            written = upsert_articles(articles, topic=topic, provider="legacy-cache")
            _store_articles_safe(articles, topic)
            _store_entity_mentions_with_translation(articles, topic)
            imported[topic] += written or len(articles)

    return {"imported": imported}


def seed_local_corpus() -> dict:
    backend_dir = Path(__file__).resolve().parent
    cache_paths = [
        backend_dir / "othello_cache.db",
        backend_dir.parent.parent / "backend" / "othello_cache.db",
    ]
    totals = {
        "articles_seen": 0,
        "inserted_or_updated": 0,
        "stored_for_entities": 0,
        "topics": {topic: 0 for topic in TOPICS},
        "paths_scanned": [],
    }

    seen_pairs: set[tuple[str, str]] = set()
    for db_path in cache_paths:
        if not db_path.exists():
            continue
        totals["paths_scanned"].append(str(db_path))
        try:
            conn = sqlite3.connect(db_path)
            briefing_rows = conn.execute("SELECT topic, sources FROM briefing_cache").fetchall()
            headline_rows = conn.execute("SELECT stories FROM headlines_cache").fetchall()
            conn.close()
        except Exception as exc:
            print(f"[seed] Failed reading cache {db_path}: {exc}")
            continue

        topic_buckets: dict[str, list[dict]] = {topic: [] for topic in TOPICS}

        for topic, sources_json in briefing_rows:
            if topic not in TOPICS:
                continue
            try:
                articles = json.loads(sources_json or "[]")
            except json.JSONDecodeError:
                continue
            for article in articles:
                url = article.get("url")
                pair = (topic, url or "")
                if not url or pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                topic_buckets[topic].append(article)
                totals["articles_seen"] += 1

        for (stories_json,) in headline_rows:
            try:
                stories = json.loads(stories_json or "[]")
            except json.JSONDecodeError:
                continue
            for story in stories:
                story_topic = story.get("topic")
                for article in story.get("sources", []):
                    url = article.get("url")
                    inferred_topics = [story_topic] if story_topic in TOPICS else infer_article_topics(article)
                    for topic in inferred_topics:
                        if topic not in TOPICS:
                            continue
                        pair = (topic, url or "")
                        if not url or pair in seen_pairs:
                            continue
                        seen_pairs.add(pair)
                        topic_buckets[topic].append(article)
                        totals["articles_seen"] += 1

        for topic, articles in topic_buckets.items():
            if not articles:
                continue
            inserted = upsert_articles(articles, topic=topic, provider="local-seed")
            _store_articles_safe(articles, topic)
            _store_entity_mentions_with_translation(articles, topic)
            totals["inserted_or_updated"] += inserted
            totals["stored_for_entities"] += len(articles)
            totals["topics"][topic] += len(articles)

    _clear_headlines_resilient()
    rebuild_headlines_cache(use_llm=False)
    for topic in TOPICS:
        build_topic_briefing(topic, force_refresh=True)

    return totals


def _ensure_topic_corpus(topic: str, minimum_articles: int = MIN_TOPIC_ARTICLES) -> None:
    if get_article_count(topic=topic, hours=72) >= minimum_articles:
        return
    ingest_topic(topic)


def _build_topic_events(topic: str, limit: int = 8, attempt_ingest: bool = False) -> list[dict]:
    if attempt_ingest:
        _ensure_topic_corpus(topic)
    articles = ensure_article_translations(
        get_recent_articles(topic=topic, limit=120, hours=CORPUS_WINDOW_HOURS, headline_corpus_only=True),
        max_articles=10,
    )
    if not articles:
        return []
    filtered_articles = []
    for article in articles:
        inferred_topics = infer_article_topics(article)
        primary_topic = inferred_topics[0] if inferred_topics else None
        if primary_topic and primary_topic != topic:
            continue
        filtered_articles.append(article)

    # Prefer topic-consistent inputs for ranking, but fall back gracefully if inference is sparse.
    event_articles = filtered_articles or articles
    events = enrich_events(cluster_articles(event_articles, topic=topic))
    return events[:limit]


def _event_article_urls(event: dict) -> set[str]:
    return {
        (article.get("url") or "").strip()
        for article in event.get("articles", [])
        if (article.get("url") or "").strip()
    }


def _events_materially_overlap(left: dict, right: dict) -> bool:
    left_urls = _event_article_urls(left)
    right_urls = _event_article_urls(right)
    if not left_urls or not right_urls:
        return False

    overlap = left_urls & right_urls
    if not overlap:
        return False

    smaller_cluster = min(len(left_urls), len(right_urls))
    if smaller_cluster <= 1:
        return True
    if (left.get("label") or "").strip() == (right.get("label") or "").strip():
        return True
    return len(overlap) >= max(2, math.ceil(smaller_cluster * 0.5))


def _dedupe_global_events(events: list[dict]) -> list[dict]:
    if len(events) <= 1:
        return events

    sort_key = lambda event: (
        _event_rank_score(event),
        event.get("latest_update", ""),
    )
    ranked = sorted(events, key=sort_key, reverse=True)
    selected: list[dict] = []
    for event in ranked:
        if any(_events_materially_overlap(event, existing) for existing in selected):
            continue
        selected.append(event)
    return selected


def _build_global_events(limit: int = 12) -> list[dict]:
    counts = _topic_counts()
    if sum(counts.values()) == 0:
        ingest_all_topics()
    events = []
    for topic in TOPICS:
        if counts.get(topic, 0) == 0:
            continue
        events.extend(_build_topic_events(topic, limit=8, attempt_ingest=False))
    events = _dedupe_global_events(events)
    recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_events = []
    older_events = []
    for event in events:
        latest = _parse_timestamp(event.get("latest_update"))
        if latest and latest >= recent_cutoff:
            recent_events.append(event)
        else:
            older_events.append(event)

    sort_key = lambda event: (
        _event_rank_score(event),
        event.get("latest_update", ""),
    )
    recent_events.sort(key=sort_key, reverse=True)
    older_events.sort(key=sort_key, reverse=True)

    observed = (recent_events + older_events)[:limit]
    observe_events(observed)
    return observed


def _story_summary_candidate_score(summary: str, headline: str) -> int:
    if not summary:
        return -10_000
    score = min(len(summary), 220)
    if len(summary) < 28:
        score -= 120
    if summary.lower() == headline.lower():
        score -= 400
    if len(summary) > 210:
        score -= len(summary) - 210
    if any(marker in summary for marker in (". ", "! ", "? ", "; ")):
        score += 16
    return score


def _standardize_story_summary(story: dict, event: dict | None = None) -> str:
    headline = normalize_article_title((event or {}).get("label") or story.get("headline") or "")
    candidates = [(event or {}).get("summary")]
    articles = list(story.get("sources") or [])
    if event:
        articles.extend(event.get("articles", []) or [])
    for article in articles:
        candidates.extend(
            [
                article.get("translated_description"),
                article.get("description"),
                article.get("original_description"),
            ]
        )
    candidates.append(story.get("summary"))

    best_summary = ""
    best_score = -10_000
    for candidate in candidates:
        normalized = normalize_article_description(candidate, headline, limit=200)
        score = _story_summary_candidate_score(normalized, headline)
        if score > best_score:
            best_summary = normalized
            best_score = score

    if best_score > -50 and best_summary:
        return best_summary

    source_count = int(story.get("source_count") or (event or {}).get("source_count") or 0)
    if source_count > 1:
        return f"{source_count} sources are tracking the latest turn in this story."
    topic = (story.get("topic") or (event or {}).get("topic") or "").replace("_", " ").strip()
    if topic:
        return f"Fresh {topic} reporting is still developing."
    return "Fresh reporting is still developing."


def _standardize_headline_story(story: dict, event: dict | None = None) -> dict:
    headline = normalize_article_title((event or {}).get("label") or story.get("headline") or "Untitled")
    return {
        **story,
        "headline": headline or "Untitled",
        "summary": _standardize_story_summary({**story, "headline": headline}, event=event),
    }


def _story_region_counts(story: dict) -> dict[str, int]:
    counts = defaultdict(int)
    for article in story.get("sources", []) or []:
        region = (
            ((article.get("source_profile") or {}).get("region"))
            or article.get("source_region")
            or "global"
        )
        normalized = str(region).strip().lower() or "global"
        counts[normalized] += 1
    if story.get("region_counts"):
        for region, count in (story.get("region_counts") or {}).items():
            normalized = str(region).strip().lower() or "global"
            counts[normalized] = max(counts.get(normalized, 0), int(count or 0))
    return dict(counts)


def _story_dominant_region(story: dict) -> str:
    counts = _story_region_counts(story)
    if not counts:
        return "global"
    non_global = {
        region: count for region, count in counts.items() if region and region != "global"
    }
    pool = non_global or counts
    return sorted(pool.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _story_rank_score(story: dict) -> float:
    latest = _parse_timestamp(story.get("latest_update") or "")
    source_count = int(story.get("source_count", 0) or 0)
    article_count = int(story.get("article_count", source_count) or 0)
    contradiction_count = int(story.get("contradiction_count", 0) or 0)
    ranking_score = float(story.get("ranking_score", 0) or 0)
    age_hours = max(
        0.0,
        (datetime.now(timezone.utc) - latest).total_seconds() / 3600,
    ) if latest else 240.0

    if age_hours <= 6:
        freshness = 36.0
    elif age_hours <= 12:
        freshness = 28.0
    elif age_hours <= 24:
        freshness = 18.0
    elif age_hours <= 48:
        freshness = 9.0
    else:
        freshness = max(0.0, 96.0 - age_hours) / 12.0

    return round(
        freshness
        + (source_count * 18.0)
        + (article_count * 4.0)
        + (contradiction_count * 1.5)
        + (ranking_score * 0.25),
        2,
    )


def _sort_headline_stories(stories: list[dict], sort_by: str = "relevance", region: str | None = None) -> list[dict]:
    normalized_region = (region or "").strip().lower()
    selected = [
        {
            **story,
            "region_counts": _story_region_counts(story),
            "dominant_region": (story.get("dominant_region") or _story_dominant_region(story)).strip().lower(),
        }
        for story in stories
    ]
    if normalized_region and normalized_region not in {"all", "global-overview"}:
        selected = [story for story in selected if story.get("dominant_region") == normalized_region]

    if sort_by == "region":
        selected.sort(
            key=lambda story: (
                story.get("dominant_region") in {"", "global"},
                story.get("dominant_region") or "global",
                -_story_rank_score(story),
                story.get("headline", ""),
            )
        )
        return selected

    selected.sort(
        key=lambda story: (
            -_story_rank_score(story),
            -(int(story.get("source_count", 0) or 0)),
            -(int(story.get("article_count", 0) or 0)),
            story.get("latest_update") or "",
        )
    )
    return selected


def _available_story_regions(stories: list[dict]) -> list[str]:
    regions = {
        (story.get("dominant_region") or _story_dominant_region(story)).strip().lower()
        for story in stories
        if (story.get("dominant_region") or _story_dominant_region(story)).strip()
    }
    return sorted(region for region in regions if region and region != "global")


def _fallback_headlines(events: list[dict]) -> list[dict]:
    stories = []
    for event in events[:7]:
        story = {
            "event_id": event["event_id"],
            "headline": event["label"],
            "summary": event["summary"],
            "topic": event.get("topic"),
            "why_signal": (
                f"{event.get('source_count', 0)} sources, "
                f"{event.get('article_count', 0)} reports, "
                f"{event.get('contradiction_count', 0)} contradiction flags."
            ),
            "entity_focus": event.get("entity_focus", []),
            "source_count": event.get("source_count", 0),
            "article_count": event.get("article_count", 0),
            "contradiction_count": event.get("contradiction_count", 0),
            "dominant_region": event.get("dominant_region"),
            "region_counts": event.get("region_counts", {}),
            "ranking_score": _event_rank_score(event),
            "sources": event.get("articles", []),
        }
        stories.append(
            _standardize_headline_story(story, event=event)
        )
    return stories


def rebuild_headlines_cache(use_llm: bool = False) -> list[dict]:
    events = _build_global_events(limit=8)
    if not events:
        return []

    fallback_stories = _fallback_headlines(events)
    if use_llm and os.getenv("GROQ_API_KEY"):
        try:
            stories = build_headlines_from_events(events)
        except Exception as exc:
            print(f"[headlines] LLM headline build failed, using fallback: {exc}")
            stories = fallback_stories
    else:
        stories = fallback_stories

    event_map = {event["event_id"]: event for event in events}
    fallback_map = {story["event_id"]: story for story in fallback_stories if story.get("event_id")}
    enriched = []
    seen_event_ids = set()
    for story in stories:
        event = event_map.get(story.get("event_id"))
        if not event:
            continue
        seen_event_ids.add(event["event_id"])
        enriched_story = {
            **fallback_map.get(event["event_id"], {}),
            **story,
            "topic": story.get("topic") or event.get("topic"),
            "entity_focus": event.get("entity_focus", []),
            "source_count": event.get("source_count", 0),
            "article_count": event.get("article_count", 0),
            "contradiction_count": event.get("contradiction_count", 0),
            "latest_update": event.get("latest_update"),
            "dominant_region": event.get("dominant_region"),
            "region_counts": event.get("region_counts", {}),
            "ranking_score": _event_rank_score(event),
            "sources": event.get("articles", []),
        }
        enriched.append(
            _standardize_headline_story(enriched_story, event=event)
        )

    for event in events:
        if event["event_id"] in seen_event_ids:
            continue
        fallback_story = fallback_map.get(event["event_id"])
        if fallback_story:
            enriched.append(
                _standardize_headline_story(fallback_story, event=event)
            )

    sorted_stories = _sort_headline_stories(enriched, sort_by="relevance")
    save_headlines(sorted_stories)
    return sorted_stories


def _briefing_fallback(topic: str, articles: list[dict], events: list[dict], signals: str = "", contradictions: str = "") -> str:
    top_event_lines = "\n".join(
        f"- {event['label']}: {event['summary']}" for event in events[:4]
    ) or "- No major clustered events yet."
    actor_lines = "\n".join(
        f"- {entity}" for entity in sorted({entity for event in events[:4] for entity in event.get("entity_focus", [])})[:6]
    ) or "- No dominant actors extracted yet."
    watch_lines = "\n".join(
        f"- {event['label']} ({event.get('source_count', 0)} sources)" for event in events[:4]
    ) or "- Awaiting more reporting."
    contradiction_block = contradictions or "No significant contradictions detected in current clustered coverage."

    return f"""SITUATION REPORT:
This briefing is generated from the stored Othello corpus for {topic}. The system is operating in deterministic mode because LLM generation is unavailable.

KEY DEVELOPMENTS:
{top_event_lines}

CRITICAL ACTORS:
{actor_lines}

SIGNAL vs NOISE:
{signals or "- Entity spike data is still forming."}

PREDICTIONS:
- Monitor whether the highest-ranked events retain source diversity over the next reporting cycle.
- Expect briefing quality to improve as the corpus expands and article clusters deepen.

DEEPER CONTEXT:
- Articles included in this briefing: {len(articles)}
- Event clusters analyzed: {len(events)}

WHAT TO WATCH:
{watch_lines}

SOURCE CONTRADICTIONS:
{contradiction_block}
"""


def _query_fallback(question: str, articles: list[dict], topic: str | None = None) -> str:
    lead = articles[:3]
    lead_lines = "\n".join(
        f"- {article.get('source', 'Unknown source')} ({article.get('published_at', 'Unknown time')}): {article.get('title', 'Untitled')}"
        for article in lead
    ) or "- No strong lead reporting was available."

    themes = []
    seen_titles = set()
    for article in articles:
        title = (article.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        themes.append(f"- {title}")
        if len(themes) == 5:
            break

    scope = f" within the {topic} corpus" if topic else ""
    return f"""SITUATION REPORT:
This answer was generated from the stored Othello corpus{scope} because live LLM analysis is temporarily unavailable. The strongest relevant reporting currently includes:
{lead_lines}

KEY DEVELOPMENTS:
{chr(10).join(themes) or "- The corpus contains relevant reporting, but the event pattern is still thin."}

ANALYTIC TAKE:
- The reporting set indicates this question is active enough to surface across multiple articles.
- Confidence should be treated as moderate until a broader mix of sources converges on the same facts.
- Use the linked sources below as the primary evidence base while the analysis model is offline.
"""


def _timeline_fallback(query: str, articles: list[dict]) -> dict:
    sorted_articles = sorted(articles, key=lambda article: article.get("published_at", ""))
    return {
        "title": f"Timeline: {query}",
        "summary": "Chronology generated directly from the stored article corpus.",
        "events": [
            {
                "date": article.get("published_at", ""),
                "headline": article.get("title", "Untitled"),
                "description": article.get("description", "No description available."),
                "significance": "MEDIUM",
                "source": article.get("source", "Unknown source"),
            }
            for article in sorted_articles[:10]
        ],
    }


def build_topic_briefing(topic: str, force_refresh: bool = False) -> dict | None:
    if topic not in BRIEFING_TOPICS:
        raise HTTPException(status_code=400, detail=f"Topic must be one of {BRIEFING_TOPICS}")

    corpus_topic = _corpus_topic_for_briefing(topic)

    if not force_refresh:
        cached = load_briefing(topic, ttl=BRIEFING_TTL)
        if cached:
            return cached

    _ensure_topic_corpus(corpus_topic)
    articles = ensure_article_translations(
        get_recent_articles(topic=corpus_topic, limit=72, hours=CORPUS_WINDOW_HOURS, headline_corpus_only=True),
        max_articles=12,
    )
    if not articles:
        return None

    events = enrich_events(cluster_articles(articles, topic=corpus_topic))
    signals = format_signals_for_briefing(corpus_topic)
    event_brief = format_event_brief(events)
    contradictions = format_contradictions_for_briefing(events)

    if REQUEST_ENABLE_LLM_RESPONSES and os.getenv("GROQ_API_KEY"):
        try:
            briefing = generate_briefing(topic, articles, signals, contradictions, event_brief)
        except Exception as exc:
            print(f"[briefing] LLM generation failed for '{topic}', using fallback: {exc}")
            briefing = _briefing_fallback(topic, articles, events, signals=signals, contradictions=contradictions)
    else:
        briefing = _briefing_fallback(topic, articles, events, signals=signals, contradictions=contradictions)

    save_briefing(topic, briefing, articles, len(articles), events=events)
    cached = load_briefing(topic, ttl=BRIEFING_TTL)
    generated_at = cached["generated_at"] if cached else time.time()
    predictions = extract_predictions_from_briefing(
        topic=topic,
        briefing_text=briefing,
        source_ref=f"{topic}:{int(generated_at)}",
        generated_at=generated_at,
        events=events,
    )
    if predictions:
        delete_prediction_records(topic=topic, source_ref=f"{topic}:{int(generated_at)}")
        upsert_prediction_records(predictions)
    return cached or load_briefing(topic, ttl=BRIEFING_TTL)


def refresh_snapshot_layer():
    ingest_all_topics()
    rebuild_headlines_cache(use_llm=True)
    for topic in TOPICS:
        build_topic_briefing(topic, force_refresh=True)


def refresh_registry_sources():
    try:
        result = ingest_registry_sources()
        save_ingestion_state(
            "source-registry-refresh",
            "source-registry",
            "directfeeds",
            None,
            None,
            "ok",
            payload=result.get("totals", {}),
        )
        return result
    except Exception as exc:
        save_ingestion_state(
            "source-registry-refresh",
            "source-registry",
            "directfeeds",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[sources] Registry refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_direct_feed_layer():
    started_at = time.time()
    try:
        result = ingest_direct_feed_layer()
        totals = result.get("totals", {})
        promoted = int(totals.get("promoted_articles", 0) or 0)
        state_status = result.get("status", "ok")
        record_ingestion_run("directfeeds", "directfeeds", promoted, started_at, "ok" if state_status == "ok" else state_status)
        save_ingestion_state(
            "direct-feed-layer-refresh",
            "directfeeds",
            "directfeeds",
            None,
            None,
            "ok" if state_status == "ok" else state_status,
            payload=totals,
        )
        return result
    except Exception as exc:
        record_ingestion_run("directfeeds", "directfeeds", 0, started_at, "error", error=str(exc))
        save_ingestion_state(
            "direct-feed-layer-refresh",
            "directfeeds",
            "directfeeds",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[directfeeds] Layer refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def sync_registry_mirror():
    try:
        result = mirror_corpus_articles_into_registry(hours=SOURCE_REGISTRY_MIRROR_HOURS)
        save_ingestion_state(
            "source-registry-mirror",
            "source-registry-mirror",
            "registry-mirror",
            None,
            None,
            "ok",
            payload=result,
        )
        return result
    except Exception as exc:
        save_ingestion_state(
            "source-registry-mirror",
            "source-registry-mirror",
            "registry-mirror",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[sources] Registry mirror failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_official_updates():
    started_at = time.time()
    try:
        result = ingest_official_updates()
        record_ingestion_run("official-updates", "official", result["totals"]["official_updates"], started_at, "ok")
        save_ingestion_state(
            "official-updates-refresh",
            "official-updates",
            "official",
            None,
            None,
            "ok",
            payload=result.get("totals", {}),
        )
        return result
    except Exception as exc:
        record_ingestion_run("official-updates", "official", 0, started_at, "error", error=str(exc))
        save_ingestion_state(
            "official-updates-refresh",
            "official-updates",
            "official",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[official] Update refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_acled_events():
    started_at = time.time()
    try:
        result = ingest_acled_recent()
        record_ingestion_run("acled", "acled", result["inserted_or_updated"], started_at, "ok")
        save_ingestion_state(
            "structured-events-refresh",
            "acled",
            "acled",
            None,
            None,
            "ok",
            payload=result,
        )
        return result
    except Exception as exc:
        record_ingestion_run("acled", "acled", 0, started_at, "error", error=str(exc))
        save_ingestion_state(
            "structured-events-refresh",
            "acled",
            "acled",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[acled] Structured event refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_gdelt_gkg_events():
    started_at = time.time()
    try:
        result = ingest_gdelt_gkg_recent(hours=GDELT_GKG_REFRESH_HOURS)
        record_ingestion_run("gdelt_gkg", "gdelt_gkg", result["inserted_or_updated"], started_at, "ok")
        save_ingestion_state(
            "gdelt-gkg-events-refresh",
            "gdelt_gkg",
            "gdelt_gkg",
            None,
            None,
            "ok",
            payload=result,
        )
        _MAP_ATTENTION_CACHE.clear()
        _STORY_LOCATION_INDEX_CACHE.clear()
        return result
    except Exception as exc:
        record_ingestion_run("gdelt_gkg", "gdelt_gkg", 0, started_at, "error", error=str(exc))
        save_ingestion_state(
            "gdelt-gkg-events-refresh",
            "gdelt_gkg",
            "gdelt_gkg",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[gdelt_gkg] Event refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_recent_translations(limit: int = 18):
    started_at = time.time()
    if not REQUEST_ENABLE_TRANSLATION:
        return {"status": "disabled", "translated": 0}

    translated = 0
    failures = 0
    provider_counts = {}
    candidates = sorted(
        get_articles_missing_translation(limit=max(limit * 3, 48), hours=336),
        key=_article_translation_priority,
        reverse=True,
    )
    for article in candidates:
        if translated >= limit:
            break
        priority = _article_translation_priority(article)
        if priority < TRANSLATION_MIN_SCORE:
            continue
        try:
            translation = translate_article(
                article,
                allow_remote_fallback=priority >= TRANSLATION_REMOTE_FALLBACK_SCORE,
            )
            save_article_translation(
                article_url=article["url"],
                source_language=article.get("language") or "unknown",
                translated_title=translation["translated_title"],
                translated_description=translation.get("translated_description"),
                translation_provider=translation.get("provider", "translation"),
                target_language=translation.get("target_language", "en"),
            )
            translated += 1
            provider = translation.get("provider", "translation")
            provider_counts[provider] = provider_counts.get(provider, 0) + 1
        except Exception as exc:
            failures += 1
            print(f"[translation] Refresh failed for {article.get('url')}: {exc}")

    status = "ok" if failures == 0 else "partial"
    provider_label = "+".join(sorted(provider_counts)) if provider_counts else "selective-translation"
    record_ingestion_run("translations", provider_label, translated, started_at, status, error=None if failures == 0 else f"{failures} translations failed")
    return {"status": status, "translated": translated, "failed": failures, "providers": provider_counts}


def refresh_source_reliability():
    started_at = time.time()
    try:
        global_snapshot = build_claim_resolution_snapshot(topic=None, days=180)
        topic_snapshots = {topic: build_claim_resolution_snapshot(topic=topic, days=180) for topic in TOPICS}
        total_claims = int(global_snapshot.get("claim_records", 0) or 0)
        record_ingestion_run("source-reliability", "claim-resolution", total_claims, started_at, "ok")
        result = {
            "status": "ok",
            "global_claim_records": total_claims,
            "global_sources": len(global_snapshot.get("sources", [])),
            "topics": {
                topic: {
                    "claim_records": snapshot.get("claim_records", 0),
                    "sources": len(snapshot.get("sources", [])),
                }
                for topic, snapshot in topic_snapshots.items()
            },
        }
        save_ingestion_state(
            "source-reliability-refresh",
            "source-reliability",
            "claim-resolution",
            None,
            None,
            "ok",
            payload=result,
        )
        return result
    except Exception as exc:
        record_ingestion_run("source-reliability", "claim-resolution", 0, started_at, "error", error=str(exc))
        save_ingestion_state(
            "source-reliability-refresh",
            "source-reliability",
            "claim-resolution",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        return {"status": "error", "error": str(exc)}


def refresh_foresight_layer():
    started_at = time.time()
    try:
        prediction_snapshot = load_prediction_ledger(refresh=True, limit=250)
        archive_snapshot = load_early_signal_archive(limit=100, minimum_gap_hours=4)
        record_ingestion_run(
            "foresight",
            "foresight",
            len(prediction_snapshot.get("predictions", [])) + archive_snapshot.get("count", 0),
            started_at,
            "ok",
        )
        result = {
            "status": "ok",
            "predictions": prediction_snapshot.get("counts", {}),
            "early_signal_count": archive_snapshot.get("count", 0),
        }
        save_ingestion_state(
            "foresight-layer-refresh",
            "foresight",
            "foresight",
            None,
            None,
            "ok",
            payload=result,
        )
        return result
    except Exception as exc:
        record_ingestion_run("foresight", "foresight", 0, started_at, "error", error=str(exc))
        save_ingestion_state(
            "foresight-layer-refresh",
            "foresight",
            "foresight",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        return {"status": "error", "error": str(exc)}


def refresh_narrative_drift_layer():
    started_at = time.time()
    targets: list[tuple[str, str]] = []
    seen_targets: set[tuple[str, str]] = set()

    for topic in TOPICS:
        for entity in get_top_entities(topic=topic, days=21, limit=max(1, NARRATIVE_DRIFT_TOP_SUBJECTS)):
            subject = (entity.get("entity") or "").strip()
            if len(subject) < 3:
                continue
            target_key = (topic, subject.lower())
            if target_key in seen_targets:
                continue
            seen_targets.add(target_key)
            targets.append((topic, subject))

    if not targets:
        result = {"status": "empty", "subjects": 0, "snapshots": []}
        record_ingestion_run("narrative-drift", "framing", 0, started_at, "empty", error="No top entities available for drift analysis.")
        save_ingestion_state(
            "narrative-drift-refresh",
            "narrative-drift",
            "framing",
            None,
            None,
            "empty",
            error="No top entities available for drift analysis.",
            payload=result,
        )
        return result

    try:
        snapshots = []
        for topic, subject in targets:
            payload = analyze_narrative_drift(subject, topic=topic, days=180, refresh=True)
            snapshots.append(
                {
                    "topic": topic,
                    "subject": subject,
                    "article_count": int(payload.get("article_count", 0) or 0),
                    "shift_count": len(payload.get("shifts", []) or []),
                }
            )

        populated = sum(1 for snapshot in snapshots if snapshot["article_count"] > 0)
        status = "ok" if populated else "empty"
        result = {
            "status": status,
            "subjects": len(snapshots),
            "populated_subjects": populated,
            "snapshots": snapshots,
        }
        record_ingestion_run("narrative-drift", "framing", populated, started_at, status)
        save_ingestion_state(
            "narrative-drift-refresh",
            "narrative-drift",
            "framing",
            None,
            None,
            status,
            payload=result,
        )
        return result
    except Exception as exc:
        record_ingestion_run("narrative-drift", "framing", 0, started_at, "error", error=str(exc))
        save_ingestion_state(
            "narrative-drift-refresh",
            "narrative-drift",
            "framing",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        return {"status": "error", "error": str(exc)}


def _schedule_initial_analytics_warm(scheduler: BackgroundScheduler) -> None:
    if not INTERNAL_SCHEDULER_ENABLED:
        return

    warm_jobs = [
        ("refresh_snapshots_initial", refresh_snapshot_layer, ANALYTICS_WARM_DELAY_SECONDS),
        ("refresh_source_reliability_initial", refresh_source_reliability, ANALYTICS_WARM_DELAY_SECONDS + 30),
        ("refresh_foresight_initial", refresh_foresight_layer, ANALYTICS_WARM_DELAY_SECONDS + 60),
        ("refresh_narrative_drift_initial", refresh_narrative_drift_layer, ANALYTICS_WARM_DELAY_SECONDS + 90),
        ("refresh_acled_initial", refresh_acled_events, ANALYTICS_WARM_DELAY_SECONDS + 120),
        ("refresh_gdelt_gkg_initial", refresh_gdelt_gkg_events, ANALYTICS_WARM_DELAY_SECONDS + 150),
    ]
    for job_id, job_func, delay_seconds in warm_jobs:
        scheduler.add_job(
            job_func,
            "date",
            run_date=datetime.now(timezone.utc) + timedelta(seconds=max(0, delay_seconds)),
            id=job_id,
            replace_existing=True,
        )


def build_scheduler(
    include_ingestion: bool = True,
    include_translations: bool = True,
    include_analytics: bool = True,
) -> BackgroundScheduler:
    scheduler = BackgroundScheduler(
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 300,
        }
    )
    if include_ingestion:
        scheduler.add_job(run_scheduled_ingest_cycle, "interval", minutes=15, id="refresh_corpus")
        scheduler.add_job(run_scheduled_gdelt_backfill, "interval", minutes=10, id="gdelt_backfill")
        scheduler.add_job(refresh_direct_feed_layer, "interval", minutes=DIRECT_FEED_REFRESH_MINUTES, id="refresh_directfeeds")
        scheduler.add_job(run_scheduled_historical_queue_fetch, "interval", minutes=HISTORICAL_FETCH_REFRESH_MINUTES, id="historical_queue_fetch")
        scheduler.add_job(ingest_article_fallback, "interval", minutes=ARTICLE_FALLBACK_REFRESH_MINUTES, id="article_fallback")
        scheduler.add_job(sync_registry_mirror, "interval", hours=6, id="mirror_registry_articles")
        scheduler.add_job(refresh_official_updates, "interval", minutes=OFFICIAL_UPDATE_REFRESH_MINUTES, id="refresh_official_updates")
        scheduler.add_job(refresh_acled_events, "interval", minutes=ACLED_REFRESH_MINUTES, id="refresh_acled_events")
        scheduler.add_job(refresh_gdelt_gkg_events, "interval", minutes=GDELT_GKG_REFRESH_MINUTES, id="refresh_gdelt_gkg_events")
        scheduler.add_job(
            run_scheduled_story_materialization,
            "interval",
            minutes=STORY_MATERIALIZATION_REFRESH_MINUTES,
            id="materialize_story_clusters",
        )
    if include_translations:
        scheduler.add_job(refresh_recent_translations, "interval", minutes=30, id="refresh_translations")
    if include_analytics:
        scheduler.add_job(refresh_source_reliability, "interval", minutes=SOURCE_RELIABILITY_REFRESH_MINUTES, id="refresh_source_reliability")
        scheduler.add_job(refresh_foresight_layer, "interval", minutes=FORESIGHT_REFRESH_MINUTES, id="refresh_foresight_layer")
        scheduler.add_job(refresh_narrative_drift_layer, "interval", minutes=NARRATIVE_DRIFT_REFRESH_MINUTES, id="refresh_narrative_drift_layer")
        scheduler.add_job(refresh_snapshot_layer, "interval", hours=1, id="refresh_snapshots")
    return scheduler


def build_worker_scheduler() -> BackgroundScheduler:
    return build_scheduler(
        include_ingestion=WORKER_ENABLE_INGESTION,
        include_translations=WORKER_ENABLE_TRANSLATIONS,
        include_analytics=WORKER_ENABLE_ANALYTICS,
    )


scheduler = build_scheduler()

for router in (
    health_router,
    analytics_router,
    briefings_router,
    events_router,
    headlines_router,
    query_router,
    entities_router,
):
    app.include_router(router)


@app.on_event("startup")
def startup():
    init_cache_db()
    init_corpus_db()
    seed_sources()
    state = runtime_status()
    if state["corpus"]["total_articles"] == 0:
        bootstrap_from_legacy_cache()
    _schedule_initial_analytics_warm(scheduler)
    if INTERNAL_SCHEDULER_ENABLED and not scheduler.running:
        scheduler.start()


@app.on_event("shutdown")
def shutdown():
    if scheduler.running:
        scheduler.shutdown()
