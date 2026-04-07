import hashlib
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

# Load environment from backend/.env by default (non-destructive)
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)


def _database_url() -> str | None:
    if os.getenv("OTHELLO_DATABASE_URL"):
        return os.getenv("OTHELLO_DATABASE_URL")

    host = os.getenv("OTHELLO_PGHOST")
    dbname = os.getenv("OTHELLO_PGDATABASE")
    user = os.getenv("OTHELLO_PGUSER")
    if not (host and dbname and user):
        return None

    password = os.getenv("OTHELLO_PGPASSWORD")
    port = os.getenv("OTHELLO_PGPORT", "5432")
    if host.startswith("/"):
        auth = quote_plus(user)
        if password:
            auth = f"{auth}:{quote_plus(password)}"
        return f"postgresql://{auth}@/{dbname}?host={quote_plus(host)}&port={port}"
    auth = quote_plus(user)
    if password:
        auth = f"{auth}:{quote_plus(password)}"
    return f"postgresql://{auth}@{host}:{port}/{dbname}"


@contextmanager
def _connect():
    conn = psycopg.connect(_database_url(), row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _canonical_url(url: str) -> str:
    parsed = urlparse((url or "").strip())
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def _domain(url: str) -> str:
    return urlparse((url or "").strip()).netloc.lower()


def _content_hash(article: dict) -> str:
    material = " | ".join(
        [
            article.get("title", "").strip(),
            article.get("description", "").strip(),
            article.get("source", "").strip(),
            article.get("published_at", "").strip(),
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _stable_hash(parts: list[str]) -> str:
    material = " | ".join((part or "").strip() for part in parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _canonical_raw_document_id(source_id: str, content_hash: str) -> str:
    return _stable_hash([source_id, content_hash])[:24]


def _normalize_entity_key(entity: str) -> str:
    return " ".join((entity or "").strip().lower().split())


def _normalize_historical_url_record(record: dict) -> dict:
    url = (record.get("url") or "").strip()
    if not url:
        raise ValueError("Historical queue record is missing url")

    topic_guess = (record.get("topic_guess") or "").strip() or None
    if topic_guess and topic_guess not in {"geopolitics", "economics"}:
        topic_guess = None

    normalized = {
        "url": url,
        "canonical_url": _canonical_url(url),
        "title": (record.get("title") or "").strip() or None,
        "source_name": (record.get("source_name") or "").strip() or None,
        "source_domain": (
            (record.get("source_domain") or _domain(url)).strip() or None
        ),
        "published_at": (record.get("published_at") or "").strip() or None,
        "language": (record.get("language") or "").strip() or None,
        "discovered_via": (record.get("discovered_via") or "gdelt-bulk").strip(),
        "topic_guess": topic_guess,
        "gdelt_query": (record.get("gdelt_query") or "").strip() or None,
        "gdelt_window_start": (record.get("gdelt_window_start") or "").strip() or None,
        "gdelt_window_end": (record.get("gdelt_window_end") or "").strip() or None,
        "fetch_status": (record.get("fetch_status") or "pending").strip() or "pending",
        "last_attempt_at": record.get("last_attempt_at"),
        "attempt_count": int(record.get("attempt_count") or 0),
    }
    payload = record.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    normalized["payload"] = {**payload, **normalized}
    return normalized


def _coerce_timestamptz(raw: str) -> str:
    """Best-effort coerce of published_at into ISO-8601 with timezone for TIMESTAMPTZ columns."""
    raw = (raw or "").strip()
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    # Already has timezone info → pass through
    if raw.endswith("Z") or "+" in raw[10:] or raw[10:].count("-") > 1:
        return raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    # Bare datetime → assume UTC
    return raw + "+00:00"


def _parse_published_at(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    compact = (
        datetime.strptime(text, "%Y%m%dT%H%M%SZ")
        if len(text) == 16 and text.endswith("Z")
        else None
    )
    if compact:
        return compact.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_article_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
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


def _row_to_article(row) -> dict:
    payload = row.get("payload")
    if isinstance(payload, str):
        payload = json.loads(payload)
    elif payload is None:
        payload = {}
    translated_title = row["translated_title"] if "translated_title" in row else None
    translated_description = (
        row["translated_description"] if "translated_description" in row else None
    )
    original_title = payload.get("title") or row["title"]
    original_description = payload.get("description") or row["description"]
    return {
        "title": translated_title or original_title,
        "description": translated_description or original_description,
        "original_title": original_title,
        "original_description": original_description,
        "translated_title": translated_title,
        "translated_description": translated_description,
        "source": payload.get("source") or row["source"],
        "source_domain": payload.get("source_domain") or row["source_domain"],
        "url": row["url"],
        "published_at": row["published_at"],
        "language": row.get("language"),
        "provider": row.get("provider"),
        "translation_source_language": row.get("translation_source_language"),
        "translation_target_language": row.get("translation_target_language"),
        "translation_provider": row.get("translation_provider"),
        "translated_at": row.get("translated_at"),
    }


def _row_to_structured_event(row) -> dict:
    source_urls = row.get("source_urls")
    payload = row.get("payload")
    if isinstance(source_urls, str):
        source_urls = json.loads(source_urls) if source_urls else []
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "event_id": row["event_id"],
        "dataset": row["dataset"],
        "dataset_event_id": row.get("dataset_event_id"),
        "event_date": row.get("event_date"),
        "country": row.get("country"),
        "region": row.get("region"),
        "admin1": row.get("admin1"),
        "admin2": row.get("admin2"),
        "location": row.get("location"),
        "latitude": row.get("latitude"),
        "longitude": row.get("longitude"),
        "event_type": row.get("event_type"),
        "sub_event_type": row.get("sub_event_type"),
        "actor_primary": row.get("actor_primary"),
        "actor_secondary": row.get("actor_secondary"),
        "fatalities": row.get("fatalities"),
        "source_count": row.get("source_count"),
        "source_urls": source_urls or [],
        "summary": row.get("summary"),
        "payload": payload or {},
        "first_ingested_at": row.get("first_ingested_at"),
        "last_ingested_at": row.get("last_ingested_at"),
    }


def _row_to_canonical_event(row) -> dict:
    if not row:
        return None
    article_urls = row.get("article_urls")
    linked = row.get("linked_structured_event_ids")
    payload = row.get("payload")
    if isinstance(article_urls, str):
        article_urls = json.loads(article_urls) if article_urls else []
    if isinstance(linked, str):
        linked = json.loads(linked) if linked else []
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "event_id": row["event_id"],
        "topic": row.get("topic"),
        "label": row.get("label"),
        "event_type": row.get("event_type"),
        "status": row.get("status"),
        "geo_country": row.get("geo_country"),
        "geo_region": row.get("geo_region"),
        "latitude": row.get("latitude"),
        "longitude": row.get("longitude"),
        "first_reported_at": row.get("first_reported_at"),
        "last_updated_at": row.get("last_updated_at"),
        "article_count": int(row.get("article_count") or 0),
        "source_count": int(row.get("source_count") or 0),
        "perspective_count": int(row.get("perspective_count") or 0),
        "contradiction_count": int(row.get("contradiction_count") or 0),
        "neutral_summary": row.get("neutral_summary"),
        "neutral_confidence": row.get("neutral_confidence"),
        "neutral_generated_at": row.get("neutral_generated_at"),
        "linked_structured_event_ids": linked or [],
        "article_urls": article_urls or [],
        "first_seen_at": row.get("first_seen_at"),
        "computed_at": row.get("computed_at"),
        "payload": payload or {},
    }


def _row_to_perspective(row) -> dict:
    frame_counts = row.get("frame_counts")
    matched_terms = row.get("matched_terms")
    payload = row.get("payload")
    if isinstance(frame_counts, str):
        frame_counts = json.loads(frame_counts) if frame_counts else {}
    if isinstance(matched_terms, str):
        matched_terms = json.loads(matched_terms) if matched_terms else []
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "perspective_id": row["perspective_id"],
        "event_id": row["event_id"],
        "article_url": row.get("article_url"),
        "source_name": row.get("source_name"),
        "source_domain": row.get("source_domain"),
        "source_reliability_score": row.get("source_reliability_score"),
        "source_trust_tier": row.get("source_trust_tier"),
        "source_region": row.get("source_region"),
        "dominant_frame": row.get("dominant_frame"),
        "frame_counts": frame_counts or {},
        "matched_terms": matched_terms or [],
        "claim_text": row.get("claim_text"),
        "claim_type": row.get("claim_type"),
        "claim_resolution_status": row.get("claim_resolution_status"),
        "sentiment": row.get("sentiment"),
        "published_at": row.get("published_at"),
        "analyzed_at": row.get("analyzed_at"),
        "payload": payload or {},
    }


def _row_to_historical_queue_item(row) -> dict:
    payload = row.get("payload")
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    elif payload is None:
        payload = {}
    return {
        "url": row.get("url"),
        "canonical_url": row.get("canonical_url"),
        "title": row.get("title"),
        "source_name": row.get("source_name"),
        "source_domain": row.get("source_domain"),
        "published_at": row.get("published_at"),
        "language": row.get("language"),
        "discovered_via": row.get("discovered_via"),
        "topic_guess": row.get("topic_guess"),
        "gdelt_query": row.get("gdelt_query"),
        "gdelt_window_start": row.get("gdelt_window_start"),
        "gdelt_window_end": row.get("gdelt_window_end"),
        "fetch_status": row.get("fetch_status"),
        "last_attempt_at": row.get("last_attempt_at"),
        "attempt_count": int(row.get("attempt_count") or 0),
        "payload": payload,
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def _headline_corpus_sql_filter(table_alias: str = "a") -> str:
    return (
        f" AND ({table_alias}.payload->>'analytic_tier' IS NULL OR "
        f"{table_alias}.payload->>'analytic_tier' IN ('', 'headline'))"
    )
