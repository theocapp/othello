"""Persist cross-source story clusters for analytics and APIs."""

from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timedelta

from contradictions import cluster_articles, enrich_events, event_cluster_key
from corpus import (
    get_recent_articles,
    get_source_registry,
    get_structured_event_coordinates_by_ids,
    list_structured_event_ids_in_date_range,
    load_claim_resolution_for_event_key,
    load_framing_signals_for_article_urls,
    load_latest_source_reliability,
    replace_materialized_story_clusters,
    upsert_canonical_events,
    upsert_event_perspectives,
)

DEFAULT_TOPICS = ("geopolitics", "economics")


_COUNTRY_ALIASES = {
    "usa": "united states",
    "us": "united states",
    "u.s.": "united states",
    "u.s.a.": "united states",
    "united states of america": "united states",
    "uk": "united kingdom",
    "u.k.": "united kingdom",
    "great britain": "united kingdom",
    "britain": "united kingdom",
    "england": "united kingdom",
    "london": "united kingdom",
    "russian federation": "russia",
    "moscow": "russia",
    "iran": "iran",
    "tehran": "iran",
    "china": "china",
    "people's republic of china": "china",
    "prc": "china",
    "beijing": "china",
    "syrian arab republic": "syria",
    "damascus": "syria",
    "kyiv": "ukraine",
    "kiev": "ukraine",
    "ankara": "turkey",
    "uae": "united arab emirates",
    "ivory coast": "cote d'ivoire",
    "côte d'ivoire": "cote d'ivoire",
    "drc": "dr congo",
    "zaire": "dr congo",
    "kinshasa": "dr congo",
    "republic of the congo": "congo",
    "brazzaville": "congo",
    "south korea": "south korea",
    "republic of korea": "south korea",
    "rok": "south korea",
    "north korea": "north korea",
    "pyongyang": "north korea",
}

_COUNTRY_CANONICAL_NAMES = frozenset(_COUNTRY_ALIASES.values())
_COUNTRY_NAME_PATTERN = re.compile(
    r"\b(" + "|".join(sorted({re.escape(name) for name in set(_COUNTRY_ALIASES) | _COUNTRY_CANONICAL_NAMES}, key=len, reverse=True)) + r")\b",
    flags=re.I,
)


def _normalize_country(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = str(name).strip().lower()
    if cleaned.startswith("the "):
        cleaned = cleaned[4:]
    return _COUNTRY_ALIASES.get(cleaned, cleaned)


def _countries_from_text(text: str | None) -> list[str]:
    if not text:
        return []
    countries: list[str] = []
    for raw in _COUNTRY_NAME_PATTERN.findall(text):
        normalized = _normalize_country(raw)
        if normalized and normalized not in countries:
            countries.append(normalized)
    return countries


def _date_range_for_cluster(event: dict, padding_days: int = 2) -> tuple[str | None, str | None]:
    def to_date(raw: str | None):
        if not raw or not str(raw).strip():
            return None
        text = str(raw).strip()
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt.date()
        except ValueError:
            return None

    earliest = to_date(event.get("earliest_update")) or to_date(event.get("latest_update"))
    latest = to_date(event.get("latest_update")) or earliest
    if earliest is None:
        return None, None
    if latest is None:
        latest = earliest
    start = earliest - timedelta(days=padding_days)
    end = latest + timedelta(days=padding_days)
    return start.isoformat(), end.isoformat()


def _story_country_preferences(event: dict) -> list[str]:
    preferences: list[str] = []
    for raw_focus in event.get("entity_focus") or []:
        if not raw_focus or not str(raw_focus).strip():
            continue
        normalized = _normalize_country(str(raw_focus))
        canonical = _COUNTRY_ALIASES.get(normalized, normalized)
        if canonical in _COUNTRY_CANONICAL_NAMES and canonical not in preferences:
            preferences.append(canonical)

    for country in _countries_from_text(event.get("label")) + _countries_from_text(event.get("summary")):
        if country not in preferences:
            preferences.append(country)

    if "united states" in preferences and len(preferences) > 1:
        preferences = [country for country in preferences if country != "united states"]
    return preferences


def _link_structured_ids(event: dict) -> list[str]:
    start, end = _date_range_for_cluster(event)
    if not start or not end:
        return []

    candidate_ids = list_structured_event_ids_in_date_range(start, end, limit=80)
    if not candidate_ids:
        return []

    preferred_countries = _story_country_preferences(event)
    if not preferred_countries:
        return candidate_ids

    coord_by_id = get_structured_event_coordinates_by_ids(candidate_ids)
    filtered_ids = []
    for event_id, meta in coord_by_id.items():
        country = _normalize_country(meta.get("country"))
        if country and country in preferred_countries:
            filtered_ids.append(event_id)
    return filtered_ids or candidate_ids


def _perspective_id(event_id: str, article_url: str) -> str:
    return hashlib.sha256(f"{event_id}|{article_url}".encode()).hexdigest()


def _infer_event_type(event: dict) -> str | None:
    label = (event.get("label") or "").lower()
    anchors = set(event.get("anchors") or [])
    if "strike" in anchors or "ceasefire" in anchors or any(w in label for w in ("war", "attack", "conflict", "military", "troops", "missile")):
        return "conflict"
    if "sanctions" in anchors or "market" in anchors or any(w in label for w in ("trade", "tariff", "economy", "inflation", "rates", "gdp")):
        return "economic"
    if "meeting" in anchors or "vote" in anchors or any(w in label for w in ("election", "summit", "diplomacy", "treaty", "talks")):
        return "diplomatic"
    if "aid" in anchors or any(w in label for w in ("humanitarian", "relief", "refugee")):
        return "humanitarian"
    if "filing" in anchors or "detention" in anchors or any(w in label for w in ("legal", "court", "indictment", "arrest")):
        return "legal"
    return "political"


def _build_canonical_row(event: dict, topic: str, linked: list[str], urls: list[str], cluster_key: str) -> dict:
    articles = event.get("articles") or []
    sources = {(a.get("source") or "").strip() for a in articles if (a.get("source") or "").strip()}
    published_dates = [
        a.get("published_at") for a in articles if a.get("published_at")
    ]
    first_reported = min(published_dates) if published_dates else None
    last_updated = max(published_dates) if published_dates else None
    return {
        "event_id": cluster_key,
        "topic": topic,
        "label": event.get("label") or "",
        "event_type": _infer_event_type(event),
        "status": "developing",
        "first_reported_at": first_reported,
        "last_updated_at": last_updated,
        "article_count": len(urls),
        "source_count": len(sources),
        "perspective_count": 0,
        "contradiction_count": len(event.get("contradictions") or []),
        "linked_structured_event_ids": linked,
        "article_urls": urls,
        "payload": {
            "summary": event.get("summary"),
            "entity_focus": event.get("entity_focus") or [],
            "anchors": list(event.get("anchors") or []),
        },
    }


def _build_perspective_rows(
    event_id: str,
    articles: list[dict],
    framing_by_url: dict[str, dict],
    claims_by_source: dict[str, dict],
    reliability_by_source: dict[str, dict],
    registry_by_domain: dict[str, dict],
) -> list[dict]:
    rows = []
    for article in articles:
        url = (article.get("url") or "").strip()
        if not url:
            continue
        source_name = (article.get("source") or "").strip()
        source_domain = (article.get("source_domain") or "").strip()
        framing = framing_by_url.get(url) or {}
        reliability = reliability_by_source.get(source_name.lower()) or {}
        reg = registry_by_domain.get(source_domain) or {}
        claim = claims_by_source.get(source_name.lower()) or {}
        rows.append(
            {
                "perspective_id": _perspective_id(event_id, url),
                "event_id": event_id,
                "article_url": url,
                "source_name": source_name,
                "source_domain": source_domain or None,
                "source_reliability_score": reliability.get("empirical_score"),
                "source_trust_tier": reg.get("trust_tier"),
                "source_region": reg.get("region"),
                "dominant_frame": framing.get("dominant_frame"),
                "frame_counts": framing.get("frame_counts") or {},
                "matched_terms": framing.get("matched_terms") or [],
                "claim_text": claim.get("claim_text"),
                "claim_type": claim.get("claim_type"),
                "claim_resolution_status": claim.get("resolution_status"),
                "published_at": article.get("published_at"),
                "analyzed_at": time.time(),
                "payload": {},
            }
        )
    return rows


def rebuild_materialized_story_clusters(
    *,
    topics: list[str] | None = None,
    window_hours: int = 96,
    articles_limit: int = 120,
) -> dict:
    topic_list = list(topics or DEFAULT_TOPICS)
    window_hours = max(1, int(window_hours))
    total_rows = 0
    detail: list[dict] = []

    # load source metadata once for all topics
    reliability_by_source = load_latest_source_reliability()
    registry_entries = get_source_registry(active_only=False)
    registry_by_domain = {(e.get("source_domain") or "").lower(): e for e in registry_entries if e.get("source_domain")}

    for topic in topic_list:
        articles = get_recent_articles(
            topic=topic,
            limit=articles_limit,
            hours=window_hours,
            headline_corpus_only=True,
        )
        if not articles:
            replace_materialized_story_clusters(topic=topic, window_hours=window_hours, rows=[])
            detail.append({"topic": topic, "clusters": 0})
            continue
        events = enrich_events(cluster_articles(articles, topic=topic))
        legacy_rows = []
        canonical_rows = []
        all_perspective_rows = []

        for event in events:
            cluster_key = event_cluster_key(event)
            linked = _link_structured_ids(event)
            urls = sorted(
                {(a.get("url") or "").strip() for a in event.get("articles", []) if (a.get("url") or "").strip()}
            )

            # legacy table (unchanged)
            legacy_rows.append(
                {
                    "cluster_key": cluster_key,
                    "label": event.get("label") or "",
                    "summary": event.get("summary"),
                    "earliest_published_at": event.get("earliest_update"),
                    "latest_published_at": event.get("latest_update"),
                    "article_urls": urls,
                    "linked_structured_event_ids": linked,
                    "event_payload": event,
                }
            )

            # canonical event
            canonical_rows.append(_build_canonical_row(event, topic, linked, urls, cluster_key))

            # perspectives: load framing + claim resolution for this cluster
            framing_by_url = load_framing_signals_for_article_urls(urls)
            claim_records = load_claim_resolution_for_event_key(cluster_key)
            claims_by_source = {
                r["source_name"].lower(): r for r in claim_records
            }
            event_articles = event.get("articles") or []
            perspective_rows = _build_perspective_rows(
                cluster_key,
                event_articles,
                framing_by_url,
                claims_by_source,
                reliability_by_source,
                registry_by_domain,
            )
            all_perspective_rows.extend(perspective_rows)

        total_rows += replace_materialized_story_clusters(topic=topic, window_hours=window_hours, rows=legacy_rows)
        upsert_canonical_events(canonical_rows)
        upsert_event_perspectives(all_perspective_rows)

        detail.append({"topic": topic, "clusters": len(legacy_rows)})

    return {"topics": topic_list, "window_hours": window_hours, "rows_written": total_rows, "detail": detail}
