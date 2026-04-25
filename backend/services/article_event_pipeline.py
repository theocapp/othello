from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any

from core.config import TOPICS
from db.common import _connect
from entities import extract_entities
from geo_constants import COUNTRY_CENTROIDS, STORY_REGION_CENTROIDS
from ranking.article_quality import article_quality_score
from services.canonical_events_pipeline import (
    _upsert_canonical_event,
    _upsert_event_perspectives,
    _upsert_identity_map,
)


_PIPELINE_LOCK = threading.Lock()
# Strong signals: any single match in title+description is sufficient for Conflict.
_CONFLICT_STRONG = frozenset({
    "killed", "airstrike", "air strike", "shelling", "missile strike", "rocket attack",
    "suicide bombing", "car bomb", "roadside bomb", "ied ", "fatalities", "death toll",
    "civilians killed", "soldiers killed", "troops killed", "fighters killed",
    "ceasefire", "offensive", "counteroffensive", "siege", "invasion", "incursion",
    "bombardment", "artillery fire", "clashes erupted", "fighting broke",
    "combat", "ambush", "gunfight", "firefight", "drone strike", "bombing campaign",
    "armed clashes", "armed attack", "mass killings", "war crimes",
})
# Weak signals: require at least two matches across the cluster.
_CONFLICT_WEAK = frozenset({
    "military operation", "armed forces clashed", "rebel group", "militant group",
    "insurgent", "troop deployment", "guerrilla", "military offensive",
    "security forces fired", "weapons seized", "troops deployed",
})
_CLUSTER_THRESHOLD = 2.5


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _week_floor_from_datetime(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    monday = value.date() - timedelta(days=value.date().weekday())
    return monday.isoformat()


def _article_primary_gpe(article: dict) -> str | None:
    for entity in article.get("_entities") or []:
        if entity.get("type") == "GPE" and _clean(entity.get("entity")):
            return _clean(entity.get("entity"))
    return None


def _resolve_article_geo(
    title: str, description: str, entities: list[dict] | None = None
) -> tuple[str | None, float | None, float | None]:
    """
    Returns (country, latitude, longitude) or (None, None, None).
    Accepts pre-computed entities to avoid a second NER pass.
    """
    text = _clean(f"{title} {description}")
    if not text:
        return None, None, None

    if entities is None:
        entities = extract_entities(text, language="en")

    for entity in entities:
        if entity.get("type") != "GPE":
            continue
        normalized = _clean(entity.get("entity"))
        if not normalized:
            continue
        centroid = COUNTRY_CENTROIDS.get(normalized.lower())
        if centroid:
            return (
                centroid.get("country") or centroid.get("label") or normalized,
                centroid.get("latitude"),
                centroid.get("longitude"),
            )

    lowered = text.lower()
    for key, centroid in sorted(COUNTRY_CENTROIDS.items(), key=lambda item: len(item[0]), reverse=True):
        if key and key in lowered:
            return (
                centroid.get("country") or centroid.get("label") or key,
                centroid.get("latitude"),
                centroid.get("longitude"),
            )

    for key, centroid in sorted(STORY_REGION_CENTROIDS.items(), key=lambda item: len(item[0]), reverse=True):
        if key and key in lowered:
            return (
                centroid.get("country") or centroid.get("label") or key,
                centroid.get("latitude"),
                centroid.get("longitude"),
            )

    return None, None, None


def _article_cluster_anchor(country: str, primary_entity: str, week_floor: str) -> str:
    """
    Deterministic deduplication key for an article-derived event.
    Compute SHA-256 of f"{country.lower()}|{primary_entity.lower()}|{week_floor}"
    Return "art-" + hexdigest[:24]
    week_floor: ISO date string of the Monday of the article's publication week
    primary_entity: the most-mentioned GPE or ORG entity in the cluster
    """

    material = "|".join(
        [
            _clean(country).lower(),
            _clean(primary_entity).lower(),
            _clean(week_floor),
        ]
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
    return f"art-{digest}"


def _published_dt(article: dict) -> datetime | None:
    return _parse_iso(article.get("published_at"))


def _entity_names(article: dict, entity_type: str) -> list[str]:
    values = []
    for entity in article.get("_entities") or []:
        if entity.get("type") != entity_type:
            continue
        normalized = _clean(entity.get("entity"))
        if normalized:
            values.append(normalized)
    return values


def _cluster_match_score(article: dict, cluster_seed: dict, cluster_members: list[dict]) -> float:
    article_country = _clean(article.get("_country")) or None
    seed_country = _clean(cluster_seed.get("_country")) or None
    if article_country and seed_country and article_country.lower() != seed_country.lower():
        return -999.0

    article_dt = _published_dt(article)
    nearest_gap_hours = None
    within_24h = False
    within_72h = False
    for member in cluster_members:
        member_dt = _published_dt(member)
        if article_dt is None or member_dt is None:
            continue
        gap_hours = abs((article_dt - member_dt).total_seconds()) / 3600.0
        if nearest_gap_hours is None or gap_hours < nearest_gap_hours:
            nearest_gap_hours = gap_hours
        if gap_hours <= 24:
            within_24h = True
        elif gap_hours <= 72:
            within_72h = True

    if nearest_gap_hours is not None and nearest_gap_hours > 24 * 7:
        return -999.0

    score = 0.0
    if article_country and seed_country and article_country.lower() == seed_country.lower():
        score += 3.0

    article_gpe = set(_entity_names(article, "GPE"))
    seed_gpe = set(_entity_names(cluster_seed, "GPE"))
    shared_gpe = len(article_gpe & seed_gpe)
    score += min(shared_gpe * 2.0, 4.0)

    article_social = set(_entity_names(article, "ORG")) | set(_entity_names(article, "PERSON"))
    seed_social = set(_entity_names(cluster_seed, "ORG")) | set(_entity_names(cluster_seed, "PERSON"))
    shared_social = len(article_social & seed_social)
    score += min(shared_social * 1.5, 3.0)

    if within_24h:
        score += 1.0
    elif within_72h:
        score += 0.5

    return score


def _cluster_articles(articles: list[dict]) -> list[list[dict]]:
    """
    Input: list of article dicts with keys: url, title, description, source_domain,
    published_at, language, _country, _entities.

    Algorithm - greedy single-pass:
    1. Sort articles by published_at ascending
    2. For each article, score it against all existing cluster seeds
    3. Place in the first cluster with match_score >= CLUSTER_THRESHOLD (use 2.5)
    4. Otherwise start a new cluster
    """

    ordered = sorted(
        articles,
        key=lambda article: (
            _published_dt(article) or datetime.min.replace(tzinfo=timezone.utc),
            _clean(article.get("url")),
        ),
    )
    clusters: list[list[dict]] = []
    for article in ordered:
        placed = False
        for cluster in clusters:
            seed = cluster[0]
            if _cluster_match_score(article, seed, cluster) >= _CLUSTER_THRESHOLD:
                cluster.append(article)
                placed = True
                break
        if not placed:
            clusters.append([article])
    return clusters


def _best_article_in_cluster(cluster: list[dict]) -> dict:
    """
    Select the highest-quality article from a cluster to be the event title/summary source.
    Use article_quality_score(article, topics=TOPICS) from ranking.article_quality.
    Return the article dict with the highest score.
    The selected article's title becomes resolved_title, description becomes resolved_summary.
    """

    return max(
        cluster,
        key=lambda article: (
            article_quality_score(article, topics=TOPICS),
            _published_dt(article) or datetime.min.replace(tzinfo=timezone.utc),
            _clean(article.get("title")),
        ),
    )


def _most_common_entity(cluster: list[dict], entity_type: str) -> str | None:
    values: list[str] = []
    for article in cluster:
        values.extend(_entity_names(article, entity_type))
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]


def _most_common_country(cluster: list[dict]) -> str | None:
    values = [_clean(article.get("_country")) for article in cluster if _clean(article.get("_country"))]
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]


def _cluster_geo(cluster: list[dict], best_article: dict) -> tuple[float | None, float | None, str | None]:
    best_lat = best_article.get("_lat")
    best_lon = best_article.get("_lon")
    if best_lat is not None and best_lon is not None:
        return best_lat, best_lon, _article_primary_gpe(best_article)

    lat_values = [float(article["_lat"]) for article in cluster if article.get("_lat") is not None]
    lon_values = [float(article["_lon"]) for article in cluster if article.get("_lon") is not None]
    lat = math.fsum(lat_values) / len(lat_values) if lat_values else None
    lon = math.fsum(lon_values) / len(lon_values) if lon_values else None
    geo_location = _article_primary_gpe(best_article)
    return lat, lon, geo_location


def _infer_event_type(cluster: list[dict]) -> str:
    strong_hits = 0
    weak_hits = 0
    for article in cluster:
        title = _clean(article.get("title") or "").lower()
        description = _clean(article.get("description") or "").lower()
        text = f"{title} {description}"
        if any(kw in text for kw in _CONFLICT_STRONG):
            strong_hits += 1
        elif any(kw in text for kw in _CONFLICT_WEAK):
            weak_hits += 1
    if strong_hits >= 1 or weak_hits >= 2:
        return "Conflict"
    return "Political"


def _source_count(cluster: list[dict]) -> int:
    sources = {
        _clean(article.get("source_domain") or article.get("source")).lower()
        for article in cluster
        if _clean(article.get("source_domain") or article.get("source"))
    }
    return len(sources)


def populate_canonical_events_from_articles(*, days: int = 3, limit: int = 2000) -> dict:
    """
    Full pipeline: articles -> clusters -> canonical_events.
    """

    if not _PIPELINE_LOCK.acquire(blocking=False):
        return {"status": "skipped", "reason": "article_event_pipeline already running"}

    try:
        safe_days = max(1, int(days))
        safe_limit = max(1, min(int(limit), 5000))
        cutoff = datetime.now(timezone.utc) - timedelta(days=safe_days)
        cutoff_iso = cutoff.isoformat()

        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT a.url, a.title, a.description, a.source, a.source_domain, a.published_at, a.language, a.payload
                FROM articles a
                WHERE a.published_at >= %s
                ORDER BY a.published_at DESC
                LIMIT %s
                """,
                (cutoff_iso, safe_limit),
            ).fetchall()

        enriched_articles: list[dict] = []
        for row in rows:
            title = _clean(row.get("title"))
            description = _clean(row.get("description"))
            combined_text = _clean(f"{title} {description}")
            entities = extract_entities(combined_text, language="en") if combined_text else []
            country, lat, lon = _resolve_article_geo(title, description, entities=entities)
            enriched_articles.append(
                {
                    "url": _clean(row.get("url")),
                    "title": title,
                    "description": description,
                    "source": _clean(row.get("source")),
                    "source_domain": _clean(row.get("source_domain")),
                    "published_at": row.get("published_at"),
                    "published_dt": _parse_iso(row.get("published_at")),
                    "language": _clean(row.get("language")),
                    "payload": row.get("payload"),
                    "_country": country,
                    "_lat": lat,
                    "_lon": lon,
                    "_entities": entities,
                }
            )

        clusters = _cluster_articles(enriched_articles)
        events_written = 0
        events_updated = 0
        perspectives_written = 0

        with _connect() as conn:
            for cluster in clusters:
                if not cluster:
                    continue

                best_article = _best_article_in_cluster(cluster)
                primary_entity = _most_common_entity(cluster, "GPE") or _most_common_entity(cluster, "ORG") or _article_primary_gpe(best_article) or _clean(best_article.get("title")) or "unknown"
                country = _most_common_country(cluster)
                lat, lon, geo_location = _cluster_geo(cluster, best_article)
                event_date_best = min(
                    (
                        _published_dt(article)
                        for article in cluster
                        if _published_dt(article) is not None
                    ),
                    default=None,
                )
                if event_date_best is None:
                    continue

                week_floor = _week_floor_from_datetime(event_date_best)
                anchor_country = country or geo_location or "unknown"
                anchor = _article_cluster_anchor(anchor_country, primary_entity, week_floor)

                existing_map = conn.execute(
                    "SELECT event_id FROM event_identity_map WHERE observation_key = %s",
                    (anchor,),
                ).fetchone()
                event_id = str(existing_map.get("event_id")) if existing_map and existing_map.get("event_id") else f"ce-art-{hashlib.sha256(anchor.encode('utf-8')).hexdigest()[:24]}"
                event_exists = conn.execute(
                    "SELECT 1 FROM canonical_events WHERE event_id = %s",
                    (event_id,),
                ).fetchone()

                article_urls = []
                seen_urls = set()
                for article in cluster:
                    url = _clean(article.get("url"))
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        article_urls.append(url)

                best_title = _clean(best_article.get("title")) or primary_entity or "Developing event"
                best_summary = _clean(best_article.get("description")) or ""
                event_type = _infer_event_type(cluster)
                importance_score = float(len(cluster) * 3.0 + article_quality_score(best_article, topics=TOPICS))
                source_count = _source_count(cluster)

                row = {
                    "event_id": event_id,
                    "topic": "geopolitics",
                    "label": best_title,
                    "event_type": event_type,
                    "status": "developing",
                    "geo_country": country,
                    "geo_region": None,
                    "geo_admin1": None,
                    "geo_location": geo_location or country or primary_entity,
                    "latitude": lat,
                    "longitude": lon,
                    "geo_precision": None,
                    "actor_primary": primary_entity,
                    "actor_secondary": None,
                    "first_reported_at": event_date_best.isoformat(),
                    "event_date_best": event_date_best,
                    "last_updated_at": datetime.now(timezone.utc).isoformat(),
                    "article_count": len(cluster),
                    "source_count": source_count,
                    "perspective_count": len(cluster),
                    "contradiction_count": 0,
                    "fatality_total": 0,
                    "importance_score": importance_score,
                    "importance_reasons": [
                        f"cluster_size={len(cluster)}",
                        f"best_quality={article_quality_score(best_article, topics=TOPICS)}",
                    ],
                    "resolved_title": best_title,
                    "resolved_summary": best_summary,
                    "linked_structured_event_ids": [],
                    "article_urls": article_urls,
                    "first_seen_at": time.time(),
                    "computed_at": time.time(),
                    "payload": {
                        "source_kind": "article_event",
                        "anchor": anchor,
                        "primary_entity": primary_entity,
                        "cluster_size": len(cluster),
                        "source_count": source_count,
                    },
                }

                _upsert_canonical_event(conn, row)

                # When updating an existing event, override the title/summary only if
                # the new cluster is higher quality than what was previously stored.
                if event_exists:
                    conn.execute(
                        """
                        UPDATE canonical_events
                        SET resolved_title   = %s,
                            resolved_summary = %s,
                            importance_score  = %s
                        WHERE event_id = %s
                          AND importance_score < %s
                        """,
                        (best_title, best_summary, importance_score, event_id, importance_score),
                    )
                    events_updated += 1
                else:
                    events_written += 1

                _upsert_identity_map(conn, anchor=anchor, event_id=event_id, topic="geopolitics")
                perspectives_written += _upsert_event_perspectives(conn, event_id=event_id, article_rows=cluster)

        return {
            "articles_processed": len(enriched_articles),
            "clusters_formed": len(clusters),
            "events_written": events_written,
            "events_updated": events_updated,
            "perspectives_written": perspectives_written,
        }
    finally:
        _PIPELINE_LOCK.release()