from __future__ import annotations

import hashlib
import json
import math
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from apply_corrections import apply_corrections
from db.common import _connect
from structured_story_rollups import build_structured_story_clusters


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _stable_anchor(event_type: str, country: str, admin1: str, window_start: str) -> str:
    material = "|".join(
        [
            _clean(event_type).lower(),
            _clean(country).lower(),
            _clean(admin1).lower(),
            _clean(window_start),
        ]
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
    return f"anchor-{digest}"


def _week_floor(date_str: str | None) -> str:
    """Return the ISO date of the Monday of the week containing date_str."""
    if not date_str:
        return "unknown"
    try:
        from datetime import date, timedelta

        d = date.fromisoformat(str(date_str)[:10])
        monday = d - timedelta(days=d.weekday())
        return monday.isoformat()
    except (ValueError, TypeError):
        return "unknown"


def _event_id_for_anchor(anchor: str) -> str:
    digest = hashlib.sha256(anchor.encode("utf-8")).hexdigest()[:24]
    return f"ce-{digest}"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_utc(value: str | None) -> str | None:
    dt = _parse_iso(value)
    if dt is None:
        return None
    return dt.isoformat().replace("+00:00", "Z")


def _extract_geo_precision(structured_event: dict) -> int | None:
    payload = structured_event.get("payload") or {}
    if not isinstance(payload, dict):
        return None
    raw = payload.get("geo_precision")
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _best_point(events: list[dict]) -> tuple[float | None, float | None, int | None]:
    candidates: list[tuple[int, int, float, float]] = []
    for event in events:
        lat = event.get("latitude")
        lon = event.get("longitude")
        if lat is None or lon is None:
            continue
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            continue
        precision = _extract_geo_precision(event)
        if precision is None:
            precision = 5
        dataset_rank = 0 if _clean(event.get("dataset")).lower() == "acled" else 1
        candidates.append((dataset_rank, precision, lat_f, lon_f))

    if not candidates:
        return None, None, None

    # Prefer ACLED points and tighter precision codes first.
    candidates.sort(key=lambda item: (item[0], item[1]))
    _, precision, lat, lon = candidates[0]
    return lat, lon, precision


def _primary_value(values: list[str]) -> str | None:
    cleaned = [_clean(v) for v in values if _clean(v)]
    if not cleaned:
        return None
    return Counter(cleaned).most_common(1)[0][0]


def _resolve_best_article(conn, source_urls: list[str]) -> tuple[str, str, list[str], int, int]:
    cleaned = []
    seen = set()
    for raw in source_urls:
        url = str(raw or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        cleaned.append(url)

    if not cleaned:
        return "", "", [], 0, 0

    placeholders = ", ".join(["%s"] * len(cleaned))
    rows = conn.execute(
        f"""
        SELECT url, canonical_url, title, description, source, source_domain, published_at
        FROM articles
        WHERE url IN ({placeholders}) OR canonical_url IN ({placeholders})
        ORDER BY published_at DESC NULLS LAST
        LIMIT 300
        """,
        [*cleaned, *cleaned],
    ).fetchall()

    matched_urls: list[str] = []
    title = ""
    summary = ""
    sources = set()
    seen_url = set()
    for row in rows:
        url = str(row.get("url") or "").strip()
        if url and url not in seen_url:
            seen_url.add(url)
            matched_urls.append(url)
        source_name = _clean(row.get("source") or row.get("source_domain"))
        if source_name:
            sources.add(source_name.lower())
        if not title:
            title = _clean(row.get("title"))
        if not summary:
            summary = _clean(row.get("description"))

    return title, summary, matched_urls, len(rows), len(sources)


def _cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = 0.0
    l2_left = 0.0
    l2_right = 0.0
    for lv, rv in zip(left, right):
        dot += float(lv) * float(rv)
        l2_left += float(lv) * float(lv)
        l2_right += float(rv) * float(rv)
    if l2_left <= 0.0 or l2_right <= 0.0:
        return 0.0
    return dot / math.sqrt(l2_left * l2_right)


def _semantic_match_existing_event(
    conn,
    *,
    geo_country: str | None,
    geo_admin1: str | None,
    event_date_best: datetime | None,
    title: str,
    summary: str,
) -> str | None:
    if not geo_country or not event_date_best or not (title or summary):
        return None

    start = (event_date_best - timedelta(days=3)).isoformat()
    end = (event_date_best + timedelta(days=3)).isoformat()
    rows = conn.execute(
        """
        SELECT event_id, resolved_title, resolved_summary, geo_country, geo_admin1
        FROM canonical_events
        WHERE status != 'superseded'
          AND LOWER(COALESCE(geo_country, '')) = LOWER(%s)
          AND event_date_best >= %s
          AND event_date_best <= %s
        ORDER BY importance_score DESC, computed_at DESC
        LIMIT 100
        """,
        (geo_country, start, end),
    ).fetchall()
    if not rows:
        return None

    filtered = []
    admin1_norm = _clean(geo_admin1).lower()
    for row in rows:
        candidate_admin1 = _clean(row.get("geo_admin1")).lower()
        if admin1_norm and candidate_admin1 and admin1_norm != candidate_admin1:
            continue
        filtered.append(row)
    if not filtered:
        filtered = rows

    query_text = _clean(f"{title} {summary}")
    if not query_text:
        return None

    try:
        from clustering import get_semantic_model

        model = get_semantic_model()
        vectors = model.encode(
            [query_text]
            + [
                _clean(f"{row.get('resolved_title') or ''} {row.get('resolved_summary') or ''}")
                for row in filtered
            ],
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        query_vector = vectors[0]
        best_event_id = None
        best_score = 0.0
        for idx, row in enumerate(filtered, start=1):
            score = _cosine_similarity(list(query_vector), list(vectors[idx]))
            if score > best_score:
                best_score = score
                best_event_id = row.get("event_id")
        if best_event_id and best_score >= 0.62:
            return str(best_event_id)
    except MemoryError:
        # Fall back to token overlap when the embedding model cannot run.
        pass
    except Exception:
        pass

    query_tokens = {t for t in query_text.lower().split() if len(t) > 3}
    best_event_id = None
    best_score = 0.0
    for row in filtered:
        text = _clean(f"{row.get('resolved_title') or ''} {row.get('resolved_summary') or ''}")
        cand_tokens = {t for t in text.lower().split() if len(t) > 3}
        if not query_tokens or not cand_tokens:
            continue
        overlap = len(query_tokens & cand_tokens)
        union = len(query_tokens | cand_tokens)
        score = overlap / union if union else 0.0
        if score > best_score:
            best_score = score
            best_event_id = row.get("event_id")
    if best_event_id and best_score >= 0.25:
        return str(best_event_id)
    return None


def _upsert_canonical_event(conn, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO canonical_events (
            event_id, topic, label, event_type, status,
            geo_country, geo_region, geo_admin1, geo_location,
            latitude, longitude, geo_precision,
            actor_primary, actor_secondary,
            first_reported_at, event_date_best, last_updated_at,
            article_count, source_count, perspective_count, contradiction_count, fatality_total,
            importance_score, importance_reasons,
            resolved_title, resolved_summary,
            linked_structured_event_ids, article_urls,
            first_seen_at, computed_at, payload
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s::jsonb,
            %s, %s,
            %s::jsonb, %s::jsonb,
            %s, %s, %s::jsonb
        )
        ON CONFLICT (event_id) DO UPDATE SET
            label = EXCLUDED.label,
            event_type = COALESCE(EXCLUDED.event_type, canonical_events.event_type),
            status = EXCLUDED.status,
            geo_country = COALESCE(EXCLUDED.geo_country, canonical_events.geo_country),
            geo_region = COALESCE(EXCLUDED.geo_region, canonical_events.geo_region),
            geo_admin1 = COALESCE(EXCLUDED.geo_admin1, canonical_events.geo_admin1),
            geo_location = COALESCE(EXCLUDED.geo_location, canonical_events.geo_location),
            latitude = COALESCE(EXCLUDED.latitude, canonical_events.latitude),
            longitude = COALESCE(EXCLUDED.longitude, canonical_events.longitude),
            geo_precision = COALESCE(EXCLUDED.geo_precision, canonical_events.geo_precision),
            actor_primary = COALESCE(EXCLUDED.actor_primary, canonical_events.actor_primary),
            actor_secondary = COALESCE(EXCLUDED.actor_secondary, canonical_events.actor_secondary),
            first_reported_at = COALESCE(canonical_events.first_reported_at, EXCLUDED.first_reported_at),
            event_date_best = COALESCE(EXCLUDED.event_date_best, canonical_events.event_date_best),
            last_updated_at = EXCLUDED.last_updated_at,
            article_count = EXCLUDED.article_count,
            source_count = EXCLUDED.source_count,
            perspective_count = EXCLUDED.perspective_count,
            contradiction_count = EXCLUDED.contradiction_count,
            fatality_total = EXCLUDED.fatality_total,
            importance_score = EXCLUDED.importance_score,
            importance_reasons = EXCLUDED.importance_reasons,
            resolved_title = COALESCE(EXCLUDED.resolved_title, canonical_events.resolved_title),
            resolved_summary = COALESCE(EXCLUDED.resolved_summary, canonical_events.resolved_summary),
            linked_structured_event_ids = EXCLUDED.linked_structured_event_ids,
            article_urls = EXCLUDED.article_urls,
            computed_at = EXCLUDED.computed_at,
            payload = EXCLUDED.payload
        """,
        (
            row["event_id"],
            row.get("topic") or "geopolitics",
            row.get("label") or row.get("resolved_title") or "Developing event",
            row.get("event_type"),
            row.get("status") or "developing",
            row.get("geo_country"),
            row.get("geo_region"),
            row.get("geo_admin1"),
            row.get("geo_location"),
            row.get("latitude"),
            row.get("longitude"),
            row.get("geo_precision"),
            row.get("actor_primary"),
            row.get("actor_secondary"),
            row.get("first_reported_at"),
            row.get("event_date_best"),
            row.get("last_updated_at"),
            int(row.get("article_count") or 0),
            int(row.get("source_count") or 0),
            int(row.get("perspective_count") or 0),
            int(row.get("contradiction_count") or 0),
            int(row.get("fatality_total") or 0),
            float(row.get("importance_score") or 0.0),
            json.dumps(row.get("importance_reasons") or [], sort_keys=True),
            row.get("resolved_title") or None,
            row.get("resolved_summary") or None,
            json.dumps(row.get("linked_structured_event_ids") or [], sort_keys=True),
            json.dumps(row.get("article_urls") or [], sort_keys=True),
            float(row.get("first_seen_at") or time.time()),
            float(row.get("computed_at") or time.time()),
            json.dumps(row.get("payload") or {}, sort_keys=True, default=str),
        ),
    )


def _upsert_identity_map(conn, *, anchor: str, event_id: str, topic: str) -> None:
    now_ts = time.time()
    conn.execute(
        """
        INSERT INTO event_identity_map (
            observation_key, event_id, topic,
            first_mapped_at, last_seen_at, identity_confidence, identity_reasons
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (observation_key) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            topic = EXCLUDED.topic,
            last_seen_at = EXCLUDED.last_seen_at,
            identity_confidence = EXCLUDED.identity_confidence,
            identity_reasons = EXCLUDED.identity_reasons
        """,
        (
            anchor,
            event_id,
            topic,
            now_ts,
            now_ts,
            0.95,
            json.dumps({"identity_anchor": anchor}, sort_keys=True),
        ),
    )


def _upsert_event_perspectives(
    conn,
    *,
    event_id: str,
    article_rows: list[dict],
) -> int:
    written = 0
    analyzed_at = time.time()
    for row in article_rows:
        article_url = str(row.get("url") or "").strip()
        if not article_url:
            continue
        pid = hashlib.sha256(f"{event_id}|{article_url}".encode("utf-8")).hexdigest()[:32]
        source_name = _clean(row.get("source") or row.get("source_domain")) or "unknown"
        claim_text = _clean(row.get("description")) or None
        dominant_frame = "coverage"
        conn.execute(
            """
            INSERT INTO event_perspectives (
                perspective_id, event_id, article_url, source_name, source_domain,
                dominant_frame, claim_text, published_at, analyzed_at, payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (perspective_id) DO UPDATE SET
                event_id = EXCLUDED.event_id,
                source_name = EXCLUDED.source_name,
                source_domain = EXCLUDED.source_domain,
                dominant_frame = EXCLUDED.dominant_frame,
                claim_text = COALESCE(EXCLUDED.claim_text, event_perspectives.claim_text),
                published_at = COALESCE(EXCLUDED.published_at, event_perspectives.published_at),
                analyzed_at = EXCLUDED.analyzed_at,
                payload = EXCLUDED.payload
            """,
            (
                pid,
                event_id,
                article_url,
                source_name,
                row.get("source_domain"),
                dominant_frame,
                claim_text,
                row.get("published_at"),
                analyzed_at,
                json.dumps({"resolved_from": "canonical_events_pipeline"}, sort_keys=True),
            ),
        )
        written += 1
    return written


def _aspect_for_event_type(event_type: str) -> str:
    et = (event_type or "").lower()
    if any(
        token in et
        for token in (
            "battle",
            "violence",
            "explosion",
            "remote",
            "riot",
            "attack",
            "armed",
        )
    ):
        return "conflict"
    if any(token in et for token in ("protest", "demonstration")):
        return "political"
    if any(token in et for token in ("sanction", "tariff", "trade", "economic")):
        return "economic"
    return "political"


def populate_canonical_events(*, days: int = 7, limit: int = 500) -> dict:
    clusters = build_structured_story_clusters(days=max(1, days), limit=max(1, limit), dataset=None)
    if not clusters:
        corrections = apply_corrections()
        return {
            "clusters_processed": 0,
            "events_written": 0,
            "perspectives_written": 0,
            "corrections": corrections,
        }

    events_written = 0
    perspectives_written = 0
    with _connect() as conn:
        for cluster in clusters:
            events = cluster.get("events") or []
            if not events:
                continue

            event_type = _primary_value([ev.get("event_type") for ev in events]) or _clean(cluster.get("primary_event_type"))
            country = _primary_value([ev.get("country") for ev in events])
            admin1 = _primary_value([ev.get("admin1") for ev in events])
            region = _primary_value([ev.get("region") for ev in events])
            location = _primary_value([ev.get("location") for ev in events])
            actor_primary = _primary_value([ev.get("actor_primary") for ev in events])
            actor_secondary = _primary_value([ev.get("actor_secondary") for ev in events])

            date_candidates = [ev.get("event_date") for ev in events if ev.get("event_date")]
            window_start = min(date_candidates) if date_candidates else None
            window_end = max(date_candidates) if date_candidates else None
            event_date_best_dt = _parse_iso(window_start or window_end)

            anchor = _stable_anchor(event_type or "unknown", country or "unknown", admin1 or "", _week_floor(window_start))

            existing_map = conn.execute(
                "SELECT event_id FROM event_identity_map WHERE observation_key = %s",
                (anchor,),
            ).fetchone()
            event_id = str(existing_map.get("event_id")) if existing_map and existing_map.get("event_id") else ""

            source_urls: list[str] = []
            structured_ids: list[str] = []
            seen_source = set()
            seen_structured = set()
            for ev in events:
                ev_id = str(ev.get("event_id") or "").strip()
                if ev_id and ev_id not in seen_structured:
                    seen_structured.add(ev_id)
                    structured_ids.append(ev_id)
                for raw_url in ev.get("source_urls") or []:
                    url = str(raw_url or "").strip()
                    if not url or url in seen_source:
                        continue
                    seen_source.add(url)
                    source_urls.append(url)

            title, summary, article_urls, article_count, source_count = _resolve_best_article(conn, source_urls)
            if not title:
                summaries = [_clean(ev.get("summary")) for ev in events if ev.get("summary")]
                if summaries:
                    summary = max(summaries, key=len)
                place = location or admin1 or country or ""
                if event_type and place:
                    title = f"{event_type} in {place}"
                elif event_type:
                    title = event_type
                elif place:
                    title = f"Developing situation in {place}"
            article_rows: list[dict] = []
            if article_urls:
                placeholders = ", ".join(["%s"] * len(article_urls))
                article_rows = conn.execute(
                    f"""
                    SELECT url, source, source_domain, published_at, title, description
                    FROM articles
                    WHERE url IN ({placeholders})
                    """,
                    article_urls,
                ).fetchall()

            lat, lon, geo_precision = _best_point(events)

            if not event_id:
                matched_event_id = _semantic_match_existing_event(
                    conn,
                    geo_country=country,
                    geo_admin1=admin1,
                    event_date_best=event_date_best_dt,
                    title=title,
                    summary=summary,
                )
                event_id = matched_event_id or _event_id_for_anchor(anchor)

            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            importance = float(cluster.get("analysis_priority") or 0.0)
            row = {
                "event_id": event_id,
                "topic": "geopolitics",
                "label": title or _clean(cluster.get("label")) or "Developing event",
                "event_type": event_type,
                "status": "developing",
                "geo_country": country,
                "geo_region": region,
                "geo_admin1": admin1,
                "geo_location": location,
                "latitude": lat,
                "longitude": lon,
                "geo_precision": geo_precision,
                "actor_primary": actor_primary,
                "actor_secondary": actor_secondary,
                "first_reported_at": _iso_utc(window_start),
                "event_date_best": event_date_best_dt.isoformat() if event_date_best_dt else None,
                "last_updated_at": now_iso,
                "article_count": article_count,
                "source_count": source_count,
                "perspective_count": article_count,
                "contradiction_count": 0,
                "fatality_total": int(cluster.get("fatality_total") or 0),
                "importance_score": importance,
                "importance_reasons": [
                    f"structured_event_count={int(cluster.get('structured_event_count') or 0)}",
                    f"fatality_total={int(cluster.get('fatality_total') or 0)}",
                ],
                "resolved_title": title,
                "resolved_summary": summary,
                "linked_structured_event_ids": structured_ids,
                "article_urls": article_urls,
                "first_seen_at": time.time(),
                "computed_at": time.time(),
                "payload": {
                    "identity_anchor": anchor,
                    "source_event_count": len(events),
                    "date_window_start": window_start,
                    "date_window_end": window_end,
                },
            }
            _upsert_canonical_event(conn, row)
            _upsert_identity_map(conn, anchor=anchor, event_id=event_id, topic="geopolitics")
            events_written += 1
            perspectives_written += _upsert_event_perspectives(conn, event_id=event_id, article_rows=article_rows)

    corrections = apply_corrections()
    return {
        "clusters_processed": len(clusters),
        "events_written": events_written,
        "perspectives_written": perspectives_written,
        "corrections": corrections,
    }


def get_canonical_events_map_payload(*, days: int = 7, limit: int = 500) -> dict:
    safe_days = max(1, min(int(days), 90))
    safe_limit = max(1, min(int(limit), 3000))
    with _connect() as conn:
        # Canonical events (article-derived, enriched with geo)
        canon_rows = conn.execute(
            """
            SELECT
                event_id,
                resolved_title,
                resolved_summary,
                event_type,
                geo_country,
                geo_admin1,
                geo_location,
                latitude,
                longitude,
                event_date_best,
                article_count,
                source_count,
                actor_primary,
                actor_secondary,
                importance_score,
                geo_precision,
                fatality_total
            FROM canonical_events
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND COALESCE(geo_precision, 5) <= 5
              AND event_date_best IS NOT NULL
              AND event_date_best >= NOW() - (%s * INTERVAL '1 day')
              AND COALESCE(status, 'developing') != 'superseded'
            ORDER BY COALESCE(importance_score, 0) DESC, COALESCE(event_date_best, NOW()) DESC
            LIMIT %s
            """,
            (safe_days, min(safe_limit, 500)),
        ).fetchall()

        # GDELT structured events — cluster by rounded coordinates to avoid overplotting
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=safe_days)).strftime("%Y-%m-%d")
        struct_rows = conn.execute(
            """
            SELECT
                ROUND(latitude::numeric, 1) AS lat_bucket,
                ROUND(longitude::numeric, 1) AS lng_bucket,
                country,
                event_type,
                MAX(location) AS location,
                COUNT(*) AS event_count,
                MAX(summary) AS summary
            FROM structured_events
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND event_date >= %s
            GROUP BY lat_bucket, lng_bucket, country, event_type
            ORDER BY event_count DESC
            LIMIT %s
            """,
            (cutoff_date, safe_limit),
        ).fetchall()

    hotspots = []

    for row in canon_rows:
        event_id = str(row.get("event_id") or "").strip()
        if not event_id:
            continue
        title = _clean(row.get("resolved_title"))
        # Skip events that have no real title — they add noise without signal
        if not title or title.lower().startswith("developing event") or title.lower().startswith("developing situation"):
            # Allow through only if there are multiple sources backing it
            if int(row.get("source_count") or 0) < 2:
                continue
        if not title:
            title = "Developing event"
        summary = _clean(row.get("resolved_summary"))
        event_type = _clean(row.get("event_type"))
        country = _clean(row.get("geo_country"))
        admin1 = _clean(row.get("geo_admin1"))
        location = _clean(row.get("geo_location"))
        hotspots.append(
            {
                "hotspot_id": event_id,
                "source_kind": "canonical_event",
                "label": title,
                "headline": title,
                "summary": summary,
                "event_types": [event_type] if event_type else [],
                "event_count": 1,
                "article_count": int(row.get("article_count") or 0),
                "source_count": int(row.get("source_count") or 0),
                "attention_score": float(row.get("importance_score") or 0.0),
                "country": country,
                "admin1": admin1,
                "location": location,
                "latitude": float(row.get("latitude")),
                "longitude": float(row.get("longitude")),
                "fatality_total": int(row.get("fatality_total") or 0),
                "aspect": _aspect_for_event_type(event_type),
                "sample_events": [
                    {
                        "event_id": event_id,
                        "title": title,
                        "summary": summary,
                        "event_type": event_type,
                        "actor_primary": _clean(row.get("actor_primary")),
                        "actor_secondary": _clean(row.get("actor_secondary")),
                        "event_date": _iso_utc(row.get("event_date_best")),
                        "source_urls": [],
                    }
                ],
            }
        )

    # Generic GDELT event types that produce noise without real signal
    _NOISY_STRUCT_TYPES = {
        "strategic developments",
        "non-violent action",
        "headquarters or base established",
    }
    # Substrings in location names that indicate GDELT geocoded to an org/bureau
    # rather than an actual conflict site
    _NOISY_LOCATION_SUBSTRINGS = (
        "world bank",
        "united nations",
        "international monetary fund",
        "european commission",
        "district of columbia",
        "washington, d.c",
    )
    # Locations that are major news bureau hubs — GDELT frequently misattributes
    # conflict events here when the story was *reported from* that location.
    # Only filter these for combat/violence event types where misattribution is obvious.
    _NEWS_HUB_LOCATIONS = {
        "new york",
        "new york, united states",
        "southwark, united kingdom",
        "london, united kingdom",
        "washington",
        "california, united states",
        "united kingdom",
        "united states",
        "france",
        "germany",
        "australia",
        "canada",
    }
    _COMBAT_STRUCT_TYPES = {"battles", "explosions/remote violence", "violence against civilians"}

    seen_struct_ids: set[str] = set()
    for row in struct_rows:
        lat = float(row.get("lat_bucket") or 0)
        lng = float(row.get("lng_bucket") or 0)
        event_type = _clean(row.get("event_type"))
        country = _clean(row.get("country"))
        event_count = int(row.get("event_count") or 1)
        location = _clean(row.get("location"))

        # Skip noisy generic event types
        if event_type.lower() in _NOISY_STRUCT_TYPES:
            continue
        # Skip buckets with only one event — too sparse to be meaningful
        if event_count < 2:
            continue
        # Skip international-org locations (substring check)
        location_lower = location.lower()
        if any(sub in location_lower for sub in _NOISY_LOCATION_SUBSTRINGS):
            continue
        # Skip major news bureau cities for combat events — GDELT geocodes
        # stories *reported from* these cities as events occurring there
        if event_type.lower() in _COMBAT_STRUCT_TYPES:
            if location_lower in _NEWS_HUB_LOCATIONS or country.lower() in {"united states", "united kingdom", "france", "germany", "australia", "canada"}:
                continue

        # Deduplicate against canonical event locations (within ~50km)
        struct_id = f"se-{round(lat,1)}-{round(lng,1)}-{event_type}"
        if struct_id in seen_struct_ids:
            continue
        seen_struct_ids.add(struct_id)
        summary = _clean(row.get("summary"))
        label = f"{event_type} — {location or country}" if event_type else (location or country or "Signal cluster")
        hotspots.append(
            {
                "hotspot_id": struct_id,
                "source_kind": "structured_event",
                "label": label,
                "headline": label,
                "summary": summary,
                "event_types": [event_type] if event_type else [],
                "event_count": event_count,
                "article_count": 0,
                "source_count": 0,
                "attention_score": float(event_count),
                "country": country,
                "admin1": None,
                "location": location,
                "latitude": lat,
                "longitude": lng,
                "fatality_total": 0,
                "aspect": _aspect_for_event_type(event_type),
                "sample_events": [],
            }
        )

    total_attention = sum(float(item.get("attention_score") or 0.0) for item in hotspots) or 1.0
    for item in hotspots:
        item["attention_share"] = round(float(item.get("attention_score") or 0.0) / total_attention, 4)

    return {
        "window": f"{safe_days}d",
        "days": safe_days,
        "hours": safe_days * 24,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "hotspot_count": len(hotspots),
        "total_events": len(hotspots),
        "hotspots": hotspots,
        "stories": [],
    }
