import json
import math
import time
from datetime import datetime, timezone, timedelta

from db.common import (
    _connect,
    _row_to_structured_event,
    _row_to_canonical_event,
    _row_to_perspective,
    _parse_published_at,
)


def _haversine_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    lat1, lon1 = math.radians(lat_a), math.radians(lon_a)
    lat2, lon2 = math.radians(lat_b), math.radians(lon_b)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    arc = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * radius_km * math.asin(min(1.0, math.sqrt(max(0.0, arc))))


def deduplicate_cross_dataset_events(
    days: int = 3,
    radius_km: float = 50.0,
    max_hours_apart: float = 24.0,
) -> dict:
    """Mark GDELT events as superseded when a matching ACLED event exists nearby.

    Matching criteria:
    - Both events within radius_km of each other (default 50 km)
    - Event dates within max_hours_apart (default 24 hours)
    - Same broad event category (Battles/Violence maps to ACLED equivalents)

    ACLED is preferred as the canonical source. When a match is found,
    the GDELT row's superseded_by is set to the ACLED event_id.
    Only processes events from the last `days` days.

    Returns a summary dict with counts of matches found and marked.
    """
    from datetime import date

    cutoff = time.time() - (max(1, int(days)) * 86400)

    with _connect() as conn:
        acled_rows = conn.execute(
            """
            SELECT event_id, latitude, longitude, event_date, event_type, country
            FROM structured_events
            WHERE dataset = 'acled'
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND last_ingested_at >= %s
            """,
            (cutoff,),
        ).fetchall()

        gdelt_rows = conn.execute(
            """
            SELECT event_id, latitude, longitude, event_date, event_type, country
            FROM structured_events
            WHERE dataset = 'gdelt_gkg'
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND superseded_by IS NULL
              AND last_ingested_at >= %s
            """,
            (cutoff,),
        ).fetchall()

    matched = 0
    updates: list[tuple[str, str]] = []
    for gdelt in gdelt_rows:
        g_lat = float(gdelt["latitude"] or 0)
        g_lon = float(gdelt["longitude"] or 0)
        g_date = str(gdelt["event_date"] or "")
        g_country = (gdelt["country"] or "").strip().lower()

        best_acled_id = None
        best_distance = float("inf")

        for acled in acled_rows:
            a_lat = float(acled["latitude"] or 0)
            a_lon = float(acled["longitude"] or 0)
            a_date = str(acled["event_date"] or "")
            a_country = (acled["country"] or "").strip().lower()

            if g_country and a_country and g_country != a_country:
                continue

            try:
                gd = date.fromisoformat(g_date[:10]) if g_date else None
                ad = date.fromisoformat(a_date[:10]) if a_date else None
                if gd and ad:
                    hours_apart = abs((gd - ad).total_seconds()) / 3600
                    if hours_apart > max_hours_apart:
                        continue
            except ValueError:
                continue

            dist = _haversine_km(g_lat, g_lon, a_lat, a_lon)
            if dist <= radius_km and dist < best_distance:
                best_distance = dist
                best_acled_id = acled["event_id"]

        if best_acled_id:
            updates.append((best_acled_id, gdelt["event_id"]))
            matched += 1

    if updates:
        with _connect() as conn:
            for best_acled_id, gdelt_event_id in updates:
                conn.execute(
                    """
                    UPDATE structured_events
                    SET superseded_by = %s
                    WHERE event_id = %s AND superseded_by IS NULL
                    """,
                    (best_acled_id, gdelt_event_id),
                )

    return {
        "acled_events_checked": len(acled_rows),
        "gdelt_events_checked": len(gdelt_rows),
        "duplicates_marked": matched,
        "radius_km": radius_km,
        "days": days,
    }


def upsert_structured_events(events: list[dict]) -> int:
    if not events:
        return 0

    inserted = 0
    with _connect() as conn:
        for event in events:
            payload = json.dumps(event.get("payload") or {}, sort_keys=True)
            source_urls = json.dumps(event.get("source_urls") or [], sort_keys=True)
            existing = conn.execute(
                "SELECT event_id FROM structured_events WHERE event_id = %s",
                (event["event_id"],),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO structured_events (
                    event_id, dataset, dataset_event_id, event_date, country, region, admin1, admin2,
                    location, latitude, longitude, event_type, sub_event_type, actor_primary,
                    actor_secondary, fatalities, source_count, source_urls, summary, payload,
                    first_ingested_at, last_ingested_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    dataset = EXCLUDED.dataset,
                    dataset_event_id = EXCLUDED.dataset_event_id,
                    event_date = EXCLUDED.event_date,
                    country = EXCLUDED.country,
                    region = EXCLUDED.region,
                    admin1 = EXCLUDED.admin1,
                    admin2 = EXCLUDED.admin2,
                    location = EXCLUDED.location,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    event_type = EXCLUDED.event_type,
                    sub_event_type = EXCLUDED.sub_event_type,
                    actor_primary = EXCLUDED.actor_primary,
                    actor_secondary = EXCLUDED.actor_secondary,
                    fatalities = EXCLUDED.fatalities,
                    source_count = EXCLUDED.source_count,
                    source_urls = EXCLUDED.source_urls,
                    summary = EXCLUDED.summary,
                    payload = EXCLUDED.payload,
                    last_ingested_at = EXCLUDED.last_ingested_at
                """,
                (
                    event["event_id"],
                    event["dataset"],
                    event.get("dataset_event_id"),
                    event["event_date"],
                    event.get("country"),
                    event.get("region"),
                    event.get("admin1"),
                    event.get("admin2"),
                    event.get("location"),
                    event.get("latitude"),
                    event.get("longitude"),
                    event.get("event_type"),
                    event.get("sub_event_type"),
                    event.get("actor_primary"),
                    event.get("actor_secondary"),
                    event.get("fatalities"),
                    event.get("source_count"),
                    source_urls,
                    event.get("summary"),
                    payload,
                    event["first_ingested_at"],
                    event["last_ingested_at"],
                ),
            )
            if not existing:
                inserted += 1
    return inserted


def get_recent_structured_events(
    *,
    days: int = 7,
    limit: int = 3000,
    dataset: str | None = None,
    country: str | None = None,
    event_type: str | None = None,
) -> list[dict]:
    base_clauses = []
    base_params: list[object] = []

    if dataset:
        base_clauses.append("dataset = %s")
        base_params.append(dataset)
    if country:
        base_clauses.append("country = %s")
        base_params.append(country)
    if event_type:
        base_clauses.append("event_type = %s")
        base_params.append(event_type)

    def fetch_rows(cutoff_value: str) -> list:
        clauses = ["event_date >= %s", "superseded_by IS NULL", *base_clauses]
        params = [cutoff_value, *base_params, limit]
        where = " AND ".join(clauses)
        with _connect() as conn:
            return conn.execute(
                f"""
                SELECT *
                FROM structured_events
                WHERE {where}
                ORDER BY event_date DESC, COALESCE(fatalities, 0) DESC, last_ingested_at DESC
                LIMIT %s
                """,
                params,
            ).fetchall()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    rows = fetch_rows(cutoff)
    if not rows:
        latest_where = f"WHERE {' AND '.join(base_clauses)}" if base_clauses else ""
        with _connect() as conn:
            latest_row = conn.execute(
                f"""
                SELECT MAX(event_date) AS latest_event_date
                FROM structured_events
                {latest_where}
                """,
                base_params,
            ).fetchone()
        latest_event_date = (
            latest_row["latest_event_date"] if latest_row else None
        ) or None
        if latest_event_date:
            parsed_latest = _parse_published_at(latest_event_date)
            if parsed_latest is not None:
                fallback_cutoff = (
                    (parsed_latest - timedelta(days=max(0, days - 1)))
                    .date()
                    .isoformat()
                )
                rows = fetch_rows(fallback_cutoff)
    return [_row_to_structured_event(row) for row in rows]


def get_recent_canonical_events(days: int = 7, limit: int = 300) -> list[dict]:
    """Fetch recent canonical events for map display.

    Returns an empty list when canonical events are unavailable in this environment.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT event_id, label, resolved_title, resolved_summary,
                       event_type, geo_country, geo_admin1, geo_location,
                       latitude, longitude, event_date_best,
                       source_count, article_count, importance_score,
                       article_urls, fatality_total
                FROM canonical_events
                WHERE event_date_best >= %s
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND resolved_title IS NOT NULL
                  AND resolved_title != ''
                ORDER BY importance_score DESC, event_date_best DESC
                LIMIT %s
                """,
                (cutoff, limit),
            ).fetchall()
    except Exception:
        return []
    return [dict(row) for row in rows]


def get_structured_event_coordinates_by_ids(event_ids: list[str]) -> dict[str, dict]:
    ids = [str(x).strip() for x in event_ids if x and str(x).strip()]
    if not ids:
        return {}
    cap = min(len(ids), 400)
    ids = ids[:cap]
    placeholders = ", ".join(["%s"] * len(ids))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
                event_id,
                latitude,
                longitude,
                country,
                admin1,
                admin2,
                location,
                event_date,
                fatalities,
                source_count,
                event_type
            FROM structured_events
            WHERE event_id IN ({placeholders})
            """,
            ids,
        ).fetchall()
    out: dict[str, dict] = {}
    for row in rows:
        eid = row.get("event_id")
        if not eid:
            continue
        out[str(eid)] = {
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "country": row.get("country"),
            "admin1": row.get("admin1"),
            "admin2": row.get("admin2"),
            "location": row.get("location"),
            "event_date": row.get("event_date"),
            "fatalities": row.get("fatalities"),
            "source_count": row.get("source_count"),
            "event_type": row.get("event_type"),
        }
    return out


def list_structured_event_ids_in_date_range(
    start: str, end: str, limit: int = 300
) -> list[str]:
    if not start or not end:
        return []
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT event_id
            FROM structured_events
            WHERE event_date >= %s AND event_date <= %s
            ORDER BY event_date DESC
            LIMIT %s
            """,
            (start, end, max(1, min(int(limit), 10000))),
        ).fetchall()
    return [row.get("event_id") for row in rows if row.get("event_id")]


def upsert_event_observations(records: list[dict]) -> int:
    if not records:
        return 0
    saved = 0
    with _connect() as conn:
        for record in records:
            article_urls = json.dumps(record.get("article_urls") or [], sort_keys=True)
            source_names = json.dumps(record.get("source_names") or [], sort_keys=True)
            payload = json.dumps(record.get("payload") or {}, sort_keys=True)
            conn.execute(
                """
                INSERT INTO event_observation_archive (
                    event_key, topic, event_label, first_othello_seen_at, latest_othello_seen_at,
                    first_article_published_at, first_major_source_published_at, earliest_source,
                    earliest_major_source, article_urls, source_names, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
                ON CONFLICT (event_key) DO UPDATE SET
                    topic = EXCLUDED.topic,
                    event_label = EXCLUDED.event_label,
                    first_othello_seen_at = LEAST(event_observation_archive.first_othello_seen_at, EXCLUDED.first_othello_seen_at),
                    latest_othello_seen_at = GREATEST(event_observation_archive.latest_othello_seen_at, EXCLUDED.latest_othello_seen_at),
                    first_article_published_at = COALESCE(event_observation_archive.first_article_published_at, EXCLUDED.first_article_published_at),
                    first_major_source_published_at = COALESCE(event_observation_archive.first_major_source_published_at, EXCLUDED.first_major_source_published_at),
                    earliest_source = COALESCE(event_observation_archive.earliest_source, EXCLUDED.earliest_source),
                    earliest_major_source = COALESCE(event_observation_archive.earliest_major_source, EXCLUDED.earliest_major_source),
                    article_urls = EXCLUDED.article_urls,
                    source_names = EXCLUDED.source_names,
                    payload = EXCLUDED.payload
                """,
                (
                    record["event_key"],
                    record.get("topic"),
                    record["event_label"],
                    record["first_othello_seen_at"],
                    record["latest_othello_seen_at"],
                    record.get("first_article_published_at"),
                    record.get("first_major_source_published_at"),
                    record.get("earliest_source"),
                    record.get("earliest_major_source"),
                    article_urls,
                    source_names,
                    payload,
                ),
            )
            saved += 1
    return saved


def load_before_news_archive(
    limit: int = 100, minimum_gap_hours: int = 4
) -> list[dict]:
    threshold = minimum_gap_hours * 3600
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM event_observation_archive
            WHERE first_major_source_published_at IS NOT NULL
            ORDER BY first_othello_seen_at DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()

    results = []
    for row in rows:
        payload = row.get("payload")
        article_urls = row.get("article_urls")
        source_names = row.get("source_names")
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        if isinstance(article_urls, str):
            article_urls = json.loads(article_urls) if article_urls else []
        if isinstance(source_names, str):
            source_names = json.loads(source_names) if source_names else []
        major_dt = _parse_published_at(row.get("first_major_source_published_at"))
        if major_dt is None:
            continue
        first_seen_dt = datetime.fromtimestamp(
            float(row.get("first_othello_seen_at") or 0), tz=timezone.utc
        )
        gap_seconds = (major_dt - first_seen_dt).total_seconds()
        if gap_seconds < threshold:
            continue
        results.append(
            {
                "event_key": row.get("event_key"),
                "topic": row.get("topic"),
                "event_label": row.get("event_label"),
                "first_othello_seen_at": row.get("first_othello_seen_at"),
                "first_major_source_published_at": row.get(
                    "first_major_source_published_at"
                ),
                "earliest_source": row.get("earliest_source"),
                "earliest_major_source": row.get("earliest_major_source"),
                "lead_time_hours": round(gap_seconds / 3600, 2),
                "article_urls": article_urls or [],
                "source_names": source_names or [],
                "payload": payload or {},
            }
        )
    return results


def load_event_observation_records(limit: int = 100) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM event_observation_archive
            ORDER BY latest_othello_seen_at DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()

    records = []
    for row in rows:
        payload = row.get("payload")
        article_urls = row.get("article_urls")
        source_names = row.get("source_names")
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        if isinstance(article_urls, str):
            article_urls = json.loads(article_urls) if article_urls else []
        if isinstance(source_names, str):
            source_names = json.loads(source_names) if source_names else []
        records.append(
            {
                "event_key": row.get("event_key"),
                "topic": row.get("topic"),
                "event_label": row.get("event_label"),
                "first_othello_seen_at": row.get("first_othello_seen_at"),
                "latest_othello_seen_at": row.get("latest_othello_seen_at"),
                "first_article_published_at": row.get("first_article_published_at"),
                "first_major_source_published_at": row.get(
                    "first_major_source_published_at"
                ),
                "earliest_source": row.get("earliest_source"),
                "earliest_major_source": row.get("earliest_major_source"),
                "article_urls": article_urls or [],
                "source_names": source_names or [],
                "payload": payload or {},
            }
        )
    return records


# ── canonical_events and perspectives


def upsert_canonical_events(rows: list[dict]) -> int:
    if not rows:
        return 0
    now = time.time()
    written = 0
    with _connect() as conn:
        for row in rows:
            event_id = (row.get("event_id") or "").strip()
            if not event_id:
                continue
            article_urls = json.dumps(
                sorted(row.get("article_urls") or []), sort_keys=True
            )
            linked = json.dumps(
                row.get("linked_structured_event_ids") or [], sort_keys=True
            )
            importance_reasons = json.dumps(
                row.get("importance_reasons") or [], sort_keys=True
            )
            payload = json.dumps(row.get("payload") or {}, sort_keys=True, default=str)
            conn.execute(
                """
                INSERT INTO canonical_events (
                    event_id, topic, label, event_type, status,
                    geo_country, geo_region, latitude, longitude,
                    first_reported_at, last_updated_at,
                    article_count, source_count, perspective_count, contradiction_count,
                    importance_score, importance_reasons,
                    linked_structured_event_ids, article_urls,
                    first_seen_at, computed_at, payload
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s::jsonb,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s::jsonb
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    label = EXCLUDED.label,
                    event_type = COALESCE(EXCLUDED.event_type, canonical_events.event_type),
                    status = EXCLUDED.status,
                    geo_country = COALESCE(EXCLUDED.geo_country, canonical_events.geo_country),
                    geo_region = COALESCE(EXCLUDED.geo_region, canonical_events.geo_region),
                    latitude = COALESCE(EXCLUDED.latitude, canonical_events.latitude),
                    longitude = COALESCE(EXCLUDED.longitude, canonical_events.longitude),
                    first_reported_at = EXCLUDED.first_reported_at,
                    last_updated_at = EXCLUDED.last_updated_at,
                    article_count = EXCLUDED.article_count,
                    source_count = EXCLUDED.source_count,
                    contradiction_count = EXCLUDED.contradiction_count,
                    importance_score = COALESCE(EXCLUDED.importance_score, canonical_events.importance_score),
                    importance_reasons = CASE
                        WHEN EXCLUDED.importance_reasons = '[]'::jsonb THEN canonical_events.importance_reasons
                        ELSE EXCLUDED.importance_reasons
                    END,
                    linked_structured_event_ids = EXCLUDED.linked_structured_event_ids,
                    article_urls = EXCLUDED.article_urls,
                    computed_at = EXCLUDED.computed_at,
                    payload = EXCLUDED.payload
                """,
                (
                    event_id,
                    row.get("topic") or "",
                    row.get("label") or "",
                    row.get("event_type"),
                    row.get("status") or "developing",
                    row.get("geo_country"),
                    row.get("geo_region"),
                    row.get("latitude"),
                    row.get("longitude"),
                    row.get("first_reported_at"),
                    row.get("last_updated_at"),
                    int(row.get("article_count") or 0),
                    int(row.get("source_count") or 0),
                    int(row.get("perspective_count") or 0),
                    int(row.get("contradiction_count") or 0),
                    float(row.get("importance_score") or 0.0),
                    importance_reasons,
                    linked,
                    article_urls,
                    row.get("first_seen_at") or now,
                    now,
                    payload,
                ),
            )
            written += 1
    return written


def update_canonical_event_synthesis(
    event_id: str,
    *,
    neutral_summary: str,
    neutral_confidence: float,
    perspective_count: int | None = None,
    contradiction_count: int | None = None,
) -> bool:
    if not event_id:
        return False
    now = time.time()
    perspective_sql = (
        ", perspective_count = %s" if perspective_count is not None else ""
    )
    contradiction_sql = (
        ", contradiction_count = %s" if contradiction_count is not None else ""
    )
    params: list[object] = [neutral_summary, float(neutral_confidence), now]
    if perspective_count is not None:
        params.append(perspective_count)
    if contradiction_count is not None:
        params.append(contradiction_count)
    params.append(event_id)
    with _connect() as conn:
        result = conn.execute(
            f"""
            UPDATE canonical_events
            SET neutral_summary = %s,
                neutral_confidence = %s,
                neutral_generated_at = %s
                {perspective_sql}
                {contradiction_sql}
            WHERE event_id = %s
            """,
            params,
        )
        return (result.rowcount or 0) > 0


def get_canonical_events(
    topic: str | None = None,
    status: str | None = None,
    limit: int = 40,
) -> list[dict]:
    clauses: list[str] = []
    params: list[object] = []
    if topic:
        clauses.append("topic = %s")
        params.append(topic)
    if status:
        clauses.append("status = %s")
        params.append(status)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(max(1, min(limit, 500)))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM canonical_events
            {where}
            ORDER BY COALESCE(importance_score, 0) DESC, computed_at DESC, last_updated_at DESC
            LIMIT %s
            """,
            params,
        ).fetchall()
    return [_row_to_canonical_event(row) for row in rows]


def get_canonical_event(event_id: str) -> dict | None:
    if not event_id:
        return None
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM canonical_events WHERE event_id = %s",
            (event_id,),
        ).fetchone()
    return _row_to_canonical_event(row) if row else None


def upsert_event_perspectives(rows: list[dict]) -> int:
    if not rows:
        return 0
    now = time.time()
    written = 0
    with _connect() as conn:
        for row in rows:
            pid = (row.get("perspective_id") or "").strip()
            if not pid:
                continue
            frame_counts = json.dumps(row.get("frame_counts") or {}, sort_keys=True)
            matched_terms = json.dumps(row.get("matched_terms") or [], sort_keys=True)
            payload = json.dumps(row.get("payload") or {}, sort_keys=True, default=str)
            conn.execute(
                """
                INSERT INTO event_perspectives (
                    perspective_id, event_id, article_url,
                    source_name, source_domain, source_reliability_score,
                    source_trust_tier, source_region,
                    dominant_frame, frame_counts, matched_terms,
                    claim_text, claim_type, claim_resolution_status,
                    sentiment, published_at, analyzed_at, payload
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (perspective_id) DO UPDATE SET
                    dominant_frame = EXCLUDED.dominant_frame,
                    frame_counts = EXCLUDED.frame_counts,
                    matched_terms = EXCLUDED.matched_terms,
                    claim_text = COALESCE(EXCLUDED.claim_text, event_perspectives.claim_text),
                    claim_type = COALESCE(EXCLUDED.claim_type, event_perspectives.claim_type),
                    claim_resolution_status = COALESCE(EXCLUDED.claim_resolution_status, event_perspectives.claim_resolution_status),
                    source_reliability_score = COALESCE(EXCLUDED.source_reliability_score, event_perspectives.source_reliability_score),
                    analyzed_at = EXCLUDED.analyzed_at,
                    payload = EXCLUDED.payload
                """,
                (
                    pid,
                    row["event_id"],
                    row.get("article_url"),
                    row["source_name"],
                    row.get("source_domain"),
                    row.get("source_reliability_score"),
                    row.get("source_trust_tier"),
                    row.get("source_region"),
                    row.get("dominant_frame"),
                    frame_counts,
                    matched_terms,
                    row.get("claim_text"),
                    row.get("claim_type"),
                    row.get("claim_resolution_status"),
                    row.get("sentiment"),
                    row.get("published_at"),
                    row.get("analyzed_at") or now,
                    payload,
                ),
            )
            written += 1
    return written


def get_event_perspectives(event_id: str) -> list[dict]:
    if not event_id:
        return []
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM event_perspectives
            WHERE event_id = %s
            ORDER BY source_reliability_score DESC NULLS LAST, analyzed_at DESC
            """,
            (event_id,),
        ).fetchall()
    return [_row_to_perspective(row) for row in rows]


def get_latest_canonical_event_observation(event_id: str) -> dict | None:
    key = (event_id or "").strip()
    if not key:
        return None
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM canonical_event_observations
            WHERE event_id = %s
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (key,),
        ).fetchone()
    if not row:
        return None
    payload = row.get("payload")
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "event_id": row.get("event_id"),
        "topic": row.get("topic"),
        "observation_key": row.get("observation_key"),
        "observed_at": row.get("observed_at"),
        "article_count": int(row.get("article_count") or 0),
        "source_count": int(row.get("source_count") or 0),
        "contradiction_count": int(row.get("contradiction_count") or 0),
        "tier_1_source_count": int(row.get("tier_1_source_count") or 0),
        "importance_score": float(row.get("importance_score") or 0.0),
        "payload": payload or {},
    }


def upsert_canonical_event_observations(rows: list[dict]) -> int:
    if not rows:
        return 0
    now = time.time()
    written = 0
    with _connect() as conn:
        for row in rows:
            event_id = (row.get("event_id") or "").strip()
            observation_key = (row.get("observation_key") or "").strip()
            if not event_id or not observation_key:
                continue
            topic = (row.get("topic") or "").strip() or None
            observed_at = float(row.get("observed_at") or now)
            payload = row.get("payload")
            payload_json = (
                json.dumps(payload, sort_keys=True, default=str)
                if isinstance(payload, dict)
                else "{}"
            )
            conn.execute(
                """
                INSERT INTO canonical_event_observations (
                    event_id,
                    topic,
                    observation_key,
                    observed_at,
                    article_count,
                    source_count,
                    contradiction_count,
                    tier_1_source_count,
                    importance_score,
                    payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (event_id, observation_key) DO UPDATE SET
                    topic = COALESCE(EXCLUDED.topic, canonical_event_observations.topic),
                    observed_at = GREATEST(canonical_event_observations.observed_at, EXCLUDED.observed_at),
                    article_count = EXCLUDED.article_count,
                    source_count = EXCLUDED.source_count,
                    contradiction_count = EXCLUDED.contradiction_count,
                    tier_1_source_count = EXCLUDED.tier_1_source_count,
                    importance_score = EXCLUDED.importance_score,
                    payload = EXCLUDED.payload
                """,
                (
                    event_id,
                    topic,
                    observation_key,
                    observed_at,
                    int(row.get("article_count") or 0),
                    int(row.get("source_count") or 0),
                    int(row.get("contradiction_count") or 0),
                    int(row.get("tier_1_source_count") or 0),
                    float(row.get("importance_score") or 0.0),
                    payload_json,
                ),
            )
            written += 1
    return written


# ── event identity resolution


def list_observation_keys_for_event(event_id: str, limit: int = 50) -> list[str]:
    key = (event_id or "").strip()
    if not key:
        return []
    safe_limit = max(1, min(int(limit), 500))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT observation_key
            FROM event_identity_map
            WHERE event_id = %s
            ORDER BY last_seen_at DESC
            LIMIT %s
            """,
            (key, safe_limit),
        ).fetchall()
    return [str(row.get("observation_key")) for row in rows if row.get("observation_key")]


def load_event_identity_history(event_id: str, limit: int = 50) -> list[dict]:
    key = (event_id or "").strip()
    if not key:
        return []
    safe_limit = max(1, min(int(limit), 500))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT observation_key, event_id, action, confidence, reasons, created_at
            FROM event_identity_events
            WHERE event_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (key, safe_limit),
        ).fetchall()
    history: list[dict] = []
    for row in rows:
        reasons = row.get("reasons")
        if isinstance(reasons, str):
            reasons = json.loads(reasons) if reasons else {}
        history.append(
            {
                "observation_key": row.get("observation_key"),
                "event_id": row.get("event_id"),
                "action": row.get("action"),
                "confidence": row.get("confidence"),
                "reasons": reasons or {},
                "created_at": row.get("created_at"),
            }
        )
    return history


def get_event_id_for_observation_key(observation_key: str) -> str | None:
    key = (observation_key or "").strip()
    if not key:
        return None
    with _connect() as conn:
        row = conn.execute(
            "SELECT event_id FROM event_identity_map WHERE observation_key = %s",
            (key,),
        ).fetchone()
    return (row.get("event_id") if row else None) or None


def list_canonical_identity_candidates(
    *, topic: str, limit: int = 500
) -> list[dict]:
    t = (topic or "").strip()
    if not t:
        return []
    safe_limit = max(1, min(int(limit), 2000))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT event_id, label, article_urls, linked_structured_event_ids, payload, computed_at
            FROM canonical_events
            WHERE topic = %s
            ORDER BY computed_at DESC
            LIMIT %s
            """,
            (t, safe_limit),
        ).fetchall()

    candidates: list[dict] = []
    for row in rows:
        article_urls = row.get("article_urls")
        linked = row.get("linked_structured_event_ids")
        payload = row.get("payload")
        if isinstance(article_urls, str):
            article_urls = json.loads(article_urls) if article_urls else []
        if isinstance(linked, str):
            linked = json.loads(linked) if linked else []
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        candidates.append(
            {
                "event_id": row.get("event_id"),
                "label": row.get("label"),
                "article_urls": article_urls or [],
                "linked_structured_event_ids": linked or [],
                "payload": payload or {},
            }
        )
    return candidates


def upsert_event_identity_mappings(rows: list[dict]) -> int:
    if not rows:
        return 0
    now = time.time()
    written = 0
    with _connect() as conn:
        for row in rows:
            observation_key = (row.get("observation_key") or "").strip()
            event_id = (row.get("event_id") or "").strip()
            if not observation_key or not event_id:
                continue
            topic = (row.get("topic") or "").strip() or None
            confidence = row.get("identity_confidence")
            reasons = row.get("identity_reasons")
            first_mapped_at = float(row.get("first_mapped_at") or now)
            last_seen_at = float(row.get("last_seen_at") or now)
            reasons_json = (
                json.dumps(reasons, sort_keys=True, default=str)
                if isinstance(reasons, dict)
                else "{}"
            )
            conn.execute(
                """
                INSERT INTO event_identity_map (
                    observation_key, event_id, topic,
                    first_mapped_at, last_seen_at,
                    identity_confidence, identity_reasons
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (observation_key) DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    topic = COALESCE(EXCLUDED.topic, event_identity_map.topic),
                    first_mapped_at = LEAST(event_identity_map.first_mapped_at, EXCLUDED.first_mapped_at),
                    last_seen_at = GREATEST(event_identity_map.last_seen_at, EXCLUDED.last_seen_at),
                    identity_confidence = COALESCE(EXCLUDED.identity_confidence, event_identity_map.identity_confidence),
                    identity_reasons = CASE
                        WHEN EXCLUDED.identity_reasons = '{}'::jsonb THEN event_identity_map.identity_reasons
                        ELSE EXCLUDED.identity_reasons
                    END
                """,
                (
                    observation_key,
                    event_id,
                    topic,
                    first_mapped_at,
                    last_seen_at,
                    float(confidence) if confidence is not None else None,
                    reasons_json,
                ),
            )
            written += 1
    return written


def append_event_identity_events(rows: list[dict]) -> int:
    if not rows:
        return 0
    now = time.time()
    written = 0
    with _connect() as conn:
        for row in rows:
            observation_key = (row.get("observation_key") or "").strip()
            event_id = (row.get("event_id") or "").strip()
            action = (row.get("action") or "").strip()
            if not observation_key or not event_id or not action:
                continue
            confidence = row.get("confidence")
            reasons = row.get("reasons")
            created_at = float(row.get("created_at") or now)
            reasons_json = (
                json.dumps(reasons, sort_keys=True, default=str)
                if isinstance(reasons, dict)
                else "{}"
            )
            conn.execute(
                """
                INSERT INTO event_identity_events (
                    observation_key, event_id, action, confidence, reasons, created_at
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    observation_key,
                    event_id,
                    action,
                    float(confidence) if confidence is not None else None,
                    reasons_json,
                    created_at,
                ),
            )
            written += 1
    return written
