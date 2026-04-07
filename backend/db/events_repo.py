import json
import time
from datetime import datetime, timezone, timedelta

from db.common import (
    _connect,
    _row_to_structured_event,
    _row_to_canonical_event,
    _row_to_perspective,
    _parse_published_at,
)


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
        clauses = ["event_date >= %s", *base_clauses]
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
            SELECT event_id, latitude, longitude, country, admin1, admin2, location, event_date
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
            payload = json.dumps(row.get("payload") or {}, sort_keys=True, default=str)
            conn.execute(
                """
                INSERT INTO canonical_events (
                    event_id, topic, label, event_type, status,
                    geo_country, geo_region, latitude, longitude,
                    first_reported_at, last_updated_at,
                    article_count, source_count, perspective_count, contradiction_count,
                    linked_structured_event_ids, article_urls,
                    first_seen_at, computed_at, payload
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
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
            ORDER BY computed_at DESC, last_updated_at DESC
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
