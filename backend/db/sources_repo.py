import json
import time

from db.common import _connect, _canonical_raw_document_id, _stable_hash


def upsert_source_registry(sources: list[dict]) -> int:
    if not sources:
        return 0

    now = time.time()
    inserted = 0
    with _connect() as conn:
        for seed in sources:
            source_id = seed["source_id"]
            metadata = json.dumps(seed.get("metadata") or {}, sort_keys=True)
            existing = conn.execute(
                "SELECT source_id FROM source_registry WHERE source_id = %s",
                (source_id,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO source_registry (
                    source_id, source_name, source_domain, source_type, trust_tier, region, language,
                    active, metadata, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                ON CONFLICT (source_id) DO UPDATE SET
                    source_name = EXCLUDED.source_name,
                    source_domain = EXCLUDED.source_domain,
                    source_type = EXCLUDED.source_type,
                    trust_tier = EXCLUDED.trust_tier,
                    region = EXCLUDED.region,
                    language = EXCLUDED.language,
                    active = EXCLUDED.active,
                    metadata = EXCLUDED.metadata,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    source_id,
                    seed["source_name"],
                    seed.get("source_domain"),
                    seed["source_type"],
                    seed["trust_tier"],
                    seed.get("region"),
                    seed.get("language", "en"),
                    bool(seed.get("active", True)),
                    metadata,
                    now,
                    now,
                ),
            )
            if not existing:
                inserted += 1
    return inserted


def get_source_registry(
    source_type: str | None = None, active_only: bool = True
) -> list[dict]:
    clauses = []
    params: list[object] = []
    if source_type:
        clauses.append("source_type = %s")
        params.append(source_type)
    if active_only:
        pass
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT source_id, source_name, source_domain, source_type, trust_tier, region, language,
                   active, metadata, created_at, updated_at
            FROM source_registry
            {where}
            ORDER BY trust_tier ASC, source_name ASC
            """,
            params,
        ).fetchall()

    results = []
    for row in rows:
        metadata = row["metadata"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata) if metadata else {}
        results.append(
            {
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "source_domain": row["source_domain"],
                "source_type": row["source_type"],
                "trust_tier": row["trust_tier"],
                "region": row["region"],
                "language": row["language"],
                "active": bool(row["active"]),
                "metadata": metadata or {},
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )
    return results


def build_source_registry_lookup(active_only: bool = True) -> dict:
    rows = get_source_registry(source_type=None, active_only=active_only)
    by_name: dict[str, dict] = {}
    by_domain: dict[str, dict] = {}
    for row in rows:
        if row.get("source_name"):
            by_name[row["source_name"].strip().lower()] = row
        if row.get("source_domain"):
            by_domain[row["source_domain"].strip().lower()] = row
    return {"by_name": by_name, "by_domain": by_domain}


def resolve_registry_row_for_article(
    source: str, source_domain: str | None, lookup: dict
) -> dict | None:
    dom_key = (source_domain or "").strip().lower()
    if dom_key:
        hit = lookup.get("by_domain", {}).get(dom_key)
        if hit is not None:
            return hit
    name_key = (source or "").strip().lower()
    if name_key:
        return lookup.get("by_name", {}).get(name_key)
    return None


def set_source_registry_active(source_ids: list[str], active: bool) -> int:
    normalized = [source_id for source_id in source_ids if source_id]
    if not normalized:
        return 0

    placeholders = ", ".join(["%s"] * len(normalized))
    with _connect() as conn:
        result = conn.execute(
            f"UPDATE source_registry SET active = %s, updated_at = %s WHERE source_id IN ({placeholders})",
            (active, time.time(), *normalized),
        )
        return result.rowcount or 0


def upsert_historical_url_queue(records: list[dict]) -> int:
    if not records:
        return 0

    now = time.time()
    inserted_or_updated = 0
    with _connect() as conn:
        for record in records:
            try:
                normalized = record if record.get("_normalized") else record
            except ValueError:
                continue

            existing = conn.execute(
                """
                SELECT canonical_url, title, source_name, source_domain, published_at, language,
                       discovered_via, topic_guess, gdelt_query, gdelt_window_start, gdelt_window_end,
                       fetch_status, last_attempt_at, attempt_count, payload
                FROM historical_url_queue
                WHERE url = %s
                """,
                (normalized["url"],),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO historical_url_queue (
                    url, canonical_url, title, source_name, source_domain, published_at, language,
                    discovered_via, topic_guess, gdelt_query, gdelt_window_start, gdelt_window_end,
                    fetch_status, last_attempt_at, attempt_count, payload, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s::jsonb, %s, %s
                )
                ON CONFLICT (url) DO UPDATE SET
                    canonical_url = EXCLUDED.canonical_url,
                    title = COALESCE(EXCLUDED.title, historical_url_queue.title),
                    source_name = COALESCE(EXCLUDED.source_name, historical_url_queue.source_name),
                    source_domain = COALESCE(EXCLUDED.source_domain, historical_url_queue.source_domain),
                    published_at = COALESCE(EXCLUDED.published_at, historical_url_queue.published_at),
                    language = COALESCE(EXCLUDED.language, historical_url_queue.language),
                    discovered_via = EXCLUDED.discovered_via,
                    topic_guess = COALESCE(EXCLUDED.topic_guess, historical_url_queue.topic_guess),
                    gdelt_query = COALESCE(EXCLUDED.gdelt_query, historical_url_queue.gdelt_query),
                    gdelt_window_start = COALESCE(EXCLUDED.gdelt_window_start, historical_url_queue.gdelt_window_start),
                    gdelt_window_end = COALESCE(EXCLUDED.gdelt_window_end, historical_url_queue.gdelt_window_end),
                    fetch_status = EXCLUDED.fetch_status,
                    last_attempt_at = COALESCE(EXCLUDED.last_attempt_at, historical_url_queue.last_attempt_at),
                    attempt_count = EXCLUDED.attempt_count,
                    payload = EXCLUDED.payload,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    normalized["url"],
                    normalized["canonical_url"],
                    normalized["title"],
                    normalized["source_name"],
                    normalized["source_domain"],
                    normalized["published_at"],
                    normalized["language"],
                    normalized["discovered_via"],
                    normalized["topic_guess"],
                    normalized["gdelt_query"],
                    normalized["gdelt_window_start"],
                    normalized["gdelt_window_end"],
                    normalized["fetch_status"],
                    normalized["last_attempt_at"],
                    normalized["attempt_count"],
                    json.dumps(normalized["payload"], sort_keys=True),
                    now,
                    now,
                ),
            )
            comparable_payload = json.dumps(normalized["payload"], sort_keys=True)
            if not existing:
                inserted_or_updated += 1
                continue
            existing_payload = existing["payload"]
            if isinstance(existing_payload, dict):
                existing_payload = json.dumps(existing_payload, sort_keys=True)
            changed = any(
                [
                    existing["canonical_url"] != normalized["canonical_url"],
                    (existing["title"] or None) != normalized["title"],
                    (existing["source_name"] or None) != normalized["source_name"],
                    (existing["source_domain"] or None) != normalized["source_domain"],
                    (existing["published_at"] or None) != normalized["published_at"],
                    (existing["language"] or None) != normalized["language"],
                    (existing["discovered_via"] or None)
                    != normalized["discovered_via"],
                    (existing["topic_guess"] or None) != normalized["topic_guess"],
                    (existing["gdelt_query"] or None) != normalized["gdelt_query"],
                    (existing["gdelt_window_start"] or None)
                    != normalized["gdelt_window_start"],
                    (existing["gdelt_window_end"] or None)
                    != normalized["gdelt_window_end"],
                    (existing["fetch_status"] or None) != normalized["fetch_status"],
                    existing["last_attempt_at"] != normalized["last_attempt_at"],
                    int(existing["attempt_count"] or 0) != normalized["attempt_count"],
                    (existing_payload or "") != comparable_payload,
                ]
            )
            if changed:
                inserted_or_updated += 1
    return inserted_or_updated


def get_historical_url_queue_batch(
    limit: int = 50,
    statuses: list[str] | None = None,
    source_domain: str | None = None,
) -> list[dict]:
    normalized_statuses = [
        status for status in (statuses or ["pending", "retry"]) if status
    ]
    clauses = []
    params: list[object] = []

    if normalized_statuses:
        status_placeholders = ", ".join(["%s"] * len(normalized_statuses))
        clauses.append(f"fetch_status IN ({status_placeholders})")
        params.extend(normalized_statuses)
    if source_domain:
        clauses.append("source_domain = %s")
        params.append(source_domain.strip().lower())

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(max(1, limit))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM historical_url_queue
            {where}
            ORDER BY
                CASE fetch_status
                    WHEN 'pending' THEN 0
                    WHEN 'retry' THEN 1
                    WHEN 'failed' THEN 2
                    ELSE 3
                END,
                CASE WHEN topic_guess IS NULL OR topic_guess = '' THEN 1 ELSE 0 END,
                COALESCE(published_at, '') DESC,
                updated_at ASC
            LIMIT %s
            """,
            params,
        ).fetchall()
    from db.common import _row_to_historical_queue_item

    return [_row_to_historical_queue_item(row) for row in rows]


def update_historical_url_queue_status(
    url: str,
    fetch_status: str,
    *,
    last_attempt_at: float | None = None,
    attempt_count: int | None = None,
    payload_patch: dict | None = None,
) -> None:
    if not url:
        return

    now = time.time()
    with _connect() as conn:
        existing = conn.execute(
            "SELECT payload, attempt_count FROM historical_url_queue WHERE url = %s",
            (url,),
        ).fetchone()
        if not existing:
            return

        payload = existing["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        payload = payload or {}
        if payload_patch:
            payload.update(payload_patch)
        next_attempt_count = (
            attempt_count
            if attempt_count is not None
            else int(existing["attempt_count"] or 0)
        )
        attempt_ts = last_attempt_at if last_attempt_at is not None else time.time()

        conn.execute(
            """
            UPDATE historical_url_queue
            SET fetch_status = %s,
                last_attempt_at = %s,
                attempt_count = %s,
                payload = %s::jsonb,
                updated_at = %s
            WHERE url = %s
            """,
            (
                fetch_status,
                attempt_ts,
                next_attempt_count,
                json.dumps(payload, sort_keys=True),
                now,
                url,
            ),
        )


def record_raw_source_documents(documents: list[dict]) -> int:
    if not documents:
        return 0

    inserted = 0
    with _connect() as conn:
        for document in documents:
            payload = json.dumps(document.get("payload") or {}, sort_keys=True)
            content_hash = document.get("content_hash") or _stable_hash(
                [
                    document.get("source_id", ""),
                    document.get("external_id", ""),
                    document.get("url", ""),
                    document.get("title", ""),
                    document.get("published_at", ""),
                ]
            )
            existing = conn.execute(
                """
                SELECT document_id
                FROM raw_source_documents
                WHERE document_id = %s OR (source_id = %s AND content_hash = %s)
                LIMIT 1
                """,
                (document["document_id"], document["source_id"], content_hash),
            ).fetchone()
            target_document_id = (
                existing["document_id"]
                if existing
                else _canonical_raw_document_id(document["source_id"], content_hash)
            )
            conn.execute(
                """
                INSERT INTO raw_source_documents (
                    document_id, source_id, external_id, url, title, published_at, fetched_at,
                    language, source_type, trust_tier, content_hash, payload, normalized_ref
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (document_id) DO UPDATE SET
                    source_id = EXCLUDED.source_id,
                    external_id = EXCLUDED.external_id,
                    url = EXCLUDED.url,
                    title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at,
                    fetched_at = EXCLUDED.fetched_at,
                    language = EXCLUDED.language,
                    source_type = EXCLUDED.source_type,
                    trust_tier = EXCLUDED.trust_tier,
                    content_hash = EXCLUDED.content_hash,
                    payload = EXCLUDED.payload,
                    normalized_ref = EXCLUDED.normalized_ref
                """,
                (
                    target_document_id,
                    document["source_id"],
                    document.get("external_id"),
                    document.get("url"),
                    document.get("title"),
                    document.get("published_at"),
                    document["fetched_at"],
                    document.get("language", "en"),
                    document["source_type"],
                    document["trust_tier"],
                    content_hash,
                    payload,
                    document.get("normalized_ref"),
                ),
            )
            if not existing:
                inserted += 1
    return inserted


def upsert_official_updates(updates: list[dict]) -> int:
    if not updates:
        return 0

    inserted = 0
    with _connect() as conn:
        for update in updates:
            payload = json.dumps(update.get("payload") or {}, sort_keys=True)
            existing = conn.execute(
                "SELECT update_id FROM official_updates WHERE update_id = %s",
                (update["update_id"],),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO official_updates (
                    update_id, issuing_body, update_type, title, url, published_at, fetched_at,
                    region, language, trust_tier, content_hash, payload, summary
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (update_id) DO UPDATE SET
                    issuing_body = EXCLUDED.issuing_body,
                    update_type = EXCLUDED.update_type,
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    published_at = EXCLUDED.published_at,
                    fetched_at = EXCLUDED.fetched_at,
                    region = EXCLUDED.region,
                    language = EXCLUDED.language,
                    trust_tier = EXCLUDED.trust_tier,
                    content_hash = EXCLUDED.content_hash,
                    payload = EXCLUDED.payload,
                    summary = EXCLUDED.summary
                """,
                (
                    update["update_id"],
                    update["issuing_body"],
                    update["update_type"],
                    update["title"],
                    update.get("url"),
                    update.get("published_at"),
                    update["fetched_at"],
                    update.get("region"),
                    update.get("language", "en"),
                    update["trust_tier"],
                    update["content_hash"],
                    payload,
                    update.get("summary"),
                ),
            )
            if not existing:
                inserted += 1
    return inserted


def load_entity_reference(
    entity: str, provider: str = "wikipedia", max_age_hours: int | None = 336
) -> dict | None:
    entity_key = _stable_hash([(entity or "").strip().lower()])
    if not entity_key:
        return None

    with _connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM entity_reference_cache
            WHERE entity_key = %s AND provider = %s
            """,
            (entity_key, provider),
        ).fetchone()
    if not row:
        return None

    fetched_at = float(row.get("fetched_at") or 0)
    if max_age_hours is not None and fetched_at:
        age_seconds = time.time() - fetched_at
        if age_seconds > max_age_hours * 3600:
            return None

    payload = row.get("payload")
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}

    return {
        "entity": row.get("query_text"),
        "entity_key": row.get("entity_key"),
        "provider": row.get("provider"),
        "title": row.get("reference_title"),
        "summary": row.get("reference_summary"),
        "url": row.get("reference_url"),
        "thumbnail_url": row.get("thumbnail_url"),
        "page_id": row.get("page_id"),
        "language": row.get("language"),
        "status": row.get("status"),
        "error": row.get("error"),
        "payload": payload or {},
        "fetched_at": fetched_at,
        "reference_only": True,
    }


def save_entity_reference(
    entity: str,
    provider: str,
    reference: dict,
    status: str = "ok",
    error: str | None = None,
) -> None:
    entity_key = _stable_hash([(entity or "").strip().lower()])
    if not entity_key:
        return

    fetched_at = time.time()
    payload = json.dumps(reference or {}, sort_keys=True)
    title = reference.get("title")
    summary = reference.get("summary")
    url = reference.get("url")
    thumbnail_url = reference.get("thumbnail_url")
    page_id = (
        str(reference.get("page_id")) if reference.get("page_id") is not None else None
    )
    language = reference.get("language")

    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO entity_reference_cache (
                entity_key, provider, query_text, reference_title, reference_summary,
                reference_url, thumbnail_url, page_id, language, status, error, payload, fetched_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (entity_key) DO UPDATE SET
                provider = EXCLUDED.provider,
                query_text = EXCLUDED.query_text,
                reference_title = EXCLUDED.reference_title,
                reference_summary = EXCLUDED.reference_summary,
                reference_url = EXCLUDED.reference_url,
                thumbnail_url = EXCLUDED.thumbnail_url,
                page_id = EXCLUDED.page_id,
                language = EXCLUDED.language,
                status = EXCLUDED.status,
                error = EXCLUDED.error,
                payload = EXCLUDED.payload,
                fetched_at = EXCLUDED.fetched_at
            """,
            (
                entity_key,
                provider,
                entity.strip(),
                title,
                summary,
                url,
                thumbnail_url,
                page_id,
                language,
                status,
                error,
                payload,
                fetched_at,
            ),
        )


def get_sources(limit: int = 12, hours: int = 72) -> list[dict]:
    cutoff = time.time() - hours * 3600
    cutoff_iso = datetime_from_timestamp(cutoff)
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT source, source_domain, COUNT(*) AS article_count, MAX(published_at) AS latest_published_at
            FROM articles
            WHERE published_at >= %s
            GROUP BY source, source_domain
            ORDER BY article_count DESC, latest_published_at DESC
            LIMIT %s
            """,
            (cutoff_iso, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def get_warehouse_counts() -> dict:
    with _connect() as conn:
        raw_docs = conn.execute(
            "SELECT COUNT(*) AS count FROM raw_source_documents"
        ).fetchone()
        official = conn.execute(
            "SELECT COUNT(*) AS count FROM official_updates"
        ).fetchone()
        structured = conn.execute(
            "SELECT COUNT(*) AS count FROM structured_events"
        ).fetchone()
        channels = conn.execute(
            "SELECT COUNT(*) AS count FROM monitored_channels"
        ).fetchone()
        registry = conn.execute(
            "SELECT COUNT(*) AS count FROM source_registry"
        ).fetchone()
    return {
        "source_registry": int((registry["count"] if registry else 0) or 0),
        "raw_source_documents": int((raw_docs["count"] if raw_docs else 0) or 0),
        "official_updates": int((official["count"] if official else 0) or 0),
        "structured_events": int((structured["count"] if structured else 0) or 0),
        "monitored_channels": int((channels["count"] if channels else 0) or 0),
    }


def datetime_from_timestamp(ts: float) -> str:
    # helper to produce ISO string for queries
    from datetime import datetime, timezone

    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
