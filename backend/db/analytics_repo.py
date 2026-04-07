import json
import hashlib
import time
from datetime import datetime, timezone, timedelta

from db.common import (
    _connect,
    _normalize_entity_key,
)


def save_article_framing_signals(signals: list[dict]) -> int:
    if not signals:
        return 0

    saved = 0
    now = time.time()
    with _connect() as conn:
        for signal in signals:
            frame_counts = json.dumps(signal.get("frame_counts") or {}, sort_keys=True)
            matched_terms = json.dumps(
                signal.get("matched_terms") or {}, sort_keys=True
            )
            payload = json.dumps(signal.get("payload") or {}, sort_keys=True)
            conn.execute(
                """
                INSERT INTO article_framing_signals (
                    article_url, subject_key, subject_label, topic, source, published_at,
                    dominant_frame, frame_counts, matched_terms, payload, analyzed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                ON CONFLICT (article_url, subject_key) DO UPDATE SET
                    subject_label = EXCLUDED.subject_label,
                    topic = EXCLUDED.topic,
                    source = EXCLUDED.source,
                    published_at = EXCLUDED.published_at,
                    dominant_frame = EXCLUDED.dominant_frame,
                    frame_counts = EXCLUDED.frame_counts,
                    matched_terms = EXCLUDED.matched_terms,
                    payload = EXCLUDED.payload,
                    analyzed_at = EXCLUDED.analyzed_at
                """,
                (
                    signal["article_url"],
                    signal["subject_key"],
                    signal["subject_label"],
                    signal.get("topic"),
                    signal.get("source"),
                    signal.get("published_at"),
                    signal.get("dominant_frame"),
                    frame_counts,
                    matched_terms,
                    payload,
                    signal.get("analyzed_at", now),
                ),
            )
            saved += 1
    return saved


def load_article_framing_signals(
    subject: str, topic: str | None = None, days: int = 180, limit: int = 500
) -> list[dict]:
    subject_key = _normalize_entity_key(subject)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    params: list[object] = [subject_key, cutoff]
    where_topic = ""
    if topic:
        where_topic = "AND topic = %s"
        params.append(topic)
    params.append(limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM article_framing_signals
            WHERE subject_key = %s
              AND COALESCE(published_at, '') >= %s
              {where_topic}
            ORDER BY published_at ASC, analyzed_at ASC
            LIMIT %s
            """,
            params,
        ).fetchall()

    signals = []
    for row in rows:
        frame_counts = row["frame_counts"]
        matched_terms = row["matched_terms"]
        payload = row["payload"]
        if isinstance(frame_counts, str):
            frame_counts = json.loads(frame_counts) if frame_counts else {}
        if isinstance(matched_terms, str):
            matched_terms = json.loads(matched_terms) if matched_terms else {}
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        signals.append(
            {
                "article_url": row["article_url"],
                "subject_key": row["subject_key"],
                "subject_label": row["subject_label"],
                "topic": row["topic"],
                "source": row["source"],
                "published_at": row["published_at"],
                "dominant_frame": row["dominant_frame"],
                "frame_counts": frame_counts or {},
                "matched_terms": matched_terms or {},
                "payload": payload or {},
                "analyzed_at": row["analyzed_at"],
            }
        )
    return signals


def save_narrative_drift_snapshot(
    subject: str,
    topic: str | None,
    window_days: int,
    payload: dict,
) -> None:
    subject_key = _normalize_entity_key(subject)
    snapshot_key = f"{subject_key}:{topic or 'global'}:{window_days}"
    article_count = int(payload.get("article_count", 0) or 0)
    earliest = payload.get("earliest_published_at")
    latest = payload.get("latest_published_at")
    serialized_payload = json.dumps(payload or {}, sort_keys=True)
    snapshot_hash = hashlib.sha256(
        " | ".join([snapshot_key, serialized_payload]).encode("utf-8")
    ).hexdigest()
    generated_at = time.time()

    with _connect() as conn:
        existing = conn.execute(
            """
            SELECT snapshot_hash
            FROM narrative_drift_snapshots
            WHERE snapshot_key = %s
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (snapshot_key,),
        ).fetchone()
        if existing is not None and existing["snapshot_hash"] == snapshot_hash:
            return

        conn.execute(
            """
            INSERT INTO narrative_drift_snapshots (
                snapshot_key, subject_key, subject_label, topic, window_days, article_count,
                earliest_published_at, latest_published_at, snapshot_hash, payload, generated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            """,
            (
                snapshot_key,
                subject_key,
                subject.strip(),
                topic,
                window_days,
                article_count,
                earliest,
                latest,
                snapshot_hash,
                serialized_payload,
                generated_at,
            ),
        )


def load_narrative_drift_snapshot(
    subject: str,
    topic: str | None = None,
    window_days: int = 180,
    max_age_hours: int = 24,
) -> dict | None:
    subject_key = _normalize_entity_key(subject)
    snapshot_key = f"{subject_key}:{topic or 'global'}:{window_days}"
    cutoff = time.time() - (max_age_hours * 3600)
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM narrative_drift_snapshots
            WHERE snapshot_key = %s
              AND generated_at >= %s
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (snapshot_key, cutoff),
        ).fetchone()
    if not row:
        return None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "snapshot_key": row["snapshot_key"],
        "subject_key": row["subject_key"],
        "subject_label": row["subject_label"],
        "topic": row["topic"],
        "window_days": row["window_days"],
        "article_count": row["article_count"],
        "earliest_published_at": row["earliest_published_at"],
        "latest_published_at": row["latest_published_at"],
        "snapshot_hash": row["snapshot_hash"],
        "payload": payload or {},
        "generated_at": row["generated_at"],
    }


def get_recent_contradiction_records(
    topic: str | None = None, hours: int = 24 * 30, limit: int = 500
) -> list[dict]:
    cutoff = time.time() - (hours * 3600)
    params: list[object] = [cutoff]
    topic_clause = ""
    if topic:
        topic_clause = "AND topic = %s"
        params.append(topic)
    params.append(limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM contradiction_records
            WHERE generated_at >= %s
              {topic_clause}
            ORDER BY generated_at DESC
            LIMIT %s
            """,
            params,
        ).fetchall()

    records = []
    for row in rows:
        contradictions = row["contradictions"]
        article_urls = row["article_urls"]
        if isinstance(contradictions, str):
            contradictions = json.loads(contradictions) if contradictions else []
        if isinstance(article_urls, str):
            article_urls = json.loads(article_urls) if article_urls else []
        records.append(
            {
                "event_key": row["event_key"],
                "topic": row["topic"],
                "event_label": row["event_label"],
                "latest_update": row["latest_update"],
                "article_urls": article_urls or [],
                "contradictions": contradictions or [],
                "contradiction_count": row["contradiction_count"],
                "generated_at": row["generated_at"],
            }
        )
    return records


def load_contradiction_record(event_key: str) -> dict | None:
    if not event_key:
        return None
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM contradiction_records WHERE event_key = %s
            """,
            (event_key,),
        ).fetchone()
    if not row:
        return None
    contradictions = row.get("contradictions")
    article_urls = row.get("article_urls")
    if isinstance(contradictions, str):
        contradictions = json.loads(contradictions) if contradictions else []
    if isinstance(article_urls, str):
        article_urls = json.loads(article_urls) if article_urls else []
    return {
        "event_key": row.get("event_key"),
        "topic": row.get("topic"),
        "event_label": row.get("event_label"),
        "latest_update": row.get("latest_update"),
        "article_urls": article_urls or [],
        "contradictions": contradictions or [],
        "contradiction_count": row.get("contradiction_count"),
        "generated_at": row.get("generated_at"),
    }


def load_contradiction_history(event_key: str, limit: int = 10) -> list[dict]:
    if not event_key:
        return []
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM contradiction_history WHERE event_key = %s ORDER BY generated_at DESC LIMIT %s
            """,
            (event_key, max(1, int(limit))),
        ).fetchall()
    out: list[dict] = []
    for row in rows:
        contradictions = row.get("contradictions")
        article_urls = row.get("article_urls")
        if isinstance(contradictions, str):
            contradictions = json.loads(contradictions) if contradictions else []
        if isinstance(article_urls, str):
            article_urls = json.loads(article_urls) if article_urls else []
        out.append(
            {
                "event_key": row.get("event_key"),
                "topic": row.get("topic"),
                "event_label": row.get("event_label"),
                "latest_update": row.get("latest_update"),
                "article_urls": article_urls or [],
                "contradictions": contradictions or [],
                "contradiction_count": row.get("contradiction_count"),
                "generated_at": row.get("generated_at"),
            }
        )
    return out


def replace_claim_resolution_snapshot(snapshot_key: str, records: list[dict]) -> int:
    now = time.time()
    with _connect() as conn:
        conn.execute(
            "DELETE FROM claim_resolution_records WHERE snapshot_key = %s",
            (snapshot_key,),
        )
        saved = 0
        for record in records:
            base_claim_record_key = record["claim_record_key"]
            storage_claim_record_key = hashlib.sha256(
                f"{snapshot_key}|{base_claim_record_key}".encode("utf-8")
            ).hexdigest()
            payload_data = dict(record.get("payload") or {})
            payload_data.setdefault("base_claim_record_key", base_claim_record_key)
            payload = json.dumps(payload_data, sort_keys=True)
            conn.execute(
                """
                INSERT INTO claim_resolution_records (
                    claim_record_key, snapshot_key, event_key, topic, event_label, source_name,
                    claim_text, opposing_claim_text, conflict_type, resolution_status, confidence,
                    evidence_url, published_at, payload, generated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (claim_record_key) DO UPDATE SET
                    snapshot_key = EXCLUDED.snapshot_key,
                    event_key = EXCLUDED.event_key,
                    topic = EXCLUDED.topic,
                    event_label = EXCLUDED.event_label,
                    source_name = EXCLUDED.source_name,
                    claim_text = EXCLUDED.claim_text,
                    opposing_claim_text = EXCLUDED.opposing_claim_text,
                    conflict_type = EXCLUDED.conflict_type,
                    resolution_status = EXCLUDED.resolution_status,
                    confidence = EXCLUDED.confidence,
                    evidence_url = EXCLUDED.evidence_url,
                    published_at = EXCLUDED.published_at,
                    payload = EXCLUDED.payload,
                    generated_at = EXCLUDED.generated_at
                """,
                (
                    storage_claim_record_key,
                    snapshot_key,
                    record.get("event_key"),
                    record.get("topic"),
                    record.get("event_label"),
                    record["source_name"],
                    record["claim_text"],
                    record.get("opposing_claim_text"),
                    record.get("conflict_type"),
                    record["resolution_status"],
                    record.get("confidence"),
                    record.get("evidence_url"),
                    record.get("published_at"),
                    payload,
                    record.get("generated_at", now),
                ),
            )
            saved += 1
    return saved


def save_source_reliability_snapshot(
    snapshot_key: str, rows: list[dict], topic: str | None = None
) -> int:
    if not rows:
        return 0
    now = time.time()
    saved = 0
    with _connect() as conn:
        for row in rows:
            payload = json.dumps(row.get("payload") or {}, sort_keys=True)
            conn.execute(
                """
                INSERT INTO source_reliability_snapshots (
                    snapshot_key, source_name, topic, corroborated_count, contradicted_count,
                    unresolved_count, mixed_count, claim_count, empirical_score, weight_multiplier,
                    payload, generated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    snapshot_key,
                    row["source_name"],
                    topic,
                    row.get("corroborated_count", 0),
                    row.get("contradicted_count", 0),
                    row.get("unresolved_count", 0),
                    row.get("mixed_count", 0),
                    row.get("claim_count", 0),
                    row.get("empirical_score", 0.5),
                    row.get("weight_multiplier", 1.0),
                    payload,
                    row.get("generated_at", now),
                ),
            )
            saved += 1
    return saved


def load_latest_source_reliability(
    topic: str | None = None, max_age_hours: int = 24 * 7
) -> dict[str, dict]:
    cutoff = time.time() - (max_age_hours * 3600)
    params: list[object] = [cutoff]
    topic_clause = ""
    if topic is None:
        topic_clause = "AND topic IS NULL"
    else:
        topic_clause = "AND topic = %s"
        params.append(topic)

    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT ON (LOWER(source_name))
                source_name, topic, corroborated_count, contradicted_count, unresolved_count, mixed_count,
                claim_count, empirical_score, weight_multiplier, payload, generated_at
            FROM source_reliability_snapshots
            WHERE generated_at >= %s
              {topic_clause}
            ORDER BY LOWER(source_name), generated_at DESC
            """,
            params,
        ).fetchall()
    result = {}
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        result[row["source_name"].strip().lower()] = {
            "source_name": row["source_name"],
            "topic": row["topic"],
            "corroborated_count": row["corroborated_count"],
            "contradicted_count": row["contradicted_count"],
            "unresolved_count": row["unresolved_count"],
            "mixed_count": row["mixed_count"],
            "claim_count": row["claim_count"],
            "empirical_score": float(row["empirical_score"] or 0.5),
            "weight_multiplier": float(row["weight_multiplier"] or 1.0),
            "payload": payload or {},
            "generated_at": row["generated_at"],
        }
    return result


def replace_materialized_story_clusters(
    *, topic: str, window_hours: int, rows: list[dict]
) -> int:
    if not topic:
        return 0
    window_hours = max(1, int(window_hours))
    now = time.time()
    with _connect() as conn:
        conn.execute(
            "DELETE FROM materialized_story_clusters WHERE topic = %s AND window_hours = %s",
            (topic, window_hours),
        )
        written = 0
        for row in rows:
            article_urls = json.dumps(row.get("article_urls") or [], sort_keys=True)
            linked = json.dumps(
                row.get("linked_structured_event_ids") or [], sort_keys=True
            )
            payload = json.dumps(
                row.get("event_payload") or {}, sort_keys=True, default=str
            )
            conn.execute(
                """
                INSERT INTO materialized_story_clusters (
                    cluster_key, topic, computed_at, window_hours, label, summary,
                    earliest_published_at, latest_published_at, article_urls,
                    linked_structured_event_ids, event_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
                """,
                (
                    row["cluster_key"],
                    topic,
                    now,
                    window_hours,
                    row["label"],
                    row.get("summary"),
                    row.get("earliest_published_at"),
                    row.get("latest_published_at"),
                    article_urls,
                    linked,
                    payload,
                ),
            )
            written += 1
    return written


def load_materialized_story_clusters(
    *,
    topic: str | None = None,
    window_hours: int | None = None,
    limit: int = 40,
) -> list[dict]:
    clauses: list[str] = []
    params: list[object] = []
    if topic:
        clauses.append("topic = %s")
        params.append(topic)
    if window_hours is not None:
        clauses.append("window_hours = %s")
        params.append(int(window_hours))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(max(1, min(limit, 500)))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM materialized_story_clusters
            {where}
            ORDER BY computed_at DESC, latest_published_at DESC
            LIMIT %s
            """,
            params,
        ).fetchall()
    out = []
    for row in rows:
        au = row.get("article_urls")
        lk = row.get("linked_structured_event_ids")
        ep = row.get("event_payload")
        if isinstance(au, str):
            au = json.loads(au) if au else []
        if isinstance(lk, str):
            lk = json.loads(lk) if lk else []
        if isinstance(ep, str):
            ep = json.loads(ep) if ep else {}
        out.append(
            {
                "cluster_key": row.get("cluster_key"),
                "topic": row.get("topic"),
                "computed_at": row.get("computed_at"),
                "window_hours": row.get("window_hours"),
                "label": row.get("label"),
                "summary": row.get("summary"),
                "earliest_published_at": row.get("earliest_published_at"),
                "latest_published_at": row.get("latest_published_at"),
                "article_urls": au or [],
                "linked_structured_event_ids": lk or [],
                "event_payload": ep or {},
            }
        )
    return out


def upsert_cluster_assignment_evidence(rows: list[dict]) -> int:
    if not rows:
        return 0
    now = time.time()
    written = 0
    with _connect() as conn:
        for row in rows:
            observation_key = (row.get("observation_key") or "").strip()
            article_url = (row.get("article_url") or "").strip()
            if not observation_key or not article_url:
                continue
            event_id = (row.get("event_id") or "").strip() or None
            topic = (row.get("topic") or "").strip() or None
            payload = json.dumps(row.get("payload") or {}, sort_keys=True, default=str)
            conn.execute(
                """
                INSERT INTO cluster_assignment_evidence (
                    observation_key,
                    event_id,
                    topic,
                    article_url,
                    rule,
                    entity_overlap,
                    anchor_overlap,
                    keyword_overlap,
                    time_gap_hours,
                    final_score,
                    payload,
                    computed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
                )
                ON CONFLICT (observation_key, article_url) DO UPDATE SET
                    event_id = COALESCE(EXCLUDED.event_id, cluster_assignment_evidence.event_id),
                    topic = COALESCE(EXCLUDED.topic, cluster_assignment_evidence.topic),
                    rule = EXCLUDED.rule,
                    entity_overlap = EXCLUDED.entity_overlap,
                    anchor_overlap = EXCLUDED.anchor_overlap,
                    keyword_overlap = EXCLUDED.keyword_overlap,
                    time_gap_hours = EXCLUDED.time_gap_hours,
                    final_score = EXCLUDED.final_score,
                    payload = EXCLUDED.payload,
                    computed_at = GREATEST(cluster_assignment_evidence.computed_at, EXCLUDED.computed_at)
                """,
                (
                    observation_key,
                    event_id,
                    topic,
                    article_url,
                    row.get("rule") or "fallback_in_cluster",
                    int(row.get("entity_overlap") or 0),
                    int(row.get("anchor_overlap") or 0),
                    int(row.get("keyword_overlap") or 0),
                    (
                        float(row.get("time_gap_hours"))
                        if row.get("time_gap_hours") is not None
                        else None
                    ),
                    float(row.get("final_score") or 0.0),
                    payload,
                    float(row.get("computed_at") or now),
                ),
            )
            written += 1
    return written


def load_cluster_assignment_evidence(
    observation_keys: list[str], limit_per_observation: int = 80
) -> dict[str, list[dict]]:
    keys = [str(key).strip() for key in observation_keys if str(key).strip()]
    if not keys:
        return {}
    keys = list(dict.fromkeys(keys))
    cap = max(1, min(int(limit_per_observation), 400))
    placeholders = ", ".join(["%s"] * len(keys))
    params: list[object] = [*keys, cap]
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT observation_key, event_id, topic, article_url, rule,
                   entity_overlap, anchor_overlap, keyword_overlap,
                   time_gap_hours, final_score, payload, computed_at
            FROM (
                SELECT
                    observation_key,
                    event_id,
                    topic,
                    article_url,
                    rule,
                    entity_overlap,
                    anchor_overlap,
                    keyword_overlap,
                    time_gap_hours,
                    final_score,
                    payload,
                    computed_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY observation_key
                        ORDER BY computed_at DESC, final_score DESC
                    ) AS rn
                FROM cluster_assignment_evidence
                WHERE observation_key IN ({placeholders})
            ) ranked
            WHERE rn <= %s
            ORDER BY observation_key ASC, final_score DESC, computed_at DESC
            """,
            params,
        ).fetchall()

    grouped: dict[str, list[dict]] = {key: [] for key in keys}
    for row in rows:
        payload = row.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        obs_key = str(row.get("observation_key") or "").strip()
        if not obs_key:
            continue
        grouped.setdefault(obs_key, []).append(
            {
                "observation_key": obs_key,
                "event_id": row.get("event_id"),
                "topic": row.get("topic"),
                "article_url": row.get("article_url"),
                "rule": row.get("rule"),
                "entity_overlap": int(row.get("entity_overlap") or 0),
                "anchor_overlap": int(row.get("anchor_overlap") or 0),
                "keyword_overlap": int(row.get("keyword_overlap") or 0),
                "time_gap_hours": row.get("time_gap_hours"),
                "final_score": float(row.get("final_score") or 0.0),
                "payload": payload or {},
                "computed_at": row.get("computed_at"),
            }
        )
    return grouped


def load_framing_signals_for_article_urls(article_urls: list[str]) -> dict[str, dict]:
    if not article_urls:
        return {}
    placeholders = ", ".join(["%s"] * len(article_urls))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM article_framing_signals
            WHERE article_url IN ({placeholders})
            ORDER BY analyzed_at DESC
            """,
            list(article_urls),
        ).fetchall()
    result: dict[str, dict] = {}
    for row in rows:
        url = row.get("article_url")
        if url in result:
            continue
        frame_counts = row.get("frame_counts")
        matched_terms = row.get("matched_terms")
        payload = row.get("payload")
        if isinstance(frame_counts, str):
            frame_counts = json.loads(frame_counts) if frame_counts else {}
        if isinstance(matched_terms, str):
            matched_terms = json.loads(matched_terms) if matched_terms else {}
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        result[url] = {
            "article_url": url,
            "subject_key": row.get("subject_key"),
            "dominant_frame": row.get("dominant_frame"),
            "frame_counts": frame_counts or {},
            "matched_terms": matched_terms or {},
            "source": row.get("source"),
            "published_at": row.get("published_at"),
            "analyzed_at": row.get("analyzed_at"),
            "payload": payload or {},
        }
    return result


def load_claim_resolution_for_event_key(event_key: str) -> list[dict]:
    if not event_key:
        return []
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM claim_resolution_records
            WHERE event_key = %s
            ORDER BY generated_at DESC
            """,
            (event_key,),
        ).fetchall()
    out = []
    for row in rows:
        payload = row.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        out.append(
            {
                "claim_record_key": row.get("claim_record_key"),
                "event_key": row.get("event_key"),
                "source_name": row.get("source_name"),
                "claim_text": row.get("claim_text"),
                "claim_type": row.get("conflict_type"),
                "resolution_status": row.get("resolution_status"),
                "confidence": row.get("confidence"),
                "published_at": row.get("published_at"),
                "payload": payload or {},
            }
        )
    return out


def save_contradiction_record(
    event_key: str, event: dict, contradictions: list[dict]
) -> None:
    now = time.time()
    article_urls = [
        article.get("url")
        for article in event.get("articles", [])
        if article.get("url")
    ]
    serialized_article_urls = json.dumps(article_urls, sort_keys=True)
    serialized_contradictions = json.dumps(contradictions, sort_keys=True)
    content_hash = hashlib.sha256(
        " | ".join(
            [
                event_key,
                event.get("event_id", ""),
                event.get("label", ""),
                event.get("latest_update", "") or "",
                serialized_article_urls,
                serialized_contradictions,
            ]
        ).encode("utf-8")
    ).hexdigest()
    with _connect() as conn:
        existing = conn.execute(
            "SELECT content_hash FROM contradiction_history WHERE event_key = %s ORDER BY generated_at DESC LIMIT 1",
            (event_key,),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO contradiction_records (
                event_key, topic, event_label, latest_update, article_urls, contradictions, contradiction_count, generated_at
            ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
            ON CONFLICT (event_key) DO UPDATE SET
                topic = EXCLUDED.topic,
                event_label = EXCLUDED.event_label,
                latest_update = EXCLUDED.latest_update,
                article_urls = EXCLUDED.article_urls,
                contradictions = EXCLUDED.contradictions,
                contradiction_count = EXCLUDED.contradiction_count,
                generated_at = EXCLUDED.generated_at
            """,
            (
                event_key,
                event.get("topic"),
                event.get("label", "Emerging event"),
                event.get("latest_update"),
                serialized_article_urls,
                serialized_contradictions,
                len(contradictions),
                now,
            ),
        )
        if existing is None or existing.get("content_hash") != content_hash:
            conn.execute(
                """
                INSERT INTO contradiction_history (
                    event_key, topic, event_label, latest_update, article_urls, contradictions,
                    contradiction_count, generated_at, content_hash
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
                """,
                (
                    event_key,
                    event.get("topic"),
                    event.get("label", "Emerging event"),
                    event.get("latest_update"),
                    serialized_article_urls,
                    serialized_contradictions,
                    len(contradictions),
                    now,
                    content_hash,
                ),
            )


def get_ingestion_summary() -> dict:
    from db.articles_repo import get_article_count

    with _connect() as conn:
        topic_rows = conn.execute("""
            SELECT DISTINCT ON (topic) topic, provider, article_count, completed_at, status, error
            FROM ingestion_runs
            ORDER BY topic, id DESC
            """).fetchall()
    topics = {}
    for row in topic_rows:
        topics[row["topic"]] = {
            "provider": row["provider"],
            "article_count": row["article_count"],
            "completed_at": row["completed_at"],
            "status": row["status"],
            "error": row["error"],
        }

    return {
        "total_articles": get_article_count(),
        "articles_last_24h": get_article_count(hours=24),
        "latest_published_at": None,
        "topics": topics,
    }


def load_ingestion_state(state_key: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM ingestion_state
            WHERE state_key = %s
            """,
            (state_key,),
        ).fetchone()
    if not row:
        return None
    payload = row.get("payload")
    if isinstance(payload, str):
        payload = json.loads(payload) if payload else {}
    return {
        "state_key": row.get("state_key"),
        "topic": row.get("topic"),
        "provider": row.get("provider"),
        "cursor_start": row.get("cursor_start"),
        "cursor_end": row.get("cursor_end"),
        "status": row.get("status"),
        "error": row.get("error"),
        "updated_at": row.get("updated_at"),
        "payload": payload or {},
    }


def save_ingestion_state(
    state_key: str,
    topic: str,
    provider: str,
    cursor_start: str | None,
    cursor_end: str | None,
    status: str,
    error: str | None = None,
    payload: dict | None = None,
) -> None:
    now = time.time()
    serialized_payload = json.dumps(payload or {}, sort_keys=True)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ingestion_state (
                state_key, topic, provider, cursor_start, cursor_end, status, error, updated_at, payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (state_key) DO UPDATE SET
                topic = EXCLUDED.topic,
                provider = EXCLUDED.provider,
                cursor_start = EXCLUDED.cursor_start,
                cursor_end = EXCLUDED.cursor_end,
                status = EXCLUDED.status,
                error = EXCLUDED.error,
                updated_at = EXCLUDED.updated_at,
                payload = EXCLUDED.payload
            """,
            (
                state_key,
                topic,
                provider,
                cursor_start,
                cursor_end,
                status,
                error,
                now,
                serialized_payload,
            ),
        )


def record_ingestion_run(
    topic: str,
    provider: str,
    article_count: int | list | tuple,
    started_at: float,
    status: str,
    error: str | None = None,
) -> None:
    """Record an ingestion run into the `ingestion_runs` table.

    Accepts either an integer count or an iterable (list/tuple) for
    `article_count` and writes a single row with `started_at` and the
    current time as `completed_at`.
    """
    try:
        if isinstance(article_count, (list, tuple)):
            count = len(article_count)
        else:
            count = int(article_count or 0)
    except Exception:
        count = 0

    completed_at = time.time()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO ingestion_runs (
                topic, provider, article_count, started_at, completed_at, status, error
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (topic, provider, count, started_at, completed_at, status, error),
        )
