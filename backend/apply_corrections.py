"""Apply analyst corrections queue against canonical events."""

from __future__ import annotations

import json
import time

from db.common import _connect


def apply_corrections() -> dict:
    """Apply merge and split corrections that have not yet been processed."""
    processed = 0
    merged_count = 0
    split_count = 0

    with _connect() as conn:
        # Fetch all unapplied corrections ordered by creation time
        corrections = conn.execute(
            """
            SELECT id, correction_type, event_a_id, event_b_id, article_url, created_at
            FROM analyst_corrections
            WHERE applied = FALSE
            ORDER BY created_at ASC
            """
        ).fetchall()

    if not corrections:
        return {"processed": 0, "merged": 0, "split": 0}

    for correction in corrections:
        correction_id = correction["id"]
        correction_type = correction["correction_type"]
        event_a_id = correction["event_a_id"]

        try:
            if correction_type == "merge":
                event_b_id = correction["event_b_id"]
                merged_count += _process_merge(event_a_id, event_b_id)
            elif correction_type in {"split", "split-article"}:
                article_url = correction["article_url"]
                split_count += _process_split(event_a_id, article_url)

            with _connect() as conn:
                conn.execute(
                    "UPDATE analyst_corrections SET applied = TRUE WHERE id = %s",
                    (correction_id,),
                )
            processed += 1
        except Exception as exc:
            print(f"Error processing correction {correction_id}: {exc}")

    return {"processed": processed, "merged": merged_count, "split": split_count}


def _load_event(conn, event_id: str) -> dict | None:
    row = conn.execute(
        """
        SELECT event_id, article_urls, article_count, source_count,
               resolved_title, resolved_summary, label, status,
               importance_score, linked_structured_event_ids
        FROM canonical_events
        WHERE event_id = %s
        """,
        (event_id,),
    ).fetchone()
    if not row:
        return None
    article_urls = row.get("article_urls")
    structured_ids = row.get("linked_structured_event_ids")
    if isinstance(article_urls, str):
        article_urls = json.loads(article_urls) if article_urls else []
    if isinstance(structured_ids, str):
        structured_ids = json.loads(structured_ids) if structured_ids else []
    return {
        "event_id": row.get("event_id"),
        "article_urls": article_urls or [],
        "article_count": int(row.get("article_count") or 0),
        "source_count": int(row.get("source_count") or 0),
        "resolved_title": row.get("resolved_title"),
        "resolved_summary": row.get("resolved_summary"),
        "label": row.get("label"),
        "status": row.get("status"),
        "importance_score": float(row.get("importance_score") or 0.0),
        "linked_structured_event_ids": structured_ids or [],
    }


def _best_title(primary: dict, secondary: dict) -> str | None:
    first = (primary.get("resolved_title") or primary.get("label") or "").strip()
    second = (secondary.get("resolved_title") or secondary.get("label") or "").strip()
    if not first:
        return second or None
    if not second:
        return first
    first_score = float(primary.get("importance_score") or 0.0)
    second_score = float(secondary.get("importance_score") or 0.0)
    if second_score > first_score:
        return second
    if first_score > second_score:
        return first
    return first if len(first) >= len(second) else second


def _process_merge(event_a_id: str, event_b_id: str) -> int:
    if not event_a_id or not event_b_id or event_a_id == event_b_id:
        return 0

    with _connect() as conn:
        event_a = _load_event(conn, event_a_id)
        event_b = _load_event(conn, event_b_id)
        if not event_a or not event_b:
            print(f"Cannot merge: event_a={event_a_id} or event_b={event_b_id} not found")
            return 0

        merged_urls = sorted(
            {
                str(url).strip()
                for url in [*event_a["article_urls"], *event_b["article_urls"]]
                if str(url).strip()
            }
        )
        merged_structured_ids = sorted(
            {
                str(sid).strip()
                for sid in [
                    *event_a.get("linked_structured_event_ids", []),
                    *event_b.get("linked_structured_event_ids", []),
                ]
                if str(sid).strip()
            }
        )
        preferred_title = _best_title(event_a, event_b)
        preferred_summary = (
            (event_a.get("resolved_summary") or "").strip()
            or (event_b.get("resolved_summary") or "").strip()
            or None
        )
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        conn.execute(
            """
            UPDATE canonical_events
            SET article_urls = %s::jsonb,
                article_count = %s,
                source_count = %s,
                linked_structured_event_ids = %s::jsonb,
                resolved_title = COALESCE(%s, resolved_title),
                resolved_summary = COALESCE(%s, resolved_summary),
                label = COALESCE(%s, label),
                last_updated_at = %s
            WHERE event_id = %s
            """,
            (
                json.dumps(merged_urls, sort_keys=True),
                len(merged_urls),
                int(event_a.get("source_count") or 0) + int(event_b.get("source_count") or 0),
                json.dumps(merged_structured_ids, sort_keys=True),
                preferred_title,
                preferred_summary,
                preferred_title,
                now_iso,
                event_a_id,
            ),
        )

        conn.execute(
            "UPDATE event_perspectives SET event_id = %s WHERE event_id = %s",
            (event_a_id, event_b_id),
        )
        conn.execute(
            "UPDATE event_identity_map SET event_id = %s, last_seen_at = %s WHERE event_id = %s",
            (event_a_id, time.time(), event_b_id),
        )
        conn.execute(
            "UPDATE canonical_events SET status = 'superseded', last_updated_at = %s WHERE event_id = %s",
            (now_iso, event_b_id),
        )
    return 1


def _process_split(event_a_id: str, article_url: str) -> int:
    event_id = (event_a_id or "").strip()
    target_url = (article_url or "").strip()
    if not event_id or not target_url:
        return 0

    with _connect() as conn:
        event_a = _load_event(conn, event_id)
        if not event_a:
            print(f"Cannot split: event_a={event_id} not found")
            return 0

        urls = [str(url).strip() for url in event_a.get("article_urls") or [] if str(url).strip()]
        if target_url not in urls:
            print(f"Cannot split: article_url={target_url} not in event {event_id}")
            return 0

        remaining = [url for url in urls if url != target_url]
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn.execute(
            """
            UPDATE canonical_events
            SET article_urls = %s::jsonb,
                article_count = %s,
                last_updated_at = %s
            WHERE event_id = %s
            """,
            (json.dumps(remaining, sort_keys=True), len(remaining), now_iso, event_id),
        )
        conn.execute(
            "DELETE FROM event_perspectives WHERE event_id = %s AND article_url = %s",
            (event_id, target_url),
        )

    return 1
