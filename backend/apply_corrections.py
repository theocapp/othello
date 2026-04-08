"""Apply analyst corrections feedback loop.

Processes queued analyst_corrections (merge/split) and applies them to canonical_events.
Called at the end of the clustering/ingestion cycle.
"""

import json
import time
import uuid
from db.common import _connect
from db.events_repo import get_canonical_event, upsert_canonical_events


def apply_corrections() -> dict:
    """
    Read all unapplied analyst_corrections and process them:
    - merge: combine article_urls from two events, delete event_b_id
    - split: remove article from event, create new standalone event
    
    Returns: {"processed": count, "merged": count, "split": count}
    """
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
            elif correction_type == "split":
                article_url = correction["article_url"]
                split_count += _process_split(event_a_id, article_url)

            # Mark as applied
            with _connect() as conn:
                conn.execute(
                    "UPDATE analyst_corrections SET applied = TRUE WHERE id = %s",
                    (correction_id,),
                )
            processed += 1
        except Exception as e:
            # Log error but continue processing
            print(f"Error processing correction {correction_id}: {e}")

    return {"processed": processed, "merged": merged_count, "split": split_count}


def _process_merge(event_a_id: str, event_b_id: str) -> int:
    """
    Merge event_b_id into event_a_id:
    1. Get both events
    2. Combine article_urls (deduplicated)
    3. Update article_count
    4. Update event_a_id
    5. Delete event_b_id
    """
    event_a = get_canonical_event(event_a_id)
    event_b = get_canonical_event(event_b_id)

    if not event_a or not event_b:
        print(f"Cannot merge: event_a={event_a_id} or event_b={event_b_id} not found")
        return 0

    # Combine article_urls (deduplicated)
    urls_a = set(event_a.get("article_urls") or [])
    urls_b = set(event_b.get("article_urls") or [])
    combined_urls = sorted(list(urls_a | urls_b))

    # Update event_a with combined articles
    event_a["article_urls"] = combined_urls
    event_a["article_count"] = len(combined_urls)
    event_a["last_updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    upsert_canonical_events([event_a])

    # Delete event_b
    with _connect() as conn:
        conn.execute(
            "DELETE FROM canonical_events WHERE event_id = %s",
            (event_b_id,),
        )

    return 1


def _process_split(event_a_id: str, article_url: str) -> int:
    """
    Split article_url out of event_a_id:
    1. Get event_a
    2. Remove article_url from its article_urls
    3. Update event_a's article_count
    4. Create new event with just that article
    5. Copy topic, geo_country, geo_region from parent
    """
    event_a = get_canonical_event(event_a_id)

    if not event_a:
        print(f"Cannot split: event_a={event_a_id} not found")
        return 0

    urls = event_a.get("article_urls") or []
    if article_url not in urls:
        print(f"Cannot split: article_url={article_url} not in event {event_a_id}")
        return 0

    # Remove article from event_a
    remaining_urls = [u for u in urls if u != article_url]
    event_a["article_urls"] = remaining_urls
    event_a["article_count"] = len(remaining_urls)
    event_a["last_updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    upsert_canonical_events([event_a])

    # Create new event for the split article
    new_event_id = f"evt_{uuid.uuid4().hex}"
    now = time.time()
    new_event = {
        "event_id": new_event_id,
        "topic": event_a.get("topic") or "global",
        "label": event_a.get("label", ""),
        "event_type": event_a.get("event_type"),
        "status": "developing",
        "geo_country": event_a.get("geo_country"),
        "geo_region": event_a.get("geo_region"),
        "latitude": event_a.get("latitude"),
        "longitude": event_a.get("longitude"),
        "first_reported_at": event_a.get("first_reported_at"),
        "last_updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "article_urls": [article_url],
        "article_count": 1,
        "source_count": 0,
        "perspective_count": 0,
        "contradiction_count": 0,
        "importance_score": 0.0,
        "importance_reasons": [],
        "linked_structured_event_ids": [],
        "first_seen_at": now,
        "computed_at": now,
        "payload": {},
    }

    upsert_canonical_events([new_event])

    return 1
