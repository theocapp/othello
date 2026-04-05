"""Persist cross-source story clusters for analytics and APIs."""

from __future__ import annotations

from datetime import datetime, timedelta

from contradictions import cluster_articles, enrich_events, event_cluster_key
from corpus import get_recent_articles, list_structured_event_ids_in_date_range, replace_materialized_story_clusters

DEFAULT_TOPICS = ("geopolitics", "economics")


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


def _link_structured_ids(event: dict) -> list[str]:
    start, end = _date_range_for_cluster(event)
    if not start or not end:
        return []
    return list_structured_event_ids_in_date_range(start, end, limit=80)


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
        rows = []
        for event in events:
            linked = _link_structured_ids(event)
            urls = sorted(
                {(a.get("url") or "").strip() for a in event.get("articles", []) if (a.get("url") or "").strip()}
            )
            rows.append(
                {
                    "cluster_key": event_cluster_key(event),
                    "label": event.get("label") or "",
                    "summary": event.get("summary"),
                    "earliest_published_at": event.get("earliest_update"),
                    "latest_published_at": event.get("latest_update"),
                    "article_urls": urls,
                    "linked_structured_event_ids": linked,
                    "event_payload": event,
                }
            )
        total_rows += replace_materialized_story_clusters(topic=topic, window_hours=window_hours, rows=rows)
        detail.append({"topic": topic, "clusters": len(rows)})
    return {"topics": topic_list, "window_hours": window_hours, "rows_written": total_rows, "detail": detail}
