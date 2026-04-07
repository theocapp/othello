"""Headline building and sorting logic extracted from main.py."""

import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException

from cache import load_headlines, save_headlines
from contradictions import cluster_articles, enrich_events
from corpus import get_article_count, get_recent_articles
from entities import format_signals_for_briefing
from news import (
    article_quality_score,
    infer_article_topics,
    normalize_article_description,
    normalize_article_title,
    should_promote_article,
)

from core.config import (
    CORPUS_WINDOW_HOURS,
    HEADLINES_TTL,
    MIN_TOPIC_ARTICLES,
    TOPICS,
)
from core.runtime import parse_timestamp


# ---------------------------------------------------------------------------
# Internal helpers (moved from main.py)
# ---------------------------------------------------------------------------


def _event_rank_score(event: dict) -> float:
    base = float(event.get("analysis_priority", 0) or 0)
    source_count = int(event.get("source_count", 0) or 0)
    article_count = int(event.get("article_count", 0) or 0)
    tier_1_source_count = int(event.get("tier_1_source_count", 0) or 0)
    latest = parse_timestamp(event.get("latest_update"))
    coverage_score = (source_count * 16.0) + (article_count * 4.5) + (tier_1_source_count * 3.0)
    if not latest:
        return round(coverage_score + (base * 0.35), 2)
    age_hours = max(0.0, (datetime.now(timezone.utc) - latest).total_seconds() / 3600)
    if age_hours <= 6:
        freshness_multiplier = 1.9
        freshness_bonus = 28.0
    elif age_hours <= 12:
        freshness_multiplier = 1.7
        freshness_bonus = 22.0
    elif age_hours <= 24:
        freshness_multiplier = 1.5
        freshness_bonus = 15.0
    elif age_hours <= 36:
        freshness_multiplier = 1.25
        freshness_bonus = 9.0
    elif age_hours <= 48:
        freshness_multiplier = 1.12
        freshness_bonus = 3.0
    else:
        freshness_multiplier = 1.0
        freshness_bonus = max(0.0, 72.0 - age_hours) / 18.0
    return round((coverage_score * freshness_multiplier) + freshness_bonus + (base * 0.35), 2)


def _story_summary_candidate_score(summary: str, headline: str) -> int:
    if not summary:
        return -10_000
    score = min(len(summary), 220)
    if len(summary) < 28:
        score -= 120
    if summary.lower() == headline.lower():
        score -= 400
    if len(summary) > 210:
        score -= len(summary) - 210
    if any(marker in summary for marker in (". ", "! ", "? ", "; ")):
        score += 16
    return score


def _standardize_story_summary(story: dict, event: dict | None = None) -> str:
    headline = normalize_article_title((event or {}).get("label") or story.get("headline") or "")
    candidates = [(event or {}).get("summary")]
    articles = list(story.get("sources") or [])
    if event:
        articles.extend(event.get("articles", []) or [])
    for article in articles:
        candidates.extend([
            article.get("translated_description"),
            article.get("description"),
            article.get("original_description"),
        ])
    candidates.append(story.get("summary"))

    best_summary = ""
    best_score = -10_000
    for candidate in candidates:
        normalized = normalize_article_description(candidate, headline, limit=200)
        score = _story_summary_candidate_score(normalized, headline)
        if score > best_score:
            best_summary = normalized
            best_score = score

    if best_score > -50 and best_summary:
        return best_summary

    source_count = int(story.get("source_count") or (event or {}).get("source_count") or 0)
    if source_count > 1:
        return f"{source_count} sources are tracking the latest turn in this story."
    topic = (story.get("topic") or (event or {}).get("topic") or "").replace("_", " ").strip()
    if topic:
        return f"Fresh {topic} reporting is still developing."
    return "Fresh reporting is still developing."


def _standardize_headline_story(story: dict, event: dict | None = None) -> dict:
    headline = normalize_article_title((event or {}).get("label") or story.get("headline") or "Untitled")
    return {
        **story,
        "headline": headline or "Untitled",
        "summary": _standardize_story_summary({**story, "headline": headline}, event=event),
    }


def _story_region_counts(story: dict) -> dict[str, int]:
    counts = defaultdict(int)
    for article in story.get("sources", []) or []:
        region = (
            ((article.get("source_profile") or {}).get("region"))
            or article.get("source_region")
            or "global"
        )
        normalized = str(region).strip().lower() or "global"
        counts[normalized] += 1
    if story.get("region_counts"):
        for region, count in (story.get("region_counts") or {}).items():
            normalized = str(region).strip().lower() or "global"
            counts[normalized] = max(counts.get(normalized, 0), int(count or 0))
    return dict(counts)


def _story_dominant_region(story: dict) -> str:
    counts = _story_region_counts(story)
    if not counts:
        return "global"
    non_global = {region: count for region, count in counts.items() if region and region != "global"}
    pool = non_global or counts
    return sorted(pool.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _story_rank_score(story: dict) -> float:
    latest = parse_timestamp(story.get("latest_update") or "")
    source_count = int(story.get("source_count", 0) or 0)
    article_count = int(story.get("article_count", source_count) or 0)
    contradiction_count = int(story.get("contradiction_count", 0) or 0)
    ranking_score = float(story.get("ranking_score", 0) or 0)
    age_hours = max(
        0.0,
        (datetime.now(timezone.utc) - latest).total_seconds() / 3600,
    ) if latest else 240.0

    if age_hours <= 6:
        freshness = 36.0
    elif age_hours <= 12:
        freshness = 28.0
    elif age_hours <= 24:
        freshness = 18.0
    elif age_hours <= 48:
        freshness = 9.0
    else:
        freshness = max(0.0, 96.0 - age_hours) / 12.0

    return round(
        freshness
        + (source_count * 18.0)
        + (article_count * 4.0)
        + (contradiction_count * 1.5)
        + (ranking_score * 0.25),
        2,
    )


def _sort_headline_stories(stories: list[dict], sort_by: str = "relevance", region: str | None = None) -> list[dict]:
    normalized_region = (region or "").strip().lower()
    selected = [
        {
            **story,
            "region_counts": _story_region_counts(story),
            "dominant_region": (story.get("dominant_region") or _story_dominant_region(story)).strip().lower(),
        }
        for story in stories
    ]
    if normalized_region and normalized_region not in {"all", "global-overview"}:
        selected = [story for story in selected if story.get("dominant_region") == normalized_region]

    if sort_by == "region":
        selected.sort(
            key=lambda story: (
                story.get("dominant_region") in {"", "global"},
                story.get("dominant_region") or "global",
                -_story_rank_score(story),
                story.get("headline", ""),
            )
        )
        return selected

    selected.sort(
        key=lambda story: (
            -_story_rank_score(story),
            -(int(story.get("source_count", 0) or 0)),
            -(int(story.get("article_count", 0) or 0)),
            story.get("latest_update") or "",
        )
    )
    return selected


def _available_story_regions(stories: list[dict]) -> list[str]:
    regions = {
        (story.get("dominant_region") or _story_dominant_region(story)).strip().lower()
        for story in stories
        if (story.get("dominant_region") or _story_dominant_region(story)).strip()
    }
    return sorted(region for region in regions if region and region != "global")


# ---------------------------------------------------------------------------
# Event building helpers (used by rebuild_headlines_cache)
# ---------------------------------------------------------------------------


def _ensure_topic_corpus(topic: str, minimum_articles: int = MIN_TOPIC_ARTICLES) -> None:
    if get_article_count(topic=topic, hours=72) >= minimum_articles:
        return
    from services.ingest_service import ingest_topic
    ingest_topic(topic)


def _build_topic_events(topic: str, limit: int = 8, attempt_ingest: bool = False) -> list[dict]:
    if attempt_ingest:
        _ensure_topic_corpus(topic)
    from services.ingest_service import ensure_article_translations
    articles = ensure_article_translations(
        get_recent_articles(topic=topic, limit=120, hours=CORPUS_WINDOW_HOURS, headline_corpus_only=True),
        max_articles=10,
    )
    if not articles:
        return []
    filtered_articles = []
    for article in articles:
        inferred_topics = infer_article_topics(article)
        primary_topic = inferred_topics[0] if inferred_topics else None
        if primary_topic and primary_topic != topic:
            continue
        filtered_articles.append(article)

    event_articles = filtered_articles or articles
    events = enrich_events(cluster_articles(event_articles, topic=topic))
    return events[:limit]


def _event_article_urls(event: dict) -> set[str]:
    return {
        (article.get("url") or "").strip()
        for article in event.get("articles", [])
        if (article.get("url") or "").strip()
    }


def _events_materially_overlap(left: dict, right: dict) -> bool:
    left_urls = _event_article_urls(left)
    right_urls = _event_article_urls(right)
    if not left_urls or not right_urls:
        return False
    overlap = left_urls & right_urls
    if not overlap:
        return False
    smaller_cluster = min(len(left_urls), len(right_urls))
    if smaller_cluster <= 1:
        return True
    if (left.get("label") or "").strip() == (right.get("label") or "").strip():
        return True
    return len(overlap) >= max(2, math.ceil(smaller_cluster * 0.5))


def _dedupe_global_events(events: list[dict]) -> list[dict]:
    if len(events) <= 1:
        return events
    sort_key = lambda event: (_event_rank_score(event), event.get("latest_update", ""))
    ranked = sorted(events, key=sort_key, reverse=True)
    selected: list[dict] = []
    for event in ranked:
        if any(_events_materially_overlap(event, existing) for existing in selected):
            continue
        selected.append(event)
    return selected


def _build_global_events(limit: int = 12) -> list[dict]:
    from core.runtime import topic_counts
    from foresight import observe_events
    counts = topic_counts()
    if sum(counts.values()) == 0:
        from services.ingest_service import ingest_all_topics
        ingest_all_topics()
    events = []
    for topic in TOPICS:
        if counts.get(topic, 0) == 0:
            continue
        events.extend(_build_topic_events(topic, limit=8, attempt_ingest=False))
    events = _dedupe_global_events(events)
    recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_events = []
    older_events = []
    for event in events:
        latest = parse_timestamp(event.get("latest_update"))
        if latest and latest >= recent_cutoff:
            recent_events.append(event)
        else:
            older_events.append(event)

    sort_key = lambda event: (_event_rank_score(event), event.get("latest_update", ""))
    recent_events.sort(key=sort_key, reverse=True)
    older_events.sort(key=sort_key, reverse=True)

    observed = (recent_events + older_events)[:limit]
    observe_events(observed)
    return observed


# ---------------------------------------------------------------------------
# Headline rebuild
# ---------------------------------------------------------------------------


def _fallback_headlines(events: list[dict]) -> list[dict]:
    stories = []
    for event in events[:7]:
        story = {
            "event_id": event["event_id"],
            "headline": event["label"],
            "summary": event["summary"],
            "topic": event.get("topic"),
            "why_signal": (
                f"{event.get('source_count', 0)} sources, "
                f"{event.get('article_count', 0)} reports, "
                f"{event.get('contradiction_count', 0)} contradiction flags."
            ),
            "entity_focus": event.get("entity_focus", []),
            "source_count": event.get("source_count", 0),
            "article_count": event.get("article_count", 0),
            "contradiction_count": event.get("contradiction_count", 0),
            "dominant_region": event.get("dominant_region"),
            "region_counts": event.get("region_counts", {}),
            "ranking_score": _event_rank_score(event),
            "sources": event.get("articles", []),
        }
        stories.append(_standardize_headline_story(story, event=event))
    return stories


def rebuild_headlines_cache(use_llm: bool = False) -> list[dict]:
    from analyst import build_headlines_from_events
    from cache import acquire_lock, release_lock
    import time as time_module
    
    # Acquire distributed lock to prevent thundering herd on cache rebuild
    lock_acquired = acquire_lock("headlines_cache_rebuild", lock_holder_id=f"rebuild_{int(time_module.time())}")
    if not lock_acquired:
        # Another process is rebuilding, wait and return stale cache if available
        print("[headlines] Another process is rebuilding cache, returning cached data if available")
        return load_headlines(ttl=0) or []  # Return any cached data, even if stale

    try:
        events = _build_global_events(limit=8)
        if not events:
            return []

        fallback_stories = _fallback_headlines(events)
        if use_llm and os.getenv("GROQ_API_KEY"):
            try:
                stories = build_headlines_from_events(events)
            except Exception as exc:
                print(f"[headlines] LLM headline build failed, using fallback: {exc}")
                stories = fallback_stories
        else:
            stories = fallback_stories

        event_map = {event["event_id"]: event for event in events}
        fallback_map = {story["event_id"]: story for story in fallback_stories if story.get("event_id")}
        enriched = []
        seen_event_ids = set()
        for story in stories:
            event = event_map.get(story.get("event_id"))
            if not event:
                continue
            seen_event_ids.add(event["event_id"])
            enriched_story = {
                **fallback_map.get(event["event_id"], {}),
                **story,
                "topic": story.get("topic") or event.get("topic"),
                "entity_focus": event.get("entity_focus", []),
                "source_count": event.get("source_count", 0),
                "article_count": event.get("article_count", 0),
                "contradiction_count": event.get("contradiction_count", 0),
                "latest_update": event.get("latest_update"),
                "dominant_region": event.get("dominant_region"),
                "region_counts": event.get("region_counts", {}),
                "ranking_score": _event_rank_score(event),
                "sources": event.get("articles", []),
            }
            enriched.append(_standardize_headline_story(enriched_story, event=event))

        for event in events:
            if event["event_id"] in seen_event_ids:
                continue
            fallback_story = fallback_map.get(event["event_id"])
            if fallback_story:
                enriched.append(_standardize_headline_story(fallback_story, event=event))

        sorted_stories = _sort_headline_stories(enriched, sort_by="relevance")
        save_headlines(sorted_stories)
        return sorted_stories
    finally:
        release_lock("headlines_cache_rebuild")


# ---------------------------------------------------------------------------
# Public service endpoints
# ---------------------------------------------------------------------------


def get_headlines_payload(sort_by: str = "relevance", region: str | None = None):
    cached = load_headlines(ttl=HEADLINES_TTL)
    if cached:
        normalized = [_standardize_headline_story(story) for story in cached]
        return {
            "stories": _sort_headline_stories(normalized, sort_by=sort_by, region=region),
            "available_regions": _available_story_regions(normalized),
            "sort_by": sort_by,
            "region": region or "all",
        }

    stories = rebuild_headlines_cache(use_llm=False)
    if not stories:
        raise HTTPException(status_code=503, detail="No article corpus available yet")
    return {
        "stories": _sort_headline_stories(stories, sort_by=sort_by, region=region),
        "available_regions": _available_story_regions(stories),
        "sort_by": sort_by,
        "region": region or "all",
    }
