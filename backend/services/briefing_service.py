"""Briefing building logic extracted from main.py."""

import os
import time

from fastapi import HTTPException

from cache import load_briefing, save_briefing
from clustering import cluster_articles
from contradictions import (
    enrich_events,
    format_contradictions_for_briefing,
    format_event_brief,
)
from corpus import (
    delete_prediction_records,
    get_article_count,
    get_recent_articles,
    upsert_prediction_records,
)
from entities import format_signals_for_briefing
from foresight import extract_predictions_from_briefing

from core.config import (
    BRIEFING_TOPICS,
    BRIEFING_TTL,
    CORPUS_WINDOW_HOURS,
    REQUEST_ENABLE_LLM_RESPONSES,
    TOPICS,
    corpus_topic_for_briefing,
)

# ---------------------------------------------------------------------------
# Briefing fallback (moved from main.py)
# ---------------------------------------------------------------------------


def _briefing_fallback(
    topic: str,
    articles: list[dict],
    events: list[dict],
    signals: str = "",
    contradictions: str = "",
) -> dict:
    top_event_lines = (
        "\n".join(f"- {event['label']}: {event['summary']}" for event in events[:4])
        or "- No major clustered events yet."
    )
    actor_lines = (
        "\n".join(
            f"- {entity}"
            for entity in sorted(
                {
                    entity
                    for event in events[:4]
                    for entity in event.get("entity_focus", [])
                }
            )[:6]
        )
        or "- No dominant actors extracted yet."
    )
    watch_lines = (
        "\n".join(
            f"- {event['label']} ({event.get('source_count', 0)} sources)"
            for event in events[:4]
        )
        or "- Awaiting more reporting."
    )
    contradiction_block = (
        contradictions
        or "No significant contradictions detected in current clustered coverage."
    )

    situation_summary = f"""SITUATION REPORT:
This briefing is generated from the stored Othello corpus for {topic}. The system is operating in deterministic mode because LLM generation is unavailable.

KEY DEVELOPMENTS:
{top_event_lines}

CRITICAL ACTORS:
{actor_lines}

SIGNAL vs NOISE:
{signals or "- Entity spike data is still forming."}

PREDICTIONS:
- Monitor whether the highest-ranked events retain source diversity over the next reporting cycle.
- Expect briefing quality to improve as the corpus expands and article clusters deepen.

DEEPER CONTEXT:
- Articles included in this briefing: {len(articles)}
- Event clusters analyzed: {len(events)}

WHAT TO WATCH:
{watch_lines}

SOURCE CONTRADICTIONS:
{contradiction_block}
"""

    return {
        "topic": topic,
        "key_developments": [],
        "critical_actors": [],
        "sources": [],
        "event_summary": [],
        "situation_summary": situation_summary,
        "signal_vs_noise": "",
        "llm_enriched": False,
    }


# ---------------------------------------------------------------------------
# Core briefing builder (moved from main.py)
# ---------------------------------------------------------------------------


def build_topic_briefing(topic: str, force_refresh: bool = False) -> dict | None:
    if topic not in BRIEFING_TOPICS:
        raise HTTPException(
            status_code=400, detail=f"Topic must be one of {BRIEFING_TOPICS}"
        )

    corpus_topic = corpus_topic_for_briefing(topic)

    if not force_refresh:
        cached = load_briefing(topic, ttl=BRIEFING_TTL)
        if cached:
            return cached

    from services.ingest_service import (
        _ensure_topic_corpus,
        ensure_article_translations,
    )

    _ensure_topic_corpus(corpus_topic)
    articles = ensure_article_translations(
        get_recent_articles(
            topic=corpus_topic,
            limit=72,
            hours=CORPUS_WINDOW_HOURS,
            headline_corpus_only=True,
        ),
        max_articles=12,
    )
    if not articles:
        return None

    events = enrich_events(cluster_articles(articles, topic=corpus_topic))
    signals = format_signals_for_briefing(corpus_topic)
    event_brief = format_event_brief(events)
    contradictions = format_contradictions_for_briefing(events)

    if REQUEST_ENABLE_LLM_RESPONSES and os.getenv("GROQ_API_KEY"):
        try:
            from analyst import generate_briefing

            briefing = generate_briefing(
                articles,
                topic=topic,
            )
        except Exception as exc:
            print(
                f"[briefing] LLM generation failed for '{topic}', using fallback: {exc}"
            )
            briefing = _briefing_fallback(
                topic, articles, events, signals=signals, contradictions=contradictions
            )
    else:
        briefing = _briefing_fallback(
            topic, articles, events, signals=signals, contradictions=contradictions
        )

    save_briefing(topic, briefing, articles, len(articles), events=events)
    cached = load_briefing(topic, ttl=BRIEFING_TTL)
    generated_at = cached["generated_at"] if cached else time.time()
    predictions = extract_predictions_from_briefing(
        topic=topic,
        briefing_text=briefing.get("situation_summary", "") + " " + briefing.get("signal_vs_noise", ""),
        source_ref=f"{topic}:{int(generated_at)}",
        generated_at=generated_at,
        events=events,
    )
    if predictions:
        delete_prediction_records(
            topic=topic, source_ref=f"{topic}:{int(generated_at)}"
        )
        upsert_prediction_records(predictions)
    return cached or load_briefing(topic, ttl=BRIEFING_TTL)


def refresh_snapshot_layer():
    from services.ingest_service import ingest_all_topics
    from services.headlines_service import rebuild_headlines_cache

    ingest_all_topics()
    rebuild_headlines_cache(use_llm=True)
    for topic in TOPICS:
        build_topic_briefing(topic, force_refresh=True)


# ---------------------------------------------------------------------------
# Public service endpoints
# ---------------------------------------------------------------------------


def get_briefing_payload(topic: str):
    result = build_topic_briefing(topic)
    if not result:
        raise HTTPException(
            status_code=503, detail="No article corpus available for this topic yet"
        )
    return {**result, "cached": True}


def cache_status_payload():
    result = {}
    for topic in BRIEFING_TOPICS:
        cached = load_briefing(topic, ttl=BRIEFING_TTL)
        ct = corpus_topic_for_briefing(topic)
        result[topic] = {
            "cached": cached is not None,
            "age_minutes": (
                int((time.time() - cached["generated_at"]) / 60) if cached else None
            ),
            "article_count": cached["article_count"] if cached else 0,
            "event_count": len(cached.get("events", [])) if cached else 0,
            "corpus_articles_72h": get_article_count(topic=ct, hours=72),
        }
    return result


def force_refresh_payload(topic: str | None = None):
    from services.headlines_service import rebuild_headlines_cache

    if topic:
        if topic not in BRIEFING_TOPICS:
            raise HTTPException(
                status_code=400, detail=f"Topic must be one of {BRIEFING_TOPICS}"
            )
        result = build_topic_briefing(topic, force_refresh=True)
        rebuild_headlines_cache()
        return {"refreshed": [topic], "success": result is not None}
    refresh_snapshot_layer()
    return {"refreshed": TOPICS, "success": True}


def get_predictions_payload(
    topic: str | None = None, refresh: bool = False, limit: int = 100
):
    from foresight import load_prediction_ledger

    return load_prediction_ledger(
        topic=topic, refresh=refresh, limit=max(1, min(limit, 300))
    )


def get_before_news_archive_payload(limit: int = 50, minimum_gap_hours: int = 0):
    from foresight import load_early_signal_archive

    return load_early_signal_archive(
        limit=max(1, min(limit, 200)), minimum_gap_hours=max(0, minimum_gap_hours)
    )
