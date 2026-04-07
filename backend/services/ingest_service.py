import json
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import HTTPException

from analyst import translate_article
from cache import clear_headlines
from chroma import store_articles
from corpus import (
    get_article_count,
    get_articles_missing_translation,
    get_topic_time_bounds,
    load_ingestion_state,
    record_ingestion_run,
    save_article_translation,
    save_ingestion_state,
    upsert_article_summaries,
    upsert_articles,
)
from entities import get_top_entities, store_entity_mentions
from fetch_historical_queue import fetch_historical_queue
from news import (
    article_quality_score,
    fetch_articles,
    fetch_articles_from_provider,
    fetch_gdelt_historic_articles,
    fetch_global_articles,
    fetch_global_articles_from_provider,
    infer_article_topics,
    is_english_article,
    probe_sources,
    should_promote_article,
    source_status,
)
from source_ingestion import (
    archive_provider_articles,
    ingest_direct_feed_layer,
    ingest_registry_sources,
    mirror_corpus_articles_into_registry,
    registry_sources_with_feed_status,
)
from official_ingestion import ingest_official_updates
from acled_ingestion import ingest_acled_recent
from gdelt_gkg_ingestion import ingest_gdelt_gkg_recent
from claim_resolution import build_claim_resolution_snapshot
from narrative_drift import analyze_narrative_drift
from foresight import load_early_signal_archive, load_prediction_ledger
from story_materialization import rebuild_materialized_story_clusters

from core.config import (
    CORPUS_WINDOW_HOURS,
    GDELT_BACKFILL_CHROMA,
    GDELT_BACKFILL_LAG_MINUTES,
    GDELT_BACKFILL_MIN_WINDOW_HOURS,
    GDELT_BACKFILL_PAGE_SIZE,
    GDELT_BACKFILL_RATE_LIMIT_RETRY_MINUTES,
    GDELT_BACKFILL_RETRY_MINUTES,
    GDELT_BACKFILL_START,
    GDELT_BACKFILL_WINDOW_HOURS,
    GDELT_GKG_REFRESH_HOURS,
    HISTORICAL_FETCH_BATCH_LIMIT,
    HISTORICAL_FETCH_DOMAIN_INTERVAL_SECONDS,
    HISTORICAL_FETCH_MAX_ATTEMPTS,
    HISTORICAL_FETCH_WRITE_BATCH_SIZE,
    MIN_TOPIC_ARTICLES,
    NARRATIVE_DRIFT_TOP_SUBJECTS,
    REQUEST_ENABLE_CHROMA_INGEST,
    REQUEST_ENABLE_TRANSLATION,
    SOURCE_REGISTRY_MIRROR_HOURS,
    TOPICS,
    TRANSLATION_MIN_SCORE,
    TRANSLATION_REMOTE_FALLBACK_SCORE,
)
from core.auth import run_exclusive, run_exclusive_or_skip
from core.locks import (
    BACKFILL_JOB_LOCK,
    HISTORICAL_FETCH_JOB_LOCK,
    INGEST_JOB_LOCK,
    STORY_MATERIALIZATION_JOB_LOCK,
)
from core.runtime import parse_timestamp, topic_counts

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _store_articles_safe(articles: list[dict], topic: str) -> None:
    if not REQUEST_ENABLE_CHROMA_INGEST:
        return
    try:
        store_articles(articles, topic)
    except Exception as exc:
        print(f"[chroma] Ingest store failed for '{topic}': {exc}")


def _store_entity_mentions_with_translation(articles: list[dict], topic: str) -> dict:
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            return store_entity_mentions(articles, topic)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == attempts:
                print(f"[entities] Entity extraction skipped for '{topic}': {exc}")
                return {
                    "topic": topic,
                    "articles_processed": len(articles),
                    "mentions_written": 0,
                    "cooccurrences_written": 0,
                    "error": str(exc),
                    "status": "skipped_locked",
                }
            time.sleep(0.5 * attempt)


def _clear_headlines_resilient() -> None:
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            clear_headlines()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == attempts:
                print(f"[cache] Headline cache clear skipped: {exc}")
                return
            time.sleep(0.5 * attempt)


def _entity_payload(entity_stats: dict | None) -> dict:
    if not entity_stats:
        return {}
    return {"entity_extraction": entity_stats}


def _article_translation_priority(article: dict) -> int:
    topics = infer_article_topics(article)
    score = article_quality_score(article, topics)
    published_at = parse_timestamp(article.get("published_at"))
    if published_at:
        age_hours = max(
            0.0,
            (
                datetime.now(timezone.utc) - published_at.astimezone(timezone.utc)
            ).total_seconds()
            / 3600,
        )
        if age_hours <= 24:
            score += 2
        elif age_hours <= 72:
            score += 1
    if article.get("source_domain") in {
        "reuters.com",
        "apnews.com",
        "bbc.com",
        "ft.com",
    }:
        score += 1
    return score


def _needs_translation(
    article: dict, min_priority: int = TRANSLATION_MIN_SCORE
) -> bool:
    return (
        REQUEST_ENABLE_TRANSLATION
        and not is_english_article(article)
        and not article.get("translated_title")
        and _article_translation_priority(article) >= min_priority
    )


def ensure_article_translations(
    articles: list[dict],
    max_articles: int = 6,
    min_priority: int = TRANSLATION_MIN_SCORE,
) -> list[dict]:
    if not REQUEST_ENABLE_TRANSLATION:
        return articles

    candidates = []
    for article in articles:
        if not _needs_translation(article, min_priority=min_priority):
            continue
        candidates.append((_article_translation_priority(article), article))

    translated_count = 0
    for _, article in sorted(candidates, key=lambda item: item[0], reverse=True):
        if translated_count >= max_articles:
            break
        try:
            priority = _article_translation_priority(article)
            translation = translate_article(
                article,
                allow_remote_fallback=priority >= TRANSLATION_REMOTE_FALLBACK_SCORE,
            )
            save_article_translation(
                article_url=article["url"],
                source_language=article.get("language") or "unknown",
                translated_title=translation["translated_title"],
                translated_description=translation.get("translated_description"),
                translation_provider=translation.get("provider", "groq"),
                target_language=translation.get("target_language", "en"),
            )
            article["translated_title"] = translation["translated_title"]
            article["translated_description"] = translation.get(
                "translated_description"
            )
            article["translation_provider"] = translation.get("provider", "translation")
            article["translation_target_language"] = translation.get(
                "target_language", "en"
            )
            article["title"] = translation["translated_title"] or article.get("title")
            article["description"] = translation.get(
                "translated_description"
            ) or article.get("description")
            translated_count += 1
        except Exception as exc:
            print(f"[translation] Failed for {article.get('url')}: {exc}")
    return articles


# ---------------------------------------------------------------------------
# Topic / global ingest
# ---------------------------------------------------------------------------


def ingest_topic(topic: str, page_size: int = 60) -> dict:
    started_at = time.time()
    state_key = f"analytic-ingest-{topic}"
    try:
        articles = fetch_articles(topic, page_size=page_size)
        provider = articles[0].get("provider", "unknown") if articles else "unknown"
        archive_summary = archive_provider_articles(
            articles, provider=provider, topic_hint=topic
        )
        if not articles:
            fallback_provider = "directfeeds"
            fallback_articles = (
                fetch_articles_from_provider(
                    topic, fallback_provider, page_size=max(20, page_size // 2)
                )
                if source_status()["directfeeds"]["enabled"]
                else []
            )
            if fallback_articles:
                articles = fallback_articles
                provider = f"{fallback_provider}-fallback"
                archive_summary = archive_provider_articles(
                    articles, provider=provider, topic_hint=topic
                )
            else:
                message = f"No fresh articles returned for topic '{topic}' from configured providers."
                record_ingestion_run(
                    topic, provider, 0, started_at, "empty", error=message
                )
                save_ingestion_state(
                    state_key,
                    topic,
                    provider,
                    None,
                    None,
                    "empty",
                    error=message,
                    payload={
                        "fetched": 0,
                        "promoted": 0,
                        "archived_documents": archive_summary["documents_written"],
                    },
                )
                print(f"[ingest] {message}")
                return {
                    "topic": topic,
                    "provider": provider,
                    "fetched": 0,
                    "promoted": 0,
                    "inserted_or_updated": 0,
                    "existing_or_unchanged": 0,
                    "status": "empty",
                    "error": message,
                }
        quality_scores = {
            a["url"]: article_quality_score(a, [topic])
            for a in articles
            if a.get("url")
        }
        promoted = [
            article for article in articles if should_promote_article(article, [topic])
        ]
        rejected = [
            article
            for article in articles
            if not should_promote_article(article, [topic])
        ]
        if rejected:
            upsert_article_summaries(
                rejected, topic=topic, quality_scores=quality_scores
            )
        if not promoted:
            message = f"Fetched {len(articles)} articles for '{topic}', but none passed analytic promotion."
            record_ingestion_run(topic, provider, 0, started_at, "empty", error=message)
            save_ingestion_state(
                state_key,
                topic,
                provider,
                None,
                None,
                "empty",
                error=message,
                payload={
                    "fetched": len(articles),
                    "promoted": 0,
                    "rejected": len(articles),
                    "archived_documents": archive_summary["documents_written"],
                },
            )
            return {
                "topic": topic,
                "provider": provider,
                "fetched": len(articles),
                "promoted": 0,
                "archived_documents": archive_summary["documents_written"],
                "inserted_or_updated": 0,
                "tier2_summaries": len(rejected),
                "existing_or_unchanged": 0,
                "status": "empty",
                "error": message,
            }
        write_provider = (
            "newsapi"
            if provider == "newsapi-fallback"
            else ("directfeeds" if provider == "directfeeds-fallback" else provider)
        )
        inserted = upsert_articles(promoted, topic=topic, provider=write_provider)
        existing_or_unchanged = max(len(promoted) - inserted, 0)
        _store_articles_safe(promoted, topic)
        entity_stats = _store_entity_mentions_with_translation(promoted, topic)
        record_ingestion_run(topic, provider, len(promoted), started_at, "ok")
        save_ingestion_state(
            state_key,
            topic,
            provider,
            None,
            None,
            "ok",
            payload={
                "fetched": len(articles),
                "promoted": len(promoted),
                "rejected": max(len(articles) - len(promoted), 0),
                "archived_documents": archive_summary["documents_written"],
                "inserted_or_updated": inserted,
                "existing_or_unchanged": existing_or_unchanged,
                "entity_extraction": entity_stats,
            },
        )
        _clear_headlines_resilient()
        return {
            "topic": topic,
            "provider": provider,
            "fetched": len(articles),
            "promoted": len(promoted),
            "archived_documents": archive_summary["documents_written"],
            "inserted_or_updated": inserted,
            "existing_or_unchanged": existing_or_unchanged,
            "status": "ok",
        }
    except Exception as exc:
        record_ingestion_run(topic, "unknown", 0, started_at, "error", error=str(exc))
        save_ingestion_state(
            state_key, topic, "unknown", None, None, "error", error=str(exc), payload={}
        )
        print(f"[ingest] Topic '{topic}' failed: {exc}")
        return {
            "topic": topic,
            "provider": "unknown",
            "fetched": 0,
            "promoted": 0,
            "inserted_or_updated": 0,
            "existing_or_unchanged": 0,
            "status": "error",
            "error": str(exc),
        }


def ingest_global(page_size: int = 100) -> dict:
    started_at = time.time()
    state_key = "analytic-ingest-global"
    try:
        articles = fetch_global_articles(page_size=page_size)
        provider = articles[0].get("provider", "unknown") if articles else "unknown"
        archive_summary = archive_provider_articles(
            articles, provider=provider, topic_hint="global"
        )
        if not articles and source_status()["directfeeds"]["enabled"]:
            articles = fetch_global_articles_from_provider(
                "directfeeds", page_size=max(30, page_size // 2)
            )
            provider = "directfeeds-fallback" if articles else provider
            archive_summary = (
                archive_provider_articles(
                    articles, provider=provider, topic_hint="global-fallback"
                )
                if articles
                else archive_summary
            )

        if not articles:
            message = (
                "Global ingest returned no fresh articles from configured providers."
            )
            for topic in TOPICS:
                record_ingestion_run(
                    topic, "unknown", 0, started_at, "empty", error=message
                )
            save_ingestion_state(
                state_key,
                "global",
                "unknown",
                None,
                None,
                "empty",
                error=message,
                payload={
                    "fetched": 0,
                    "archived_documents": archive_summary["documents_written"],
                },
            )
            print(f"[ingest] {message}")
            return {
                "provider": "unknown",
                "fetched": 0,
                "classified": {topic: 0 for topic in TOPICS},
                "inserted_or_updated": 0,
                "unclassified": 0,
                "status": "empty",
                "error": message,
            }
        topic_buckets = {topic: [] for topic in TOPICS}
        tier2_articles = []
        unclassified = 0
        rejected = 0

        for article in articles:
            article_topics = infer_article_topics(article)
            if not article_topics:
                unclassified += 1
                tier2_articles.append(article)
                continue
            if not should_promote_article(article, article_topics):
                rejected += 1
                tier2_articles.append((article, article_topics[0]))
                continue
            topic_buckets[article_topics[0]].append(article)

        if tier2_articles:
            global_quality_scores = {
                a["url"]: article_quality_score(a) for a in articles if a.get("url")
            }
            for entry in tier2_articles:
                if isinstance(entry, tuple):
                    art, t2_topic = entry
                    upsert_article_summaries(
                        [art], topic=t2_topic, quality_scores=global_quality_scores
                    )
                else:
                    upsert_article_summaries(
                        [entry], topic=None, quality_scores=global_quality_scores
                    )

        total_written = 0
        entity_stats_by_topic = {}
        write_provider = (
            "newsapi"
            if provider == "newsapi-fallback"
            else ("directfeeds" if provider == "directfeeds-fallback" else provider)
        )
        for topic, topic_articles in topic_buckets.items():
            if not topic_articles:
                record_ingestion_run(
                    topic,
                    provider,
                    0,
                    started_at,
                    "empty",
                    error=f"No classified articles for topic '{topic}' in this ingest batch.",
                )
                continue
            total_written += upsert_articles(
                topic_articles, topic=topic, provider=write_provider
            )
            _store_articles_safe(topic_articles, topic)
            entity_stats_by_topic[topic] = _store_entity_mentions_with_translation(
                topic_articles, topic
            )
            record_ingestion_run(topic, provider, len(topic_articles), started_at, "ok")
        classified_total = sum(len(items) for items in topic_buckets.values())
        existing_or_unchanged = max(classified_total - total_written, 0)

        if (
            total_written == 0
            and provider != "directfeeds-fallback"
            and source_status()["directfeeds"]["enabled"]
        ):
            fallback_result = ingest_article_fallback(page_size=max(30, page_size // 2))
            if fallback_result.get("inserted_or_updated", 0) > 0:
                return {
                    "provider": fallback_result.get("provider", "directfeeds-fallback"),
                    "fetched": len(articles),
                    "classified": {
                        topic: len(items) for topic, items in topic_buckets.items()
                    },
                    "rejected": rejected,
                    "archived_documents": archive_summary["documents_written"],
                    "inserted_or_updated": fallback_result["inserted_or_updated"],
                    "existing_or_unchanged": fallback_result.get(
                        "existing_or_unchanged", 0
                    ),
                    "unclassified": unclassified,
                    "status": "ok",
                    "fallback": fallback_result,
                }

        _clear_headlines_resilient()
        save_ingestion_state(
            state_key,
            "global",
            provider,
            None,
            None,
            "ok" if total_written else "empty",
            payload={
                "fetched": len(articles),
                "classified": {
                    topic: len(items) for topic, items in topic_buckets.items()
                },
                "rejected": rejected,
                "unclassified": unclassified,
                "archived_documents": archive_summary["documents_written"],
                "inserted_or_updated": total_written,
                "existing_or_unchanged": existing_or_unchanged,
                "entity_extraction": entity_stats_by_topic,
            },
        )
        return {
            "provider": provider,
            "fetched": len(articles),
            "classified": {topic: len(items) for topic, items in topic_buckets.items()},
            "rejected": rejected,
            "archived_documents": archive_summary["documents_written"],
            "inserted_or_updated": total_written,
            "existing_or_unchanged": existing_or_unchanged,
            "unclassified": unclassified,
            "status": "ok" if total_written else "empty",
        }
    except Exception as exc:
        for topic in TOPICS:
            record_ingestion_run(
                topic, "unknown", 0, started_at, "error", error=str(exc)
            )
        save_ingestion_state(
            state_key,
            "global",
            "unknown",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[ingest] Global ingest failed: {exc}")
        return {
            "provider": "unknown",
            "fetched": 0,
            "classified": {topic: 0 for topic in TOPICS},
            "inserted_or_updated": 0,
            "unclassified": 0,
            "status": "error",
            "error": str(exc),
        }


def ingest_all_topics() -> list[dict]:
    global_result = ingest_global(page_size=100)
    results = [global_result]
    sparse_topics = [
        topic for topic, count in topic_counts().items() if count < MIN_TOPIC_ARTICLES
    ]
    for topic in sparse_topics:
        results.append(ingest_topic(topic))
    return results


# ---------------------------------------------------------------------------
# Scheduled cycles
# ---------------------------------------------------------------------------


def run_scheduled_ingest_cycle():
    return run_exclusive_or_skip(INGEST_JOB_LOCK, "ingest", ingest_all_topics)


def run_scheduled_gdelt_backfill():
    return run_exclusive_or_skip(
        BACKFILL_JOB_LOCK, "gdelt backfill", run_incremental_gdelt_backfill
    )


# ---------------------------------------------------------------------------
# GDELT backfill helpers
# ---------------------------------------------------------------------------


def _backfill_state_key(topic: str) -> str:
    return f"gdelt-backfill:{topic}"


def _scheduler_state_key() -> str:
    return "gdelt-backfill:scheduler"


def _cursor_start_for_topic(topic: str) -> datetime:
    state = load_ingestion_state(_backfill_state_key(topic))
    if state:
        if state.get("status") == "error" and state.get("cursor_start"):
            return parse_timestamp(state["cursor_start"]) or datetime.now(
                timezone.utc
            ) - timedelta(days=1)
        if state.get("cursor_end"):
            return parse_timestamp(state["cursor_end"]) or datetime.now(
                timezone.utc
            ) - timedelta(days=1)

    if GDELT_BACKFILL_START:
        configured = parse_timestamp(GDELT_BACKFILL_START)
        if configured:
            return configured

    bounds = get_topic_time_bounds(topic)
    if bounds.get("earliest_published_at"):
        parsed = parse_timestamp(bounds["earliest_published_at"])
        if parsed:
            return parsed - timedelta(hours=6)

    return datetime.now(timezone.utc) - timedelta(days=3)


def _backfill_should_wait(state: dict | None) -> tuple[bool, str | None]:
    if not state:
        return False, None
    payload = state.get("payload") or {}
    retry_after = payload.get("retry_after")
    if not retry_after:
        return False, None
    retry_at = parse_timestamp(retry_after)
    if retry_at and retry_at > datetime.now(timezone.utc):
        return True, retry_after
    return False, None


def _adaptive_backfill_config(state: dict | None) -> tuple[int, int, int]:
    payload = (state or {}).get("payload") or {}
    window_hours = int(payload.get("window_hours") or GDELT_BACKFILL_WINDOW_HOURS)
    page_size = int(payload.get("page_size") or GDELT_BACKFILL_PAGE_SIZE)
    failure_count = int(payload.get("failure_count") or 0)
    return (
        max(GDELT_BACKFILL_MIN_WINDOW_HOURS, window_hours),
        max(4, page_size),
        max(0, failure_count),
    )


def _next_scheduled_topics(topics: list[str] | None) -> list[str]:
    if topics:
        return topics
    state = load_ingestion_state(_scheduler_state_key())
    last_topic = state.get("topic") if state else None
    if last_topic in TOPICS:
        next_index = (TOPICS.index(last_topic) + 1) % len(TOPICS)
    else:
        next_index = 0
    chosen = TOPICS[next_index]
    save_ingestion_state(
        _scheduler_state_key(),
        topic=chosen,
        provider="gdelt-backfill-scheduler",
        cursor_start=None,
        cursor_end=None,
        status="ok",
        payload={"last_topic": chosen},
    )
    return [chosen]


def run_incremental_gdelt_backfill(topics: list[str] | None = None) -> list[dict]:
    target_end = datetime.now(timezone.utc) - timedelta(
        minutes=GDELT_BACKFILL_LAG_MINUTES
    )
    selected_topics = _next_scheduled_topics(topics)
    results = []

    for topic in selected_topics:
        started_at = time.time()
        state_key = _backfill_state_key(topic)
        state = load_ingestion_state(state_key)
        window_hours, page_size, failure_count = _adaptive_backfill_config(state)
        should_wait, retry_after = _backfill_should_wait(state)
        if should_wait:
            results.append(
                {
                    "topic": topic,
                    "status": "backoff",
                    "retry_after": retry_after,
                    "window_hours": window_hours,
                    "page_size": page_size,
                    "cursor_start": state.get("cursor_start") if state else None,
                    "cursor_end": state.get("cursor_end") if state else None,
                }
            )
            continue

        window_start = _cursor_start_for_topic(topic)
        if window_start >= target_end:
            results.append(
                {
                    "topic": topic,
                    "status": "idle",
                    "window_hours": window_hours,
                    "page_size": page_size,
                    "cursor_start": window_start.isoformat(),
                    "cursor_end": window_start.isoformat(),
                    "message": "Backfill cursor is caught up to the configured lag window.",
                }
            )
            continue

        window_end = min(window_start + timedelta(hours=window_hours), target_end)

        try:
            articles = fetch_gdelt_historic_articles(
                topic=topic,
                start=window_start,
                end=window_end,
                page_size=page_size,
                min_window_hours=GDELT_BACKFILL_MIN_WINDOW_HOURS,
            )
            if articles:
                inserted = upsert_articles(
                    articles,
                    topic=topic,
                    provider="gdelt",
                    default_analytic_tier="volume",
                )
                if GDELT_BACKFILL_CHROMA:
                    _store_articles_safe(articles, topic)
                entity_stats = _store_entity_mentions_with_translation(articles, topic)
                record_ingestion_run(
                    topic, "gdelt-backfill", len(articles), started_at, "ok"
                )
                save_ingestion_state(
                    state_key,
                    topic=topic,
                    provider="gdelt-backfill",
                    cursor_start=window_start.isoformat(),
                    cursor_end=window_end.isoformat(),
                    status="ok",
                    payload={
                        "fetched": len(articles),
                        "inserted_or_updated": inserted,
                        "retry_after": None,
                        "window_hours": min(
                            GDELT_BACKFILL_WINDOW_HOURS,
                            (
                                window_hours * 2
                                if len(articles) < max(4, page_size // 2)
                                else window_hours
                            ),
                        ),
                        "page_size": min(
                            GDELT_BACKFILL_PAGE_SIZE,
                            (
                                page_size + 1
                                if len(articles) >= max(4, page_size // 2)
                                else page_size
                            ),
                        ),
                        "failure_count": 0,
                        "entity_extraction": entity_stats,
                    },
                )
                results.append(
                    {
                        "topic": topic,
                        "status": "ok",
                        "cursor_start": window_start.isoformat(),
                        "cursor_end": window_end.isoformat(),
                        "window_hours": window_hours,
                        "page_size": page_size,
                        "fetched": len(articles),
                        "inserted_or_updated": inserted,
                    }
                )
            else:
                message = "No GDELT articles returned for this backfill window."
                record_ingestion_run(
                    topic, "gdelt-backfill", 0, started_at, "empty", error=message
                )
                save_ingestion_state(
                    state_key,
                    topic=topic,
                    provider="gdelt-backfill",
                    cursor_start=window_start.isoformat(),
                    cursor_end=window_end.isoformat(),
                    status="empty",
                    error=message,
                    payload={
                        "retry_after": None,
                        "window_hours": min(
                            GDELT_BACKFILL_WINDOW_HOURS, window_hours * 2
                        ),
                        "page_size": (
                            max(4, page_size - 1) if page_size > 4 else page_size
                        ),
                        "failure_count": 0,
                    },
                )
                results.append(
                    {
                        "topic": topic,
                        "status": "empty",
                        "cursor_start": window_start.isoformat(),
                        "cursor_end": window_end.isoformat(),
                        "window_hours": window_hours,
                        "page_size": page_size,
                        "fetched": 0,
                        "inserted_or_updated": 0,
                    }
                )
        except Exception as exc:
            error_text = str(exc)
            is_rate_limit = "429" in error_text or "Too Many Requests" in error_text
            is_timeout = (
                "Read timed out" in error_text or "timed out" in error_text.lower()
            )
            retry_delay = (
                GDELT_BACKFILL_RATE_LIMIT_RETRY_MINUTES
                if is_rate_limit
                else GDELT_BACKFILL_RETRY_MINUTES
            )
            next_failure_count = failure_count + 1
            retry_delay += min(360, next_failure_count * (45 if is_rate_limit else 20))
            retry_after = (
                datetime.now(timezone.utc) + timedelta(minutes=retry_delay)
            ).isoformat()
            next_window_hours = max(
                GDELT_BACKFILL_MIN_WINDOW_HOURS,
                (
                    max(1, window_hours // 2)
                    if (is_rate_limit or is_timeout)
                    else window_hours
                ),
            )
            next_page_size = (
                max(4, page_size - (4 if is_rate_limit else 2))
                if (is_rate_limit or is_timeout)
                else page_size
            )
            record_ingestion_run(
                topic, "gdelt-backfill", 0, started_at, "error", error=str(exc)
            )
            save_ingestion_state(
                state_key,
                topic=topic,
                provider="gdelt-backfill",
                cursor_start=window_start.isoformat(),
                cursor_end=window_end.isoformat(),
                status="error",
                error=error_text,
                payload={
                    "retry_after": retry_after,
                    "window_hours": next_window_hours,
                    "page_size": next_page_size,
                    "failure_count": next_failure_count,
                },
            )
            results.append(
                {
                    "topic": topic,
                    "status": "error",
                    "cursor_start": window_start.isoformat(),
                    "cursor_end": window_end.isoformat(),
                    "window_hours": window_hours,
                    "page_size": page_size,
                    "next_window_hours": next_window_hours,
                    "next_page_size": next_page_size,
                    "error": error_text,
                    "retry_after": retry_after,
                }
            )

    return results


# ---------------------------------------------------------------------------
# Historical queue fetch
# ---------------------------------------------------------------------------


def run_historical_queue_fetch(
    limit: int = HISTORICAL_FETCH_BATCH_LIMIT,
    batch_size: int = HISTORICAL_FETCH_WRITE_BATCH_SIZE,
    min_domain_interval_seconds: float = HISTORICAL_FETCH_DOMAIN_INTERVAL_SECONDS,
    max_attempts: int = HISTORICAL_FETCH_MAX_ATTEMPTS,
) -> dict:
    started_at = time.time()
    state_key = "historical-fetch-queue"
    try:
        result = fetch_historical_queue(
            limit=max(1, limit),
            batch_size=max(1, batch_size),
            min_domain_interval_seconds=max(0.0, min_domain_interval_seconds),
            max_attempts=max(1, max_attempts),
            dry_run=False,
        )
        status = (
            "ok"
            if result.get("inserted_or_updated", 0) > 0
            else ("empty" if result.get("processed", 0) == 0 else "ok")
        )
        save_ingestion_state(
            state_key,
            "historical",
            "historical-fetch",
            None,
            None,
            status,
            payload=result,
        )
        record_ingestion_run(
            "historical",
            "historical-fetch",
            int(result.get("inserted_or_updated", 0) or 0),
            started_at,
            status,
        )
        return result
    except Exception as exc:
        save_ingestion_state(
            state_key,
            "historical",
            "historical-fetch",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        record_ingestion_run(
            "historical", "historical-fetch", 0, started_at, "error", error=str(exc)
        )
        print(f"[historical-fetch] Historical queue fetch failed: {exc}")
        return {"status": "error", "error": str(exc)}


def run_scheduled_historical_queue_fetch():
    return run_exclusive_or_skip(
        HISTORICAL_FETCH_JOB_LOCK, "historical queue fetch", run_historical_queue_fetch
    )


def run_scheduled_story_materialization():
    def _job():
        return rebuild_materialized_story_clusters(
            topics=TOPICS,
            window_hours=CORPUS_WINDOW_HOURS,
            articles_limit=120,
        )

    return run_exclusive_or_skip(
        STORY_MATERIALIZATION_JOB_LOCK, "story materialization", _job
    )


# ---------------------------------------------------------------------------
# Fallback / health checks
# ---------------------------------------------------------------------------


def _gdelt_unhealthy() -> bool:
    states = [load_ingestion_state(_backfill_state_key(topic)) for topic in TOPICS]
    active_states = [state for state in states if state]
    if not active_states:
        return False
    unhealthy = 0
    for state in active_states:
        if state.get("status") != "error":
            continue
        payload = state.get("payload") or {}
        if payload.get("retry_after") or state.get("error"):
            unhealthy += 1
    return unhealthy >= len(TOPICS)


def _article_corpus_stale(max_age_hours: int = 8) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    global_bounds = get_topic_time_bounds()
    latest_global = parse_timestamp(global_bounds.get("latest_published_at"))
    if latest_global is None or latest_global < cutoff:
        return True
    sparse_topics = 0
    for topic in TOPICS:
        latest_topic = parse_timestamp(
            get_topic_time_bounds(topic).get("latest_published_at")
        )
        if latest_topic is None or latest_topic < cutoff:
            sparse_topics += 1
    return sparse_topics >= 1


def ingest_article_fallback(page_size: int = 40) -> dict:
    started_at = time.time()
    state_key = "analytic-ingest-fallback"
    fallback_provider = (
        "directfeeds" if source_status()["directfeeds"]["enabled"] else None
    )
    fallback_state_provider = (
        f"{fallback_provider}-fallback" if fallback_provider else "directfeeds-fallback"
    )
    if fallback_provider is None:
        save_ingestion_state(
            state_key,
            "global-fallback",
            fallback_state_provider,
            None,
            None,
            "disabled",
            error="No direct-feed fallback is configured.",
            payload={},
        )
        return {
            "status": "disabled",
            "reason": "No direct-feed fallback is configured.",
        }
    gdelt_unhealthy = _gdelt_unhealthy()
    corpus_stale = _article_corpus_stale()
    if not gdelt_unhealthy and not corpus_stale:
        reason = "GDELT is not marked unhealthy and the article corpus is still fresh."
        save_ingestion_state(
            state_key,
            "global-fallback",
            fallback_state_provider,
            None,
            None,
            "skipped",
            error=reason,
            payload={"gdelt_unhealthy": gdelt_unhealthy, "corpus_stale": corpus_stale},
        )
        return {"status": "skipped", "reason": reason}

    try:
        articles = fetch_global_articles_from_provider(
            fallback_provider, page_size=page_size
        )
        archive_summary = archive_provider_articles(
            articles, provider=fallback_state_provider, topic_hint="global-fallback"
        )
        if not articles:
            message = f"{fallback_provider} fallback did not return any fresh articles."
            for topic in TOPICS:
                record_ingestion_run(
                    topic,
                    fallback_state_provider,
                    0,
                    started_at,
                    "empty",
                    error=message,
                )
            save_ingestion_state(
                state_key,
                "global-fallback",
                fallback_state_provider,
                None,
                None,
                "empty",
                error=message,
                payload={
                    "fetched": 0,
                    "archived_documents": archive_summary["documents_written"],
                },
            )
            return {"status": "empty", "fetched": 0, "error": message}

        topic_buckets = {topic: [] for topic in TOPICS}
        rejected = 0
        for article in articles:
            article_topics = infer_article_topics(article)
            if not article_topics:
                continue
            if not should_promote_article(article, article_topics):
                rejected += 1
                continue
            topic_buckets[article_topics[0]].append(article)

        total_written = 0
        entity_stats_by_topic = {}
        for topic, topic_articles in topic_buckets.items():
            if not topic_articles:
                record_ingestion_run(
                    topic,
                    fallback_state_provider,
                    0,
                    started_at,
                    "empty",
                    error=f"No classified fallback articles for '{topic}'.",
                )
                continue
            total_written += upsert_articles(
                topic_articles, topic=topic, provider=fallback_provider
            )
            _store_articles_safe(topic_articles, topic)
            entity_stats_by_topic[topic] = _store_entity_mentions_with_translation(
                topic_articles, topic
            )
            record_ingestion_run(
                topic, fallback_state_provider, len(topic_articles), started_at, "ok"
            )
        classified_total = sum(len(rows) for rows in topic_buckets.values())
        existing_or_unchanged = max(classified_total - total_written, 0)

        if total_written:
            _clear_headlines_resilient()

        save_ingestion_state(
            state_key,
            "global-fallback",
            fallback_state_provider,
            None,
            None,
            "ok" if total_written else "empty",
            payload={
                "fetched": len(articles),
                "classified": {
                    topic: len(rows) for topic, rows in topic_buckets.items()
                },
                "rejected": rejected,
                "archived_documents": archive_summary["documents_written"],
                "inserted_or_updated": total_written,
                "existing_or_unchanged": existing_or_unchanged,
                "entity_extraction": entity_stats_by_topic,
            },
        )
        return {
            "provider": fallback_state_provider,
            "status": "ok" if total_written else "empty",
            "fetched": len(articles),
            "rejected": rejected,
            "archived_documents": archive_summary["documents_written"],
            "inserted_or_updated": total_written,
            "existing_or_unchanged": existing_or_unchanged,
            "classified": {topic: len(rows) for topic, rows in topic_buckets.items()},
        }
    except Exception as exc:
        for topic in TOPICS:
            record_ingestion_run(
                topic, fallback_state_provider, 0, started_at, "error", error=str(exc)
            )
        save_ingestion_state(
            state_key,
            "global-fallback",
            fallback_state_provider,
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        return {"status": "error", "error": str(exc)}


# ---------------------------------------------------------------------------
# Bootstrap / seeding
# ---------------------------------------------------------------------------


def bootstrap_from_legacy_cache() -> dict:
    backend_dir = Path(__file__).resolve().parent.parent
    legacy_paths = [
        backend_dir / "othello_cache.db",
        backend_dir.parent.parent / "backend" / "othello_cache.db",
    ]
    imported = {topic: 0 for topic in TOPICS}

    for db_path in legacy_paths:
        if not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(db_path)
            rows = conn.execute("SELECT topic, sources FROM briefing_cache").fetchall()
            conn.close()
        except Exception as exc:
            print(f"[bootstrap] Failed reading legacy cache {db_path}: {exc}")
            continue

        for topic, sources_json in rows:
            if topic not in TOPICS:
                continue
            try:
                articles = json.loads(sources_json or "[]")
            except json.JSONDecodeError:
                continue
            if not articles:
                continue
            written = upsert_articles(articles, topic=topic, provider="legacy-cache")
            _store_articles_safe(articles, topic)
            _store_entity_mentions_with_translation(articles, topic)
            imported[topic] += written or len(articles)

    return {"imported": imported}


def seed_local_corpus() -> dict:
    backend_dir = Path(__file__).resolve().parent.parent
    cache_paths = [
        backend_dir / "othello_cache.db",
        backend_dir.parent.parent / "backend" / "othello_cache.db",
    ]
    totals = {
        "articles_seen": 0,
        "inserted_or_updated": 0,
        "stored_for_entities": 0,
        "topics": {topic: 0 for topic in TOPICS},
        "paths_scanned": [],
    }

    seen_pairs: set[tuple[str, str]] = set()
    for db_path in cache_paths:
        if not db_path.exists():
            continue
        totals["paths_scanned"].append(str(db_path))
        try:
            conn = sqlite3.connect(db_path)
            briefing_rows = conn.execute(
                "SELECT topic, sources FROM briefing_cache"
            ).fetchall()
            headline_rows = conn.execute(
                "SELECT stories FROM headlines_cache"
            ).fetchall()
            conn.close()
        except Exception as exc:
            print(f"[seed] Failed reading cache {db_path}: {exc}")
            continue

        topic_buckets: dict[str, list[dict]] = {topic: [] for topic in TOPICS}

        for topic, sources_json in briefing_rows:
            if topic not in TOPICS:
                continue
            try:
                articles = json.loads(sources_json or "[]")
            except json.JSONDecodeError:
                continue
            for article in articles:
                url = article.get("url")
                pair = (topic, url or "")
                if not url or pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                topic_buckets[topic].append(article)
                totals["articles_seen"] += 1

        for (stories_json,) in headline_rows:
            try:
                stories = json.loads(stories_json or "[]")
            except json.JSONDecodeError:
                continue
            for story in stories:
                story_topic = story.get("topic")
                for article in story.get("sources", []):
                    url = article.get("url")
                    inferred_topics = (
                        [story_topic]
                        if story_topic in TOPICS
                        else infer_article_topics(article)
                    )
                    for topic in inferred_topics:
                        if topic not in TOPICS:
                            continue
                        pair = (topic, url or "")
                        if not url or pair in seen_pairs:
                            continue
                        seen_pairs.add(pair)
                        topic_buckets[topic].append(article)
                        totals["articles_seen"] += 1

        for topic, articles in topic_buckets.items():
            if not articles:
                continue
            inserted = upsert_articles(articles, topic=topic, provider="local-seed")
            _store_articles_safe(articles, topic)
            _store_entity_mentions_with_translation(articles, topic)
            totals["inserted_or_updated"] += inserted
            totals["stored_for_entities"] += len(articles)
            totals["topics"][topic] += len(articles)

    _clear_headlines_resilient()

    # Rebuild headlines cache and topic briefings (late import to avoid
    # circular dependency -- these functions have not yet been extracted from
    # main.py).
    from services.headlines_service import rebuild_headlines_cache
    from services.briefing_service import build_topic_briefing

    rebuild_headlines_cache(use_llm=False)
    for topic in TOPICS:
        build_topic_briefing(topic, force_refresh=True)

    return totals


def _ensure_topic_corpus(
    topic: str, minimum_articles: int = MIN_TOPIC_ARTICLES
) -> None:
    if get_article_count(topic=topic, hours=72) >= minimum_articles:
        return
    ingest_topic(topic)


# ---------------------------------------------------------------------------
# Source / registry refresh
# ---------------------------------------------------------------------------


def refresh_registry_sources():
    try:
        result = ingest_registry_sources()
        save_ingestion_state(
            "source-registry-refresh",
            "source-registry",
            "directfeeds",
            None,
            None,
            "ok",
            payload=result.get("totals", {}),
        )
        return result
    except Exception as exc:
        save_ingestion_state(
            "source-registry-refresh",
            "source-registry",
            "directfeeds",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[sources] Registry refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_direct_feed_layer():
    started_at = time.time()
    try:
        result = ingest_direct_feed_layer()
        totals = result.get("totals", {})
        promoted = int(totals.get("promoted_articles", 0) or 0)
        state_status = result.get("status", "ok")
        record_ingestion_run(
            "directfeeds",
            "directfeeds",
            promoted,
            started_at,
            "ok" if state_status == "ok" else state_status,
        )
        save_ingestion_state(
            "direct-feed-layer-refresh",
            "directfeeds",
            "directfeeds",
            None,
            None,
            "ok" if state_status == "ok" else state_status,
            payload=totals,
        )
        return result
    except Exception as exc:
        record_ingestion_run(
            "directfeeds", "directfeeds", 0, started_at, "error", error=str(exc)
        )
        save_ingestion_state(
            "direct-feed-layer-refresh",
            "directfeeds",
            "directfeeds",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[directfeeds] Layer refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def sync_registry_mirror():
    try:
        result = mirror_corpus_articles_into_registry(
            hours=SOURCE_REGISTRY_MIRROR_HOURS
        )
        save_ingestion_state(
            "source-registry-mirror",
            "source-registry-mirror",
            "registry-mirror",
            None,
            None,
            "ok",
            payload=result,
        )
        return result
    except Exception as exc:
        save_ingestion_state(
            "source-registry-mirror",
            "source-registry-mirror",
            "registry-mirror",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[sources] Registry mirror failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_official_updates():
    started_at = time.time()
    try:
        result = ingest_official_updates()
        record_ingestion_run(
            "official-updates",
            "official",
            result["totals"]["official_updates"],
            started_at,
            "ok",
        )
        save_ingestion_state(
            "official-updates-refresh",
            "official-updates",
            "official",
            None,
            None,
            "ok",
            payload=result.get("totals", {}),
        )
        return result
    except Exception as exc:
        record_ingestion_run(
            "official-updates", "official", 0, started_at, "error", error=str(exc)
        )
        save_ingestion_state(
            "official-updates-refresh",
            "official-updates",
            "official",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[official] Update refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_acled_events():
    started_at = time.time()
    try:
        result = ingest_acled_recent()
        record_ingestion_run(
            "acled", "acled", result["inserted_or_updated"], started_at, "ok"
        )
        save_ingestion_state(
            "structured-events-refresh",
            "acled",
            "acled",
            None,
            None,
            "ok",
            payload=result,
        )
        return result
    except Exception as exc:
        record_ingestion_run("acled", "acled", 0, started_at, "error", error=str(exc))
        save_ingestion_state(
            "structured-events-refresh",
            "acled",
            "acled",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[acled] Structured event refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_gdelt_gkg_events():
    started_at = time.time()
    try:
        result = ingest_gdelt_gkg_recent(hours=GDELT_GKG_REFRESH_HOURS)
        record_ingestion_run(
            "gdelt_gkg", "gdelt_gkg", result["inserted_or_updated"], started_at, "ok"
        )
        save_ingestion_state(
            "gdelt-gkg-events-refresh",
            "gdelt_gkg",
            "gdelt_gkg",
            None,
            None,
            "ok",
            payload=result,
        )
        from services.map_service import (
            _MAP_ATTENTION_CACHE,
            _STORY_LOCATION_INDEX_CACHE,
        )

        _MAP_ATTENTION_CACHE.clear()
        _STORY_LOCATION_INDEX_CACHE.clear()
        return result
    except Exception as exc:
        record_ingestion_run(
            "gdelt_gkg", "gdelt_gkg", 0, started_at, "error", error=str(exc)
        )
        save_ingestion_state(
            "gdelt-gkg-events-refresh",
            "gdelt_gkg",
            "gdelt_gkg",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        print(f"[gdelt_gkg] Event refresh failed: {exc}")
        return {"status": "error", "error": str(exc)}


def refresh_recent_translations(limit: int = 18):
    started_at = time.time()
    if not REQUEST_ENABLE_TRANSLATION:
        return {"status": "disabled", "translated": 0}

    translated = 0
    failures = 0
    provider_counts = {}
    candidates = sorted(
        get_articles_missing_translation(limit=max(limit * 3, 48), hours=336),
        key=_article_translation_priority,
        reverse=True,
    )
    for article in candidates:
        if translated >= limit:
            break
        priority = _article_translation_priority(article)
        if priority < TRANSLATION_MIN_SCORE:
            continue
        try:
            translation = translate_article(
                article,
                allow_remote_fallback=priority >= TRANSLATION_REMOTE_FALLBACK_SCORE,
            )
            save_article_translation(
                article_url=article["url"],
                source_language=article.get("language") or "unknown",
                translated_title=translation["translated_title"],
                translated_description=translation.get("translated_description"),
                translation_provider=translation.get("provider", "translation"),
                target_language=translation.get("target_language", "en"),
            )
            translated += 1
            provider = translation.get("provider", "translation")
            provider_counts[provider] = provider_counts.get(provider, 0) + 1
        except Exception as exc:
            failures += 1
            print(f"[translation] Refresh failed for {article.get('url')}: {exc}")

    status = "ok" if failures == 0 else "partial"
    provider_label = (
        "+".join(sorted(provider_counts))
        if provider_counts
        else "selective-translation"
    )
    record_ingestion_run(
        "translations",
        provider_label,
        translated,
        started_at,
        status,
        error=None if failures == 0 else f"{failures} translations failed",
    )
    return {
        "status": status,
        "translated": translated,
        "failed": failures,
        "providers": provider_counts,
    }


def refresh_source_reliability():
    started_at = time.time()
    try:
        global_snapshot = build_claim_resolution_snapshot(topic=None, days=180)
        topic_snapshots = {
            topic: build_claim_resolution_snapshot(topic=topic, days=180)
            for topic in TOPICS
        }
        total_claims = int(global_snapshot.get("claim_records", 0) or 0)
        record_ingestion_run(
            "source-reliability", "claim-resolution", total_claims, started_at, "ok"
        )
        result = {
            "status": "ok",
            "global_claim_records": total_claims,
            "global_sources": len(global_snapshot.get("sources", [])),
            "topics": {
                topic: {
                    "claim_records": snapshot.get("claim_records", 0),
                    "sources": len(snapshot.get("sources", [])),
                }
                for topic, snapshot in topic_snapshots.items()
            },
        }
        save_ingestion_state(
            "source-reliability-refresh",
            "source-reliability",
            "claim-resolution",
            None,
            None,
            "ok",
            payload=result,
        )
        return result
    except Exception as exc:
        record_ingestion_run(
            "source-reliability",
            "claim-resolution",
            0,
            started_at,
            "error",
            error=str(exc),
        )
        save_ingestion_state(
            "source-reliability-refresh",
            "source-reliability",
            "claim-resolution",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        return {"status": "error", "error": str(exc)}


def refresh_foresight_layer():
    started_at = time.time()
    try:
        prediction_snapshot = load_prediction_ledger(refresh=True, limit=250)
        archive_snapshot = load_early_signal_archive(limit=100, minimum_gap_hours=4)
        record_ingestion_run(
            "foresight",
            "foresight",
            len(prediction_snapshot.get("predictions", []))
            + archive_snapshot.get("count", 0),
            started_at,
            "ok",
        )
        result = {
            "status": "ok",
            "predictions": prediction_snapshot.get("counts", {}),
            "early_signal_count": archive_snapshot.get("count", 0),
        }
        save_ingestion_state(
            "foresight-layer-refresh",
            "foresight",
            "foresight",
            None,
            None,
            "ok",
            payload=result,
        )
        return result
    except Exception as exc:
        record_ingestion_run(
            "foresight", "foresight", 0, started_at, "error", error=str(exc)
        )
        save_ingestion_state(
            "foresight-layer-refresh",
            "foresight",
            "foresight",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        return {"status": "error", "error": str(exc)}


def refresh_narrative_drift_layer():
    started_at = time.time()
    targets: list[tuple[str, str]] = []
    seen_targets: set[tuple[str, str]] = set()

    for topic in TOPICS:
        for entity in get_top_entities(
            topic=topic, days=21, limit=max(1, NARRATIVE_DRIFT_TOP_SUBJECTS)
        ):
            subject = (entity.get("entity") or "").strip()
            if len(subject) < 3:
                continue
            target_key = (topic, subject.lower())
            if target_key in seen_targets:
                continue
            seen_targets.add(target_key)
            targets.append((topic, subject))

    if not targets:
        result = {"status": "empty", "subjects": 0, "snapshots": []}
        record_ingestion_run(
            "narrative-drift",
            "framing",
            0,
            started_at,
            "empty",
            error="No top entities available for drift analysis.",
        )
        save_ingestion_state(
            "narrative-drift-refresh",
            "narrative-drift",
            "framing",
            None,
            None,
            "empty",
            error="No top entities available for drift analysis.",
            payload=result,
        )
        return result

    try:
        snapshots = []
        for topic, subject in targets:
            payload = analyze_narrative_drift(
                subject, topic=topic, days=180, refresh=True
            )
            snapshots.append(
                {
                    "topic": topic,
                    "subject": subject,
                    "article_count": int(payload.get("article_count", 0) or 0),
                    "shift_count": len(payload.get("shifts", []) or []),
                }
            )

        populated = sum(1 for snapshot in snapshots if snapshot["article_count"] > 0)
        status = "ok" if populated else "empty"
        result = {
            "status": status,
            "subjects": len(snapshots),
            "populated_subjects": populated,
            "snapshots": snapshots,
        }
        record_ingestion_run(
            "narrative-drift", "framing", populated, started_at, status
        )
        save_ingestion_state(
            "narrative-drift-refresh",
            "narrative-drift",
            "framing",
            None,
            None,
            status,
            payload=result,
        )
        return result
    except Exception as exc:
        record_ingestion_run(
            "narrative-drift", "framing", 0, started_at, "error", error=str(exc)
        )
        save_ingestion_state(
            "narrative-drift-refresh",
            "narrative-drift",
            "framing",
            None,
            None,
            "error",
            error=str(exc),
            payload={},
        )
        return {"status": "error", "error": str(exc)}


# ---------------------------------------------------------------------------
# Public service payloads (called from API route handlers)
# ---------------------------------------------------------------------------


def trigger_ingest_payload(topic: str | None = None):
    def run():
        if topic:
            if topic not in TOPICS:
                raise HTTPException(
                    status_code=400, detail=f"Topic must be one of {TOPICS}"
                )
            return {"results": [ingest_topic(topic)]}
        return {"results": ingest_all_topics()}

    return run_exclusive(INGEST_JOB_LOCK, "Ingest", run)


def trigger_backfill_payload(topic: str | None = None):
    def run():
        if topic:
            if topic not in TOPICS:
                raise HTTPException(
                    status_code=400, detail=f"Topic must be one of {TOPICS}"
                )
            return {"results": run_incremental_gdelt_backfill([topic])}
        return {"results": run_incremental_gdelt_backfill()}

    return run_exclusive(BACKFILL_JOB_LOCK, "GDELT backfill", run)


def source_registry_payload():
    sources = registry_sources_with_feed_status()
    return {"count": len(sources), "sources": sources}


def source_refresh_payload():
    return {"refresh": refresh_registry_sources(), "mirror": sync_registry_mirror()}


def official_refresh_payload():
    return refresh_official_updates()


def acled_refresh_payload():
    return refresh_acled_events()


def gdelt_gkg_refresh_payload():
    return refresh_gdelt_gkg_events()


def trigger_local_seed_payload():
    return seed_local_corpus()


def source_probe_payload(query: str = "Iran OR Israel OR war", page_size: int = 10):
    return probe_sources(query, page_size=max(1, min(page_size, 25)))
