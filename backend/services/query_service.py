"""Query and timeline logic extracted from main.py."""

import os
import re

from chroma import get_collection_stats, search_articles
from fastapi import HTTPException
from pydantic import BaseModel

from cache import load_headlines
from corpus import (
    get_articles_by_urls,
    get_recent_articles,
    search_recent_articles_by_keywords,
)
from news import fetch_articles_for_query

from core.config import (
    HEADLINES_TTL,
    QUERY_STOPWORDS,
    REQUEST_ENABLE_LIVE_FETCH,
    REQUEST_ENABLE_LLM_RESPONSES,
    REQUEST_ENABLE_VECTOR_SEARCH,
    TOPICS,
)


class QueryRequest(BaseModel):
    question: str
    topic: str | None = None
    region_context: str | None = None
    hotspot_id: str | None = None
    story_event_id: str | None = None
    source_urls: list[str] | None = None
    attention_window: str | None = None


# ---------------------------------------------------------------------------
# Internal query helpers (moved from main.py)
# ---------------------------------------------------------------------------


def _extract_search_focus(question: str) -> str:
    quoted = [
        match.group(1).strip() for match in re.finditer(r'"([^"]{4,})"', question or "")
    ]
    if quoted:
        return quoted[0]
    cleaned = re.sub(r"[^A-Za-z0-9\s-]", " ", question or "")
    words = [
        word
        for word in cleaned.split()
        if len(word) >= 3 and word.lower() not in QUERY_STOPWORDS
    ]
    if not words:
        return (question or "").strip()
    return " ".join(words[:8])


def _compose_query_search_seed(
    question: str,
    region_context: str | None = None,
    source_urls: list[str] | None = None,
) -> str:
    parts: list[str] = []
    if region_context and str(region_context).strip():
        parts.append(str(region_context).strip())
    q = (question or "").strip()
    if not parts:
        return q
    return " ".join(parts) + (" " + q if q else "")


def _clean_source_urls(source_urls: list[str] | None, *, limit: int = 12) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for url in source_urls or []:
        candidate = str(url or "").strip()
        if not candidate.startswith("http") or candidate in seen:
            continue
        seen.add(candidate)
        cleaned.append(candidate)
        if len(cleaned) >= limit:
            break
    return cleaned


def _resolve_story_source_urls(story_event_id: str | None) -> list[str]:
    event_id = str(story_event_id or "").strip()
    if not event_id:
        return []
    from services.headlines_service import rebuild_headlines_cache

    cached = load_headlines(ttl=HEADLINES_TTL) or rebuild_headlines_cache(use_llm=False)
    for story in cached:
        if str(story.get("event_id") or "").strip() != event_id:
            continue
        return _clean_source_urls(
            [
                source.get("url")
                for source in (story.get("sources") or [])
                if isinstance(source, dict)
            ],
            limit=12,
        )
    return []


def _resolve_hotspot_source_urls(
    hotspot_id: str | None, attention_window: str | None = None
) -> list[str]:
    target_id = str(hotspot_id or "").strip()
    if not target_id:
        return []
    from services.map_service import _build_hotspot_attention_map

    payload = _build_hotspot_attention_map(window=attention_window or "24h")
    for hotspot in payload.get("hotspots", []):
        if str(hotspot.get("hotspot_id") or "").strip() != target_id:
            continue
        urls: list[str] = []
        for sample in hotspot.get("sample_events") or []:
            sample_urls = sample.get("source_urls")
            if isinstance(sample_urls, list):
                urls.extend(sample_urls)
            event_id = str(sample.get("event_id") or "").strip()
            if event_id.startswith("http"):
                urls.append(event_id)
        return _clean_source_urls(urls, limit=12)
    return []


def _normalize_query_corpus_topic(topic: str | None) -> str | None:
    if not topic:
        return None
    t = str(topic).strip().lower()
    if t == "conflict":
        return "geopolitics"
    if t in TOPICS:
        return t
    return None


def _infer_query_topic(query: str) -> str | None:
    lowered = (query or "").lower()
    best_topic = None
    best_score = 0
    for topic, keywords in {
        "geopolitics": {
            "iran",
            "israel",
            "ukraine",
            "russia",
            "china",
            "taiwan",
            "war",
            "military",
            "sanctions",
            "diplomacy",
            "missile",
            "nato",
            "conflict",
            "ceasefire",
            "strike",
        },
        "economics": {
            "inflation",
            "tariffs",
            "rates",
            "markets",
            "economy",
            "economic",
            "fed",
            "reserve",
            "jobs",
            "gdp",
            "oil",
            "trade",
            "currency",
            "bonds",
            "yields",
        },
    }.items():
        score = sum(1 for keyword in keywords if keyword in lowered)
        if score > best_score:
            best_topic = topic
            best_score = score
    return best_topic if best_score else None


def _append_unique_articles(
    target: list[dict], seen_urls: set[str], articles: list[dict], limit: int
) -> None:
    for article in articles:
        url = article.get("url")
        if not url or url in seen_urls:
            continue
        target.append(article)
        seen_urls.add(url)
        if len(target) >= limit:
            return


def _gather_query_articles(
    question: str,
    topic: str | None = None,
    limit: int = 12,
    keyword_hours: int = 336,
    *,
    region_context: str | None = None,
    hotspot_id: str | None = None,
    story_event_id: str | None = None,
    source_urls: list[str] | None = None,
    attention_window: str | None = None,
) -> tuple[list[dict], dict]:
    from services.ingest_service import ensure_article_translations

    grounding_urls = _clean_source_urls(source_urls, limit=12)
    if not grounding_urls and story_event_id:
        grounding_urls = _resolve_story_source_urls(story_event_id)
    if not grounding_urls and hotspot_id:
        grounding_urls = _resolve_hotspot_source_urls(
            hotspot_id, attention_window=attention_window
        )

    search_seed = _compose_query_search_seed(question, region_context, source_urls)
    focus = _extract_search_focus(search_seed)
    resolved_topic = topic or _infer_query_topic(focus or search_seed)
    combined: list[dict] = []
    seen_urls: set[str] = set()
    historical_sources = 0
    live_sources = 0
    grounding_used = False

    if grounding_urls:
        grounded_map = get_articles_by_urls(
            grounding_urls, limit=max(limit, len(grounding_urls))
        )
        grounded_articles = [
            grounded_map[url] for url in grounding_urls if url in grounded_map
        ]
        if grounded_articles:
            _append_unique_articles(combined, seen_urls, grounded_articles, limit)
            historical_sources = len(combined)
            grounding_used = True

    if REQUEST_ENABLE_VECTOR_SEARCH and len(combined) < limit:
        try:
            vector_hits = search_articles(
                focus or search_seed, n_results=min(limit, 8), topic=resolved_topic
            )
            _append_unique_articles(combined, seen_urls, vector_hits, limit)
            historical_sources = len(combined)
        except Exception as exc:
            print(
                f"[query] Chroma search failed, falling back to keyword search: {exc}"
            )

    keyword_queries = []
    if focus:
        keyword_queries.append(focus)
    if search_seed not in keyword_queries:
        keyword_queries.append(search_seed)

    for candidate in keyword_queries:
        keyword_hits = search_recent_articles_by_keywords(
            candidate,
            topic=resolved_topic,
            limit=max(limit * 2, 18),
            hours=keyword_hours,
        )
        _append_unique_articles(combined, seen_urls, keyword_hits, limit)
        historical_sources = len(combined)
        if len(combined) >= limit:
            break

    if len(combined) < max(6, limit // 2) and resolved_topic:
        recent_topic_articles = get_recent_articles(
            topic=resolved_topic, limit=limit, hours=keyword_hours
        )
        _append_unique_articles(combined, seen_urls, recent_topic_articles, limit)
        historical_sources = len(combined)

    if not combined:
        recent_global_articles = get_recent_articles(limit=limit, hours=keyword_hours)
        _append_unique_articles(combined, seen_urls, recent_global_articles, limit)
        historical_sources = len(combined)

    if REQUEST_ENABLE_LIVE_FETCH and len(combined) < max(6, limit // 2):
        try:
            live_hits = fetch_articles_for_query(
                focus or search_seed, page_size=min(limit, 8)
            )
            before = len(combined)
            _append_unique_articles(combined, seen_urls, live_hits, limit)
            live_sources = len(combined) - before
        except Exception as exc:
            print(
                f"[query] Live fetch failed, continuing with stored corpus only: {exc}"
            )

    combined = ensure_article_translations(combined[:limit], max_articles=8)
    return combined, {
        "focus": focus or search_seed,
        "topic": resolved_topic,
        "historical_sources": historical_sources,
        "live_sources": live_sources,
        "region_context": region_context,
        "hotspot_id": hotspot_id,
        "story_event_id": story_event_id,
        "grounding_used": grounding_used,
        "grounding_source_count": len(grounding_urls),
    }


# ---------------------------------------------------------------------------
# Fallbacks (moved from main.py)
# ---------------------------------------------------------------------------


def _query_fallback(
    question: str, articles: list[dict], topic: str | None = None
) -> str:
    lead = articles[:3]
    lead_lines = (
        "\n".join(
            f"- {article.get('source', 'Unknown source')} ({article.get('published_at', 'Unknown time')}): {article.get('title', 'Untitled')}"
            for article in lead
        )
        or "- No strong lead reporting was available."
    )

    themes = []
    seen_titles = set()
    for article in articles:
        title = (article.get("title") or "").strip()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        themes.append(f"- {title}")
        if len(themes) == 5:
            break

    scope = f" within the {topic} corpus" if topic else ""
    return f"""SITUATION REPORT:
This answer was generated from the stored Othello corpus{scope} because live LLM analysis is temporarily unavailable. The strongest relevant reporting currently includes:
{lead_lines}

KEY DEVELOPMENTS:
{chr(10).join(themes) or "- The corpus contains relevant reporting, but the event pattern is still thin."}

ANALYTIC TAKE:
- The reporting set indicates this question is active enough to surface across multiple articles.
- Confidence should be treated as moderate until a broader mix of sources converges on the same facts.
- Use the linked sources below as the primary evidence base while the analysis model is offline.
"""


def _timeline_fallback(query: str, articles: list[dict]) -> dict:
    sorted_articles = sorted(
        articles, key=lambda article: article.get("published_at", "")
    )
    return {
        "title": f"Timeline: {query}",
        "summary": "Chronology generated directly from the stored article corpus.",
        "events": [
            {
                "date": article.get("published_at", ""),
                "headline": article.get("title", "Untitled"),
                "description": article.get("description", "No description available."),
                "significance": "MEDIUM",
                "source": article.get("source", "Unknown source"),
            }
            for article in sorted_articles[:10]
        ],
    }


# ---------------------------------------------------------------------------
# Public service endpoints
# ---------------------------------------------------------------------------


def query_payload(request: QueryRequest):
    from analyst import answer_query

    corpus_topic = _normalize_query_corpus_topic(request.topic)
    combined, meta = _gather_query_articles(
        request.question,
        topic=corpus_topic,
        limit=12,
        region_context=request.region_context,
        hotspot_id=request.hotspot_id,
        story_event_id=request.story_event_id,
        source_urls=request.source_urls,
        attention_window=request.attention_window,
    )
    if not combined:
        raise HTTPException(
            status_code=404, detail="No relevant reporting found in the corpus"
        )
    if REQUEST_ENABLE_LLM_RESPONSES and os.getenv("GROQ_API_KEY"):
        try:
            answer = answer_query(request.question, combined, topic=meta["topic"])
        except Exception:
            answer = _query_fallback(request.question, combined, topic=meta["topic"])
    else:
        answer = _query_fallback(request.question, combined, topic=meta["topic"])
    return {
        "question": request.question,
        "answer": answer,
        "sources": combined,
        "source_count": len(combined),
        "historical_sources": meta["historical_sources"],
        "live_sources": meta["live_sources"],
        "topic": meta["topic"] or request.topic,
    }


def timeline_payload(query: str):
    from analyst import build_timeline

    articles, meta = _gather_query_articles(query, limit=18)
    if not articles:
        raise HTTPException(status_code=404, detail="No articles found for this topic")
    if not REQUEST_ENABLE_LLM_RESPONSES or not os.getenv("GROQ_API_KEY"):
        return _timeline_fallback(query, articles)
    try:
        return build_timeline(query, articles)
    except Exception:
        return _timeline_fallback(query, articles)


def chroma_stats_payload():
    return get_collection_stats()
