import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from sources.source_catalog import SOURCE_PACKS, SOURCE_SEEDS

# Re-export public API from submodules for backwards compatibility
from normalization.articles import (
    normalize_article_title,
    normalize_article_description,
    is_english_article,
)
from ranking.article_quality import (
    article_quality_score,
    should_promote_article,
    diversify_articles,
)
from classification.topics import infer_article_topics
from providers.gdelt import (
    fetch_gdelt_historic_articles,
    probe_gdelt_window,
    GDELT_MAX_RECORDS,
    GDELT_ARCHIVE_MIN_WINDOW_HOURS,
)


try:
    from langdetect import LangDetectException, detect
except Exception:  # pragma: no cover - optional runtime dependency
    LangDetectException = Exception
    detect = None

load_dotenv(Path(__file__).with_name(".env"))

# Orchestration-level constants
TOPIC_QUERIES = {
    "geopolitics": [
        "war OR conflict OR sanctions OR diplomacy OR military OR ceasefire",
        '"Russia" OR "Ukraine" OR "China" OR "Taiwan" OR "Iran" OR "Israel"',
        "NATO OR deterrence OR escalation OR missile OR strike",
    ],
    "economics": [
        '"Federal Reserve" OR inflation OR recession OR markets OR tariffs OR "interest rates"',
        '"central bank" OR yields OR bonds OR trade OR gdp OR jobs',
        '"oil prices" OR manufacturing OR liquidity OR deficit OR currency',
    ],
}

GLOBAL_INGEST_QUERIES = [
    "war OR conflict OR diplomacy OR sanctions OR summit OR ceasefire OR military",
    "inflation OR markets OR recession OR tariffs OR trade OR rates OR economy",
]

TOPIC_ARCHIVE_QUERIES = {
    "geopolitics": [
        "(Iran OR Israel OR Ukraine OR Russia OR China OR Taiwan) AND (war OR military OR strike OR sanctions OR diplomacy OR ceasefire)",
        "(NATO OR Pentagon OR Kremlin OR Beijing OR Tehran OR Hezbollah OR Hamas) AND (missile OR summit OR conflict OR invasion OR talks)",
    ],
    "economics": [
        '("Federal Reserve" OR inflation OR recession OR tariffs OR trade OR markets OR "interest rates") AND (economy OR market OR rates OR prices)',
        "(oil OR bonds OR yields OR currency OR manufacturing OR jobs OR gdp) AND (economy OR trade OR inflation OR slowdown)",
    ],
}

ENABLE_NEWSAPI_FALLBACK = (
    os.getenv("OTHELLO_ENABLE_NEWSAPI_FALLBACK", "false").lower() == "true"
)


def source_status() -> dict:
    directfeed_enabled = any(
        (seed.get("source_type") == "article")
        and ((seed.get("metadata") or {}).get("adapter") == "rss")
        for seed in SOURCE_SEEDS
    )
    return {
        "preferred": "gdelt+directfeeds",
        "gdelt": {"enabled": True, "api_key_required": False},
        "directfeeds": {
            "enabled": directfeed_enabled,
            "api_key_required": False,
            "packs": [
                pack_name
                for pack_name, meta in SOURCE_PACKS.items()
                if "article" in (meta.get("source_types") or [])
            ],
        },
        "newsapi": {
            "enabled": bool(os.getenv("NEWS_API_KEY")) and ENABLE_NEWSAPI_FALLBACK,
            "available": bool(os.getenv("NEWS_API_KEY")),
            "api_key_required": True,
        },
    }


def _dedupe(articles: list[dict]) -> list[dict]:
    deduped = []
    seen = set()
    for article in articles:
        url = article.get("url")
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(article)
    return deduped


def _normalize_topic_queries(topic: str) -> list[str]:
    configured = TOPIC_QUERIES.get(topic, topic)
    if isinstance(configured, list):
        return configured
    return [configured]


def _archive_queries_for_topic(topic: str) -> list[str]:
    return TOPIC_ARCHIVE_QUERIES.get(topic, _normalize_topic_queries(topic))


def _provider_fetch(provider: str, query: str, page_size: int) -> list[dict]:
    """Fetch articles from a specific provider."""
    if provider == "gdelt":
        from providers.gdelt import _fetch_gdelt
        return _fetch_gdelt(query, maxrecords=page_size)
    
    if provider == "directfeeds":
        from providers.directfeeds import _articles_from_direct_feeds
        inferred_topic = (
            "economics"
            if "inflation" in query.lower()
            or "rates" in query.lower()
            or "econom" in query.lower()
            else "geopolitics"
        )
        return _articles_from_direct_feeds(topic=inferred_topic, page_size=page_size)
    
    if provider == "newsapi":
        from providers.newsapi import _fetch_newsapi
        return _fetch_newsapi(query, page_size=page_size)
    
    raise ValueError(f"Unknown provider: {provider}")


def _fetch_from_providers(query: str, page_size: int) -> list[dict]:
    """Try multiple providers in order until one succeeds."""
    from providers.gdelt import _fetch_gdelt
    from providers.newsapi import _fetch_newsapi
    
    provider_order = os.getenv("OTHELLO_SOURCE_PROVIDER", "auto").lower()
    errors = []

    def try_gdelt():
        return _fetch_gdelt(query, maxrecords=page_size)

    def try_newsapi():
        return _fetch_newsapi(query, page_size=page_size)

    providers = {
        "gdelt": [try_gdelt],
        "directfeeds": [lambda: _provider_fetch("directfeeds", query, page_size)],
        "newsapi": [try_newsapi],
        "auto": [try_gdelt] + ([try_newsapi] if ENABLE_NEWSAPI_FALLBACK else []),
    }.get(
        provider_order, [try_gdelt] + ([try_newsapi] if ENABLE_NEWSAPI_FALLBACK else [])
    )

    for fetcher in providers:
        try:
            articles = _dedupe(fetcher())
            if articles:
                return articles
        except Exception as exc:
            errors.append(str(exc))

    if errors:
        print(f"[news] Source fetch errors: {' | '.join(errors)}")
    return []


def _collect_query_batch(queries: list[str], page_size: int) -> list[dict]:
    """Fetch articles for multiple queries, combining results."""
    collected = []
    per_query = max(12, page_size // max(len(queries), 1))
    for query in queries:
        collected.extend(_fetch_from_providers(query, page_size=per_query))
        deduped = _dedupe(collected)
        if len(deduped) >= page_size:
            return deduped[:page_size]
        collected = deduped
    return _dedupe(collected)[:page_size]


def probe_provider(provider: str, query: str, page_size: int = 10) -> dict:
    started = datetime.now(timezone.utc)
    try:
        articles = _dedupe(_provider_fetch(provider, query, page_size=page_size))
        return {
            "provider": provider,
            "query": query,
            "requested": page_size,
            "status": "ok" if articles else "empty",
            "article_count": len(articles),
            "sample_titles": [
                article.get("title", "Untitled") for article in articles[:5]
            ],
            "sample_urls": [article.get("url", "") for article in articles[:3]],
            "latest_published_at": max(
                (article.get("published_at", "") for article in articles), default=None
            ),
            "checked_at": started.isoformat(),
            "error": None,
        }
    except Exception as exc:
        return {
            "provider": provider,
            "query": query,
            "requested": page_size,
            "status": "error",
            "article_count": 0,
            "sample_titles": [],
            "sample_urls": [],
            "latest_published_at": None,
            "checked_at": started.isoformat(),
            "error": str(exc),
        }


def probe_sources(query: str, page_size: int = 10) -> dict:
    gdelt = probe_provider("gdelt", query, page_size=page_size)
    directfeeds = probe_provider("directfeeds", query, page_size=page_size)
    newsapi = probe_provider("newsapi", query, page_size=page_size)
    return {
        "query": query,
        "page_size": page_size,
        "preferred": os.getenv("OTHELLO_SOURCE_PROVIDER", "auto").lower(),
        "providers": {
            "gdelt": gdelt,
            "directfeeds": directfeeds,
            "newsapi": newsapi,
        },
    }


def fetch_articles(topic: str, page_size: int = 50) -> list[dict]:
    from providers.directfeeds import _articles_from_direct_feeds
    
    queries = _normalize_topic_queries(topic)
    collected = _collect_query_batch(queries, page_size=page_size)
    direct_feed_articles = _articles_from_direct_feeds(
        topic=topic, page_size=max(12, page_size // 2)
    )
    deduped = _dedupe(collected + direct_feed_articles)
    
    if not deduped:
        from classification.topics import TOPIC_KEYWORDS
        keyword_fallback = [
            " OR ".join(list(TOPIC_KEYWORDS.get(topic, []))[:5]),
            " OR ".join(list(TOPIC_KEYWORDS.get(topic, []))[5:10]),
        ]
        deduped = _dedupe(
            _collect_query_batch(
                [query for query in keyword_fallback if query.strip()],
                page_size=page_size,
            )
            + direct_feed_articles
        )
    
    if ENABLE_NEWSAPI_FALLBACK:
        deduped = _dedupe(
            deduped
            + fetch_articles_from_provider(
                topic, "newsapi", page_size=max(12, page_size // 2)
            )
        )
    
    deduped.sort(
        key=lambda article: (
            -article_quality_score(article, [topic]),
            article.get("published_at", ""),
        ),
        reverse=False,
    )
    ranked = list(reversed(deduped))
    return diversify_articles(
        ranked, page_size=page_size, topics=[topic], max_per_domain=2
    )


def fetch_articles_from_provider(
    topic: str, provider: str, page_size: int = 50
) -> list[dict]:
    from providers.directfeeds import _articles_from_direct_feeds
    
    if provider == "directfeeds":
        return _articles_from_direct_feeds(topic=topic, page_size=page_size)

    queries = _normalize_topic_queries(topic)
    collected = []
    per_query = max(8, page_size // max(len(queries), 1))
    for query in queries:
        try:
            collected.extend(
                _dedupe(_provider_fetch(provider, query, page_size=per_query))
            )
        except Exception as exc:
            print(
                f"[news] Provider '{provider}' topic fetch failed for '{topic}': {exc}"
            )
    
    deduped = _dedupe(collected)
    deduped.sort(
        key=lambda article: (
            -article_quality_score(article, [topic]),
            article.get("published_at", ""),
        ),
        reverse=False,
    )
    ranked = list(reversed(deduped))
    return diversify_articles(
        ranked, page_size=page_size, topics=[topic], max_per_domain=2
    )


def fetch_articles_for_query(question: str, page_size: int = 20) -> list[dict]:
    stop_words = {
        "what",
        "why",
        "how",
        "when",
        "where",
        "who",
        "is",
        "are",
        "the",
        "a",
        "an",
        "do",
        "does",
        "will",
        "can",
        "should",
        "tell",
        "me",
        "about",
        "explain",
        "describe",
        "think",
    }
    words = question.lower().replace("?", "").replace(",", "").split()
    key_terms = [word for word in words if word not in stop_words and len(word) > 3]
    query = " OR ".join(key_terms[:6]) if key_terms else question
    return _fetch_from_providers(query, page_size=page_size)


def fetch_global_articles(page_size: int = 90) -> list[dict]:
    from providers.directfeeds import _articles_from_direct_feeds
    
    deduped = _collect_query_batch(GLOBAL_INGEST_QUERIES, page_size=page_size)
    direct_feed_articles = _articles_from_direct_feeds(
        topic=None, page_size=max(16, page_size // 2)
    )
    deduped = _dedupe(deduped + direct_feed_articles)
    
    if deduped:
        return diversify_articles(deduped, page_size=page_size, max_per_domain=2)

    topic_queries = []
    for topic in TOPIC_QUERIES:
        topic_queries.extend(_normalize_topic_queries(topic))
    deduped = _dedupe(
        _collect_query_batch(topic_queries, page_size=page_size) + direct_feed_articles
    )
    
    if ENABLE_NEWSAPI_FALLBACK:
        deduped = _dedupe(
            deduped
            + fetch_global_articles_from_provider(
                "newsapi", page_size=max(20, page_size // 2)
            )
        )
    
    return diversify_articles(deduped, page_size=page_size, max_per_domain=2)


def fetch_global_articles_from_provider(
    provider: str, page_size: int = 90
) -> list[dict]:
    from providers.directfeeds import _articles_from_direct_feeds
    
    if provider == "directfeeds":
        return _articles_from_direct_feeds(topic=None, page_size=page_size)

    collected = []
    per_query = max(10, page_size // max(len(GLOBAL_INGEST_QUERIES), 1))
    for query in GLOBAL_INGEST_QUERIES:
        try:
            collected.extend(
                _dedupe(_provider_fetch(provider, query, page_size=per_query))
            )
        except Exception as exc:
            print(f"[news] Provider '{provider}' global fetch failed: {exc}")
    
    deduped = _dedupe(collected)
    deduped.sort(
        key=lambda article: (
            -article_quality_score(article),
            article.get("published_at", ""),
        ),
        reverse=False,
    )
    ranked = list(reversed(deduped))
    return diversify_articles(ranked, page_size=page_size, max_per_domain=2)

