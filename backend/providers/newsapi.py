import os
from newsapi import NewsApiClient

from providers.base import (
    get_http_session,
    _cooldown_active,
    _set_provider_cooldown,
    _is_rate_limit_error,
    _is_timeout_error,
)
from normalization.articles import _normalize_article

TRUSTED_SOURCES = ",".join(
    [
        "reuters",
        "associated-press",
        "bbc-news",
        "the-guardian-uk",
        "financial-times",
        "the-economist",
        "bloomberg",
        "the-wall-street-journal",
        "the-new-york-times",
        "the-washington-post",
        "foreign-policy",
        "al-jazeera-english",
        "axios",
        "the-hill",
        "time",
        "newsweek",
    ]
)

TRUSTED_SOURCE_LIST = [
    source.strip() for source in TRUSTED_SOURCES.split(",") if source.strip()
]

ENABLE_NEWSAPI_FALLBACK = (
    os.getenv("OTHELLO_ENABLE_NEWSAPI_FALLBACK", "false").lower() == "true"
)

_newsapi = None


def get_news_client():
    global _newsapi
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        raise RuntimeError("NEWS_API_KEY is missing.")
    if _newsapi is None:
        _newsapi = NewsApiClient(api_key=api_key)
    return _newsapi


def _normalize_newsapi_articles(response: dict) -> list[dict]:
    articles = []
    for article in response.get("articles", []):
        if not article.get("title") or not article.get("url"):
            continue
        if article["title"] == "[Removed]":
            continue
        article_record = _normalize_article(
            title=article["title"],
            description=article.get("description"),
            source=(article.get("source") or {}).get("name", ""),
            url=article["url"],
            published_at=article.get("publishedAt") or "",
            language=article.get("language") or "en",
            provider="newsapi",
        )
        articles.append(article_record)
    return articles


def _fetch_newsapi(query: str, page_size: int) -> list[dict]:
    from news import _dedupe
    
    if not ENABLE_NEWSAPI_FALLBACK:
        return []
    if _cooldown_active("newsapi"):
        return []

    client = get_news_client()
    if not TRUSTED_SOURCE_LIST:
        return []

    collected = []
    chunk_size = 4
    per_chunk = max(4, min(10, page_size // 3 or 4))
    for index in range(0, len(TRUSTED_SOURCE_LIST), chunk_size):
        source_chunk = TRUSTED_SOURCE_LIST[index : index + chunk_size]
        try:
            response = client.get_everything(
                q=query,
                sources=",".join(source_chunk),
                language="en",
                sort_by="publishedAt",
                page_size=per_chunk,
            )
            collected.extend(_normalize_newsapi_articles(response))
        except Exception as exc:
            print(f"[news] NewsAPI chunk fetch failed for {source_chunk}: {exc}")
            if _is_rate_limit_error(exc):
                _set_provider_cooldown("newsapi", 12 * 60 * 60)
                break
            if _is_timeout_error(exc):
                _set_provider_cooldown("newsapi", 30 * 60)
    return _dedupe(collected)
