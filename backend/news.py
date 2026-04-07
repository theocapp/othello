import json
import os
import re
import ssl
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

from dotenv import load_dotenv
from newsapi import NewsApiClient
import requests
from source_catalog import SOURCE_PACKS, SOURCE_SEEDS, source_pack_for

load_dotenv(Path(__file__).with_name(".env"))

ENGLISH_LANGUAGE_CODES = {
    "en",
    "eng",
    "english",
    "en-us",
    "en-gb",
}

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

TOPIC_KEYWORDS = {
    "geopolitics": {
        "war",
        "conflict",
        "military",
        "sanctions",
        "diplomacy",
        "ceasefire",
        "nato",
        "iran",
        "israel",
        "ukraine",
        "russia",
        "china",
        "taiwan",
        "missile",
        "strike",
    },
    "economics": {
        "inflation",
        "market",
        "markets",
        "rates",
        "tariffs",
        "trade",
        "economy",
        "economic",
        "recession",
        "yield",
        "yields",
        "fed",
        "federal",
        "reserve",
        "stocks",
        "oil",
        "gdp",
    },
}

LOW_SIGNAL_PATTERNS = [
    r"\bhow to\b",
    r"\bwhat is\b",
    r"\bwhat are\b",
    r"\bwhen asked\b",
    r"\btalking about\b",
    r"\bbest\b",
    r"\btop \d+\b",
    r"\bquiz\b",
    r"\bopinion\b",
    r"\breview\b",
    r"\blive updates?\b",
]

REGISTERED_ARTICLE_DOMAINS = {
    (seed.get("source_domain") or "").lower()
    for seed in SOURCE_SEEDS
    if seed.get("source_type") == "article" and seed.get("source_domain")
}

SOURCE_METADATA_BY_DOMAIN = {
    (seed.get("source_domain") or "").lower(): seed
    for seed in SOURCE_SEEDS
    if seed.get("source_domain")
}

DOMINANT_GLOBAL_DOMAINS = {
    "www.aljazeera.com",
    "aljazeera.com",
    "bbc.co.uk",
    "www.bbc.co.uk",
    "apnews.com",
    "reuters.com",
}

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

GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_FALLBACK_ENDPOINTS = [
    "https://api.gdeltproject.org/api/v2/doc/doc",
    "http://api.gdeltproject.org/api/v2/doc/doc",
]

HTML_BLOCK_TAGS_RE = re.compile(
    r"</?(?:p|div|br|li|ul|ol|h[1-6]|blockquote|tr|td|th)[^>]*>", re.IGNORECASE
)
HTML_TAGS_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
GDELT_MAX_RECORDS = 100
GDELT_ARCHIVE_MIN_WINDOW_HOURS = 3
ENABLE_NEWSAPI_FALLBACK = (
    os.getenv("OTHELLO_ENABLE_NEWSAPI_FALLBACK", "false").lower() == "true"
)
_provider_cooldowns: dict[str, float] = {}
_provider_last_request_at: dict[str, float] = {}
_provider_failures: dict[str, int] = {}

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
_newsapi = None
_http = None


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


def get_news_client():
    global _newsapi
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        raise RuntimeError("NEWS_API_KEY is missing.")
    if _newsapi is None:
        _newsapi = NewsApiClient(api_key=api_key)
    return _newsapi


def get_http_session():
    global _http
    if _http is None:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "OthelloV2/1.0 (+local intelligence dashboard)",
                "Accept": "application/json,text/plain,*/*",
            }
        )
        _http = session
    return _http


def _cooldown_active(provider: str) -> bool:
    return _provider_cooldowns.get(provider, 0) > time.time()


def _set_provider_cooldown(provider: str, seconds: int) -> None:
    _provider_cooldowns[provider] = max(
        _provider_cooldowns.get(provider, 0), time.time() + max(seconds, 0)
    )


def _mark_provider_success(provider: str) -> None:
    _provider_failures.pop(provider, None)
    _provider_cooldowns.pop(provider, None)


def _mark_provider_failure(provider: str) -> int:
    failures = _provider_failures.get(provider, 0) + 1
    _provider_failures[provider] = failures
    return failures


def _normalize_gdelt_query(query: str) -> str:
    text = (query or "").strip()
    if not text:
        return text
    if " OR " in text.upper() and not (text.startswith("(") and text.endswith(")")):
        return f"({text})"
    return text


def _is_rate_limit_error(exc: Exception | str) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in [
            "rate limit",
            "ratelimit",
            "too many requests",
            "429",
            "please limit requests to one every 5 seconds",
        ]
    )


def _is_timeout_error(exc: Exception | str) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in ["timed out", "timeout", "connect timeout", "read timeout"]
    )


def _gdelt_cooldown_seconds(exc: Exception | str) -> int:
    message = str(exc).lower()
    if "please limit requests to one every 5 seconds" in message:
        return 60
    if "429" in message or "too many requests" in message or "rate limit" in message:
        return 15 * 60
    return 20 * 60


def _gdelt_retry_after_seconds(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return None


def _gdelt_rate_limit_cooldown(exc: Exception | str) -> int:
    base = _gdelt_cooldown_seconds(exc)
    retry_after = (
        _gdelt_retry_after_seconds(exc) if isinstance(exc, Exception) else None
    )
    if retry_after:
        base = max(base, retry_after)
    failures = _mark_provider_failure("gdelt")
    return min(6 * 60 * 60, base + ((failures - 1) * 15 * 60))


def _respect_provider_min_interval(provider: str, default_seconds: float) -> None:
    env_name = f"OTHELLO_{provider.upper()}_MIN_INTERVAL_SECONDS"
    min_interval = float(os.getenv(env_name, str(default_seconds)))
    last_request_at = _provider_last_request_at.get(provider)
    if last_request_at is not None:
        wait_seconds = min_interval - (time.time() - last_request_at)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
    _provider_last_request_at[provider] = time.time()


def _normalize_text_fragment(text: str | None) -> str:
    if text is None:
        return ""
    clean = unescape(text)
    clean = HTML_BLOCK_TAGS_RE.sub(" ", clean)
    clean = HTML_TAGS_RE.sub(" ", clean)
    clean = clean.replace("\xa0", " ")
    return WHITESPACE_RE.sub(" ", clean).strip()


def _trim_summary(text: str, limit: int = 200) -> str:
    clean = _normalize_text_fragment(text)
    if len(clean) <= limit:
        return clean
    sentence_cutoff = max(120, limit - 40)
    for marker in (". ", "! ", "? ", "; "):
        boundary = clean.rfind(marker, 0, limit + 1)
        if boundary >= sentence_cutoff:
            return clean[: boundary + 1].strip()
    boundary = clean.rfind(" ", 0, limit - 1)
    if boundary <= 0:
        boundary = limit - 1
    return clean[:boundary].rstrip(" ,;:") + "…"


def normalize_article_title(title: str | None) -> str:
    return _normalize_text_fragment(title)


def normalize_article_description(
    description: str | None, title: str | None = None, limit: int | None = None
) -> str:
    clean_title = normalize_article_title(title)
    clean_description = _normalize_text_fragment(description)
    if clean_title and clean_description.startswith(clean_title):
        remainder = clean_description[len(clean_title) :].lstrip(" :-|")
        if remainder:
            clean_description = remainder
    if not clean_description:
        clean_description = clean_title
    if limit is not None and clean_description:
        clean_description = _trim_summary(clean_description, limit=limit)
    return clean_description


def _feed_entry_text(node, tag_names: list[str]) -> str | None:
    for tag_name in tag_names:
        match = node.find(tag_name)
        if match is not None:
            text = "".join(match.itertext()).strip()
            if text:
                return text
    return None


def _normalize_feed_timestamp(raw: str | None) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:
        return _normalize_time(raw)


def _parse_feed_entries(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    entries = []
    for item in root.findall(".//item"):
        entries.append(
            {
                "title": _feed_entry_text(item, ["title"]),
                "url": _feed_entry_text(item, ["link"]),
                "description": _feed_entry_text(item, ["description"]),
                "published_at": _normalize_feed_timestamp(
                    _feed_entry_text(item, ["pubDate", "published", "updated"])
                ),
                "language": _feed_entry_text(item, ["language"]),
            }
        )
    for item in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        link = item.find("{http://www.w3.org/2005/Atom}link")
        href = link.attrib.get("href") if link is not None else None
        entries.append(
            {
                "title": _feed_entry_text(item, ["{http://www.w3.org/2005/Atom}title"]),
                "url": href,
                "description": _feed_entry_text(
                    item,
                    [
                        "{http://www.w3.org/2005/Atom}summary",
                        "{http://www.w3.org/2005/Atom}content",
                    ],
                ),
                "published_at": _normalize_feed_timestamp(
                    _feed_entry_text(
                        item,
                        [
                            "{http://www.w3.org/2005/Atom}updated",
                            "{http://www.w3.org/2005/Atom}published",
                        ],
                    )
                ),
                "language": item.attrib.get(
                    "{http://www.w3.org/XML/1998/namespace}lang"
                ),
            }
        )
    return [entry for entry in entries if entry.get("title") and entry.get("url")]


def _direct_feed_sources(topic: str | None = None) -> list[dict]:
    sources = []
    for seed in SOURCE_SEEDS:
        metadata = seed.get("metadata") or {}
        if seed.get("source_type") != "article" or metadata.get("adapter") != "rss":
            continue
        feeds = metadata.get("feeds") or []
        filtered_feeds = []
        for feed in feeds:
            hints = set(feed.get("topic_hints") or [])
            if topic and hints and topic not in hints:
                continue
            filtered_feeds.append(feed)
        if topic and not filtered_feeds:
            continue
        if filtered_feeds:
            sources.append({**seed, "metadata": {**metadata, "feeds": filtered_feeds}})
    if topic == "geopolitics":
        pack_order = {
            "conflict_region_outlets": 0,
            "regional_flagships": 1,
            "global_wires": 2,
        }
    elif topic == "economics":
        pack_order = {
            "global_wires": 0,
            "regional_flagships": 1,
            "conflict_region_outlets": 2,
        }
    else:
        pack_order = {
            "global_wires": 0,
            "regional_flagships": 1,
            "conflict_region_outlets": 2,
        }
    sources.sort(
        key=lambda source: (
            pack_order.get(source_pack_for(source) or "", 99),
            source.get("source_name", ""),
        )
    )
    return sources


def _articles_from_direct_feeds(
    topic: str | None = None, page_size: int = 40
) -> list[dict]:
    if _cooldown_active("directfeeds"):
        return []

    session = get_http_session()
    sources = _direct_feed_sources(topic=topic)
    if not sources:
        return []

    collected = []
    feed_errors = 0
    total_feeds = sum(
        len((source.get("metadata") or {}).get("feeds") or []) for source in sources
    )
    per_feed_limit = max(4, min(10, page_size // max(total_feeds, 1) + 1))

    for source in sources:
        metadata = source.get("metadata") or {}
        for feed in metadata.get("feeds") or []:
            try:
                response = session.get(feed["url"], timeout=18)
                response.raise_for_status()
                entries = _parse_feed_entries(response.text)[:per_feed_limit]
                for entry in entries:
                    collected.append(
                        _normalize_article(
                            title=entry["title"],
                            description=entry.get("description"),
                            source=source["source_name"],
                            url=entry["url"],
                            published_at=entry["published_at"],
                            language=entry.get("language")
                            or source.get("language")
                            or "en",
                            provider="directfeeds",
                        )
                    )
            except Exception as exc:
                feed_errors += 1
                print(
                    f"[news] Direct feed fetch failed for {source['source_name']} ({feed.get('url')}): {exc}"
                )

    if feed_errors and feed_errors >= max(3, total_feeds):
        _set_provider_cooldown("directfeeds", 15 * 60)

    deduped = _dedupe(collected)
    return diversify_articles(
        deduped,
        page_size=page_size,
        topics=[topic] if topic else None,
        max_per_domain=2,
    )


def _normalize_time(raw: str | None) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    text = raw.strip()
    if text.endswith("Z"):
        return text
    if len(text) == 14 and text.isdigit():
        return (
            datetime.strptime(text, "%Y%m%d%H%M%S")
            .replace(tzinfo=timezone.utc)
            .isoformat()
        )
    return text


def _normalize_article(
    title: str,
    description: str | None,
    source: str,
    url: str,
    published_at: str,
    language: str = "en",
    provider: str = "unknown",
) -> dict:
    language_text = (language or "en").strip()
    clean_title = normalize_article_title(title)
    clean_description = normalize_article_description(description, clean_title)
    return {
        "title": clean_title,
        "description": clean_description or clean_title,
        "source": (source or urlparse(url).netloc or "Unknown source").strip(),
        "source_domain": urlparse(url).netloc.lower(),
        "url": url.strip(),
        "published_at": _normalize_time(published_at),
        "language": language_text,
        "provider": provider,
    }


def is_english_article(article: dict) -> bool:
    language = str(article.get("language") or "").strip().lower()
    if not language:
        return True
    return language in ENGLISH_LANGUAGE_CODES


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


def _gdelt_params(query: str, maxrecords: int) -> dict:
    return {
        "query": _normalize_gdelt_query(query),
        "mode": "artlist",
        "format": "json",
        "maxrecords": min(maxrecords, GDELT_MAX_RECORDS),
        "sort": "datedesc",
        "timespan": os.getenv("OTHELLO_GDELT_TIMESPAN", "1week"),
    }


def _gdelt_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def _gdelt_window_params(
    query: str, start: datetime, end: datetime, maxrecords: int
) -> dict:
    return {
        "query": _normalize_gdelt_query(query),
        "mode": "artlist",
        "format": "json",
        "maxrecords": min(maxrecords, GDELT_MAX_RECORDS),
        "sort": "datedesc",
        "startdatetime": _gdelt_datetime(start),
        "enddatetime": _gdelt_datetime(end),
    }


def _fetch_gdelt_payload(params: dict) -> dict:
    if _cooldown_active("gdelt"):
        raise RuntimeError("GDELT is in cooldown after recent upstream failures.")

    allow_insecure = os.getenv("OTHELLO_ALLOW_INSECURE_GDELT", "true").lower() == "true"
    errors = []
    payload = None

    for endpoint in GDELT_FALLBACK_ENDPOINTS:
        try:
            _respect_provider_min_interval("gdelt", default_seconds=8.5)
            response = get_http_session().get(
                endpoint,
                params=params,
                timeout=20,
                verify=not allow_insecure,
            )
            response.raise_for_status()
            text = response.text.lstrip()
            if text.lower().startswith("please limit requests to one every 5 seconds"):
                raise RuntimeError(text.strip())
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type.lower() and not text.startswith("{"):
                raise ValueError(
                    f"GDELT returned unexpected content type: {content_type or 'unknown'}"
                )
            payload = response.json()
            _mark_provider_success("gdelt")
            break
        except Exception as exc:
            if _is_rate_limit_error(exc):
                _set_provider_cooldown("gdelt", _gdelt_rate_limit_cooldown(exc))
                errors.append(f"{endpoint}: {exc}")
                break
            elif _is_timeout_error(exc):
                _mark_provider_failure("gdelt")
                _set_provider_cooldown("gdelt", 20 * 60)
            errors.append(f"{endpoint}: {exc}")

    if payload is None:
        if errors and any(_is_rate_limit_error(error) for error in errors):
            raise RuntimeError(" | ".join(errors))
        # Fallback to stdlib urlopen as a last resort in case requests-specific TLS/proxy handling is the problem.
        context = ssl.create_default_context()
        if allow_insecure:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        url = f"{GDELT_ENDPOINT}?{urlencode(params)}"
        try:
            _respect_provider_min_interval("gdelt", default_seconds=8.5)
            with urlopen(url, timeout=20, context=context) as response:
                raw = response.read().decode("utf-8", errors="replace")
            if (
                raw.lstrip()
                .lower()
                .startswith("please limit requests to one every 5 seconds")
            ):
                raise RuntimeError(raw.strip())
            payload = json.loads(raw)
            _mark_provider_success("gdelt")
        except Exception as exc:
            if _is_rate_limit_error(exc):
                _set_provider_cooldown("gdelt", _gdelt_rate_limit_cooldown(exc))
            elif _is_timeout_error(exc):
                _mark_provider_failure("gdelt")
                _set_provider_cooldown("gdelt", 20 * 60)
            errors.append(f"{GDELT_ENDPOINT} (urlopen): {exc}")
            raise RuntimeError(" | ".join(errors))

    return payload


def _normalize_gdelt_payload(payload: dict) -> list[dict]:
    articles = []
    for article in payload.get("articles", []):
        title = article.get("title") or ""
        url = article.get("url") or ""
        if not title or not url:
            continue

        description = (
            article.get("snippet")
            or article.get("description")
            or article.get("excerpt")
            or article.get("title")
        )
        source = article.get("domain") or article.get("source") or urlparse(url).netloc
        published_at = (
            article.get("seendate") or article.get("published") or article.get("date")
        )
        language = article.get("language") or "English"
        article_record = _normalize_article(
            title=title,
            description=description,
            source=source,
            url=url,
            published_at=published_at or "",
            language=language,
            provider="gdelt",
        )
        articles.append(article_record)
    return articles


def _fetch_gdelt(query: str, maxrecords: int) -> list[dict]:
    payload = _fetch_gdelt_payload(_gdelt_params(query, maxrecords=maxrecords))
    return _normalize_gdelt_payload(payload)


def _fetch_gdelt_window(
    query: str, start: datetime, end: datetime, maxrecords: int = GDELT_MAX_RECORDS
) -> list[dict]:
    payload = _fetch_gdelt_payload(
        _gdelt_window_params(query, start=start, end=end, maxrecords=maxrecords)
    )
    return _normalize_gdelt_payload(payload)


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


def _fetch_newsapi(query: str, page_size: int) -> list[dict]:
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


def _provider_fetch(provider: str, query: str, page_size: int) -> list[dict]:
    if provider == "gdelt":
        return _fetch_gdelt(query, maxrecords=page_size)
    if provider == "directfeeds":
        inferred_topic = (
            "economics"
            if "inflation" in query.lower()
            or "rates" in query.lower()
            or "econom" in query.lower()
            else "geopolitics"
        )
        return _articles_from_direct_feeds(topic=inferred_topic, page_size=page_size)
    if provider == "newsapi":
        return _fetch_newsapi(query, page_size=page_size)
    raise ValueError(f"Unknown provider: {provider}")


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


def _fetch_from_providers(query: str, page_size: int) -> list[dict]:
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
    collected = []
    per_query = max(12, page_size // max(len(queries), 1))
    for query in queries:
        collected.extend(_fetch_from_providers(query, page_size=per_query))
        deduped = _dedupe(collected)
        if len(deduped) >= page_size:
            return deduped[:page_size]
        collected = deduped
    return _dedupe(collected)[:page_size]


def _normalize_topic_queries(topic: str) -> list[str]:
    configured = TOPIC_QUERIES.get(topic, topic)
    if isinstance(configured, list):
        return configured
    return [configured]


def _archive_queries_for_topic(topic: str) -> list[str]:
    return TOPIC_ARCHIVE_QUERIES.get(topic, _normalize_topic_queries(topic))


def _score_article(article: dict, topic: str) -> int:
    haystack = " ".join(
        [
            article.get("title", ""),
            article.get("description", ""),
            article.get("source", ""),
        ]
    ).lower()
    score = 0
    raw_terms = (
        " ".join(_normalize_topic_queries(topic)).replace('"', " ").replace("OR", " ")
    )
    for token in raw_terms.lower().split():
        clean = token.strip()
        if not clean or clean in {"or"} or len(clean) < 4:
            continue
        if clean in haystack:
            score += 1
    return score


def article_quality_score(article: dict, topics: list[str] | None = None) -> int:
    text = " ".join(
        [
            article.get("title", ""),
            article.get("description", ""),
            article.get("source", ""),
        ]
    ).strip()
    haystack = text.lower()
    title = (article.get("title") or "").strip()
    description = (article.get("description") or "").strip()
    source_domain = (article.get("source_domain") or "").strip().lower()
    source_meta = SOURCE_METADATA_BY_DOMAIN.get(source_domain, {})

    score = 0

    if source_domain in REGISTERED_ARTICLE_DOMAINS:
        score += 3
    if source_meta.get("region") and source_meta.get("region") not in {
        "global",
        "united-states",
        "europe",
    }:
        score += 2
    if source_meta.get("trust_tier") == "tier_2":
        score += 1
    if title and 24 <= len(title) <= 180:
        score += 2
    if description and description != title and len(description) >= 48:
        score += 2
    if article.get("published_at"):
        score += 1
    if is_english_article(article):
        score += 1
    elif article.get("translated_title") or article.get("translated_description"):
        score += 2
    elif article.get("language"):
        score += 1

    for pattern in LOW_SIGNAL_PATTERNS:
        if re.search(pattern, haystack):
            score -= 4

    if len(title.split()) < 5:
        score -= 2
    if description == title and len(description) < 80:
        score -= 2

    topic_list = topics or infer_article_topics(article)
    topic_bonus = 0
    for topic in topic_list:
        topic_bonus = max(topic_bonus, _score_article(article, topic))
    score += topic_bonus
    if source_domain in DOMINANT_GLOBAL_DOMAINS:
        score -= 1
    return score


def should_promote_article(article: dict, topics: list[str] | None = None) -> bool:
    topic_list = topics or infer_article_topics(article)
    quality = article_quality_score(article, topic_list)
    source_domain = (article.get("source_domain") or "").strip().lower()
    source_meta = SOURCE_METADATA_BY_DOMAIN.get(source_domain, {})

    if not topic_list:
        return False
    if quality >= 7:
        return True
    if source_domain in REGISTERED_ARTICLE_DOMAINS and quality >= 5:
        return True
    if (
        source_meta.get("region")
        and source_meta.get("region") not in {"global", "united-states", "europe"}
        and quality >= 4
    ):
        return True
    if not is_english_article(article) and quality >= 5:
        return True
    return False


def diversify_articles(
    articles: list[dict],
    page_size: int,
    topics: list[str] | None = None,
    max_per_domain: int = 2,
) -> list[dict]:
    if not articles:
        return []

    ranked = sorted(
        articles,
        key=lambda article: (
            -article_quality_score(article, topics),
            -int(
                (
                    SOURCE_METADATA_BY_DOMAIN.get(
                        (article.get("source_domain") or "").strip().lower(), {}
                    )
                    or {}
                ).get("region")
                not in {"", "global", "united-states", "europe"}
            ),
            -int(not is_english_article(article)),
            article.get("published_at", ""),
        ),
        reverse=False,
    )
    ranked = list(reversed(ranked))

    selected = []
    overflow = []
    per_domain_counts: dict[str, int] = {}

    for article in ranked:
        domain = (
            (article.get("source_domain") or article.get("source") or "unknown")
            .strip()
            .lower()
        )
        limit = max_per_domain
        if domain in DOMINANT_GLOBAL_DOMAINS:
            limit = min(limit, 1)
        if per_domain_counts.get(domain, 0) < limit:
            selected.append(article)
            per_domain_counts[domain] = per_domain_counts.get(domain, 0) + 1
        else:
            overflow.append(article)
        if len(selected) >= page_size:
            return selected[:page_size]

    if len(selected) >= max(3, page_size // 2):
        return selected[:page_size]

    for article in overflow:
        selected.append(article)
        if len(selected) >= page_size:
            break

    return selected[:page_size]


def fetch_articles(topic: str, page_size: int = 50) -> list[dict]:
    queries = _normalize_topic_queries(topic)
    collected = _collect_query_batch(queries, page_size=page_size)
    direct_feed_articles = _articles_from_direct_feeds(
        topic=topic, page_size=max(12, page_size // 2)
    )
    deduped = _dedupe(collected + direct_feed_articles)
    if not deduped:
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
    if ENABLE_NEWSAPI_FALLBACK and len(deduped) < max(10, page_size // 3):
        deduped = _dedupe(
            deduped
            + fetch_articles_from_provider(
                topic, "newsapi", page_size=max(12, page_size // 2)
            )
        )
    deduped.sort(
        key=lambda article: (
            -article_quality_score(article, [topic]),
            -_score_article(article, topic),
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
            -_score_article(article, topic),
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
    if ENABLE_NEWSAPI_FALLBACK and len(deduped) < max(12, page_size // 3):
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


def fetch_gdelt_historic_articles(
    topic: str,
    start: datetime,
    end: datetime,
    page_size: int = GDELT_MAX_RECORDS,
    min_window_hours: int = GDELT_ARCHIVE_MIN_WINDOW_HOURS,
) -> list[dict]:
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    if end <= start:
        return []

    min_window = timedelta(hours=max(1, min_window_hours))
    queries = _archive_queries_for_topic(topic)
    collected: list[dict] = []

    def harvest(query: str, left: datetime, right: datetime) -> list[dict]:
        articles = _dedupe(
            _fetch_gdelt_window(query, left, right, maxrecords=page_size)
        )
        if len(articles) >= page_size and (right - left) > min_window:
            midpoint = left + ((right - left) / 2)
            return _dedupe(
                harvest(query, left, midpoint) + harvest(query, midpoint, right)
            )
        return articles

    for query in queries:
        collected.extend(harvest(query, start, end))

    deduped = _dedupe(collected)
    deduped.sort(key=lambda article: article.get("published_at", ""), reverse=True)
    return diversify_articles(
        deduped, page_size=len(deduped), topics=[topic], max_per_domain=3
    )


def probe_gdelt_window(
    topic: str, start: datetime, end: datetime, page_size: int = 25
) -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        articles = fetch_gdelt_historic_articles(
            topic=topic,
            start=start,
            end=end,
            page_size=min(page_size, GDELT_MAX_RECORDS),
            min_window_hours=GDELT_ARCHIVE_MIN_WINDOW_HOURS,
        )
        return {
            "topic": topic,
            "status": "ok" if articles else "empty",
            "article_count": len(articles),
            "sample_titles": [
                article.get("title", "Untitled") for article in articles[:5]
            ],
            "latest_published_at": max(
                (article.get("published_at", "") for article in articles), default=None
            ),
            "window_start": start.astimezone(timezone.utc).isoformat(),
            "window_end": end.astimezone(timezone.utc).isoformat(),
            "checked_at": started_at,
            "error": None,
        }
    except Exception as exc:
        return {
            "topic": topic,
            "status": "error",
            "article_count": 0,
            "sample_titles": [],
            "latest_published_at": None,
            "window_start": start.astimezone(timezone.utc).isoformat(),
            "window_end": end.astimezone(timezone.utc).isoformat(),
            "checked_at": started_at,
            "error": str(exc),
        }


def infer_article_topics(article: dict) -> list[str]:
    haystack = " ".join(
        [
            article.get("title", ""),
            article.get("description", ""),
            article.get("source", ""),
        ]
    ).lower()
    scored = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in haystack)
        if score > 0:
            scored.append((topic, score))

    if not scored:
        return []

    scored.sort(key=lambda item: item[1], reverse=True)
    best_topic, best_score = scored[0]

    if best_score < 2 and not (best_topic == "economics" and "market" in haystack):
        return []

    matches = [best_topic]
    for topic, score in scored[1:]:
        if score >= best_score - 1 and score >= 3:
            matches.append(topic)
    return matches
