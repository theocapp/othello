import os
import re
import json
import ssl
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

from providers.base import (
    get_http_session,
    _cooldown_active,
    _set_provider_cooldown,
    _mark_provider_success,
    _mark_provider_failure,
    _respect_provider_min_interval,
    _is_rate_limit_error,
    _is_timeout_error,
)
from normalization.articles import _normalize_article

try:
    from langdetect import LangDetectException, detect
except Exception:  # pragma: no cover - optional runtime dependency
    LangDetectException = Exception
    detect = None

GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_FALLBACK_ENDPOINTS = [
    "https://api.gdeltproject.org/api/v2/doc/doc",
    "http://api.gdeltproject.org/api/v2/doc/doc",
]
GDELT_MAX_RECORDS = 100
GDELT_ARCHIVE_MIN_WINDOW_HOURS = 3


def _normalize_gdelt_query(query: str) -> str:
    text = (query or "").strip()
    if not text:
        return text
    if " OR " in text.upper() and not (text.startswith("(") and text.endswith(")")):
        return f"({text})"
    return text


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
    language_name_map = {
        "arabic": "ar",
        "bulgarian": "bg",
        "chinese": "zh",
        "english": "en",
        "french": "fr",
        "german": "de",
        "hindi": "hi",
        "japanese": "ja",
        "korean": "ko",
        "norwegian": "no",
        "polish": "pl",
        "portuguese": "pt",
        "romanian": "ro",
        "russian": "ru",
        "spanish": "es",
        "turkish": "tr",
    }

    def _normalize_language_value(value: str | None) -> str | None:
        from normalization.articles import ENGLISH_LANGUAGE_CODES
        
        text = str(value or "").strip()
        if not text:
            return None
        lowered = text.lower().replace("_", "-")
        base = lowered.split("-", 1)[0]
        if base in ENGLISH_LANGUAGE_CODES:
            return "en"
        if re.fullmatch(r"[a-z]{2,3}", base):
            return base
        return language_name_map.get(lowered)

    def _extract_translation_language(value) -> str | None:
        if isinstance(value, dict):
            for key in (
                "sourceLanguage",
                "source_language",
                "language",
                "fromLanguage",
                "from_language",
            ):
                normalized = _normalize_language_value(value.get(key))
                if normalized:
                    return normalized
            return None
        text = str(value or "")
        if not text:
            return None
        for token in re.findall(r"[A-Za-z]{2,12}", text):
            normalized = _normalize_language_value(token)
            if normalized:
                return normalized
        return None

    def _gdelt_language(article: dict) -> str:
        from normalization.articles import ENGLISH_LANGUAGE_CODES
        
        for key in (
            "language",
            "Language",
            "sourceLanguage",
            "source_language",
        ):
            normalized = _normalize_language_value(article.get(key))
            if normalized:
                return normalized

        for key in ("TranslationInfo", "translationinfo", "translationInfo"):
            normalized = _extract_translation_language(article.get(key))
            if normalized:
                return normalized

        for key in ("Extras", "extras"):
            extras = article.get(key)
            if isinstance(extras, dict):
                normalized = _extract_translation_language(extras)
                if normalized:
                    return normalized
                for subkey in (
                    "language",
                    "Language",
                    "sourceLanguage",
                    "source_language",
                ):
                    normalized = _normalize_language_value(extras.get(subkey))
                    if normalized:
                        return normalized

        title = (article.get("title") or "").strip()
        if detect and title:
            try:
                detected = _normalize_language_value(detect(title))
                if detected:
                    return detected
            except LangDetectException:
                pass

        return "unknown"

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
        language = _gdelt_language(article)
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


def fetch_gdelt_historic_articles(
    topic: str,
    start: datetime,
    end: datetime,
    page_size: int = GDELT_MAX_RECORDS,
    min_window_hours: int = GDELT_ARCHIVE_MIN_WINDOW_HOURS,
) -> list[dict]:
    from news import _archive_queries_for_topic, _dedupe
    from ranking.article_quality import diversify_articles
    
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
