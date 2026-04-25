import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from urllib.parse import urlparse

try:
    from langdetect import LangDetectException, detect
except Exception:  # pragma: no cover - optional runtime dependency
    LangDetectException = Exception
    detect = None

ENGLISH_LANGUAGE_CODES = {
    "en",
    "eng",
    "english",
    "en-us",
    "en-gb",
}

HTML_BLOCK_TAGS_RE = re.compile(
    r"</?(?:p|div|br|li|ul|ol|h[1-6]|blockquote|tr|td|th)[^>]*>", re.IGNORECASE
)
HTML_TAGS_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")


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


def _normalize_feed_timestamp(raw: str | None) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    text = raw.strip()
    try:
        return parsedate_to_datetime(text).astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except ValueError:
        pass
    return datetime.now(timezone.utc).isoformat()


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
