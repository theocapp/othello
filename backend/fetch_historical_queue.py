import argparse
import json
import re
import time
from collections import defaultdict
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urlparse

import requests

from bootstrap_sources import seed_sources
from cache import init_db as init_cache_db
from corpus import get_historical_url_queue_batch
from corpus import init_db as init_corpus_db
from corpus import update_historical_url_queue_status
from corpus import upsert_articles
from news import infer_article_topics

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)
REQUEST_TIMEOUT_SECONDS = 15
_NON_RETRYABLE_EXTRACTION_ERRORS = (
    "could not extract a title",
    "unsupported content type",
)
DOMAIN_RULES = {
    "reuters.com": {
        "body_markers": [
            "article-body__",
            "article-body",
            "paywall-article",
            "StandardArticleBody_body",
        ],
    },
    "apnews.com": {
        "body_markers": ["RichTextStoryBody", "RichTextBody", "Page-content"],
    },
    "bbc.com": {
        "body_markers": [
            "story-body",
            "article__body-content",
            "ssrcss-",
            "main-content",
        ],
    },
    "bbc.co.uk": {
        "body_markers": [
            "story-body",
            "article__body-content",
            "ssrcss-",
            "main-content",
        ],
    },
    "aljazeera.com": {
        "body_markers": [
            "wysiwyg",
            "article-p-wrapper",
            "main-article-body",
            "article-body",
        ],
    },
    "ft.com": {
        "body_markers": ["article__content-body", "n-content-body", "article-body"],
    },
}


def _domain_rule(domain: str) -> dict:
    normalized = (domain or "").lower()
    for key, value in DOMAIN_RULES.items():
        if normalized == key or normalized.endswith(f".{key}"):
            return value
    return {}


class _ArticleTextParser(HTMLParser):
    def __init__(self, domain: str = "") -> None:
        super().__init__()
        self.title = ""
        self.meta: dict[str, str] = {}
        self._in_title = False
        self._capture_stack: list[str] = []
        self._chunks: list[str] = []
        self._paragraphs: list[str] = []
        self._seen_paragraphs: set[str] = set()
        self._domain_rules = _domain_rule(domain)

    def handle_starttag(self, tag: str, attrs) -> None:
        attr_map = {key.lower(): value for key, value in attrs}
        if tag == "title":
            self._in_title = True
        if tag == "meta":
            key = (attr_map.get("property") or attr_map.get("name") or "").lower()
            content = attr_map.get("content") or ""
            if key and content:
                self.meta[key] = content.strip()
        if tag in {"p", "article", "div", "section"}:
            css = " ".join(
                filter(None, [attr_map.get("class"), attr_map.get("id")])
            ).lower()
            body_markers = [
                marker.lower() for marker in self._domain_rules.get("body_markers", [])
            ]
            is_capture = (
                tag == "p"
                or any(
                    marker in css
                    for marker in ("article", "story", "content", "body", "main")
                )
                or any(marker and marker in css for marker in body_markers)
            )
            if is_capture:
                self._capture_stack.append(tag)
            else:
                self._capture_stack.append("")

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        if tag in {"p", "article", "div", "section"}:
            if not self._capture_stack:
                return
            triggered = self._capture_stack.pop()
            if triggered:
                text = _collapse_whitespace(" ".join(self._chunks))
                if len(text) >= 60 and text not in self._seen_paragraphs:
                    self._paragraphs.append(text)
                    self._seen_paragraphs.add(text)
                self._chunks = []

    def handle_data(self, data: str) -> None:
        text = _collapse_whitespace(data)
        if not text:
            return
        if self._in_title:
            self.title = f"{self.title} {text}".strip()
        if any(self._capture_stack):
            self._chunks.append(text)

    @property
    def body_text(self) -> str:
        return "\n\n".join(self._paragraphs[:12]).strip()


def _collapse_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", unescape(text or "")).strip()


def _extract_json_ld(html: str) -> dict | None:
    for match in re.finditer(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        raw = match.group(1).strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            kind = str(candidate.get("@type") or "").lower()
            if "article" in kind or "report" in kind:
                return candidate
    return None


def _extract_article_from_html(
    html: str, fallback_url: str, fallback_title: str | None = None
) -> dict:
    parser = _ArticleTextParser(urlparse(fallback_url).netloc.lower())
    parser.feed(html)
    json_ld = _extract_json_ld(html) or {}

    title = (
        json_ld.get("headline")
        or parser.meta.get("og:title")
        or parser.meta.get("twitter:title")
        or parser.title
        or fallback_title
        or ""
    )
    description = (
        json_ld.get("description")
        or parser.meta.get("description")
        or parser.meta.get("og:description")
        or parser.meta.get("twitter:description")
        or ""
    )
    published_at = (
        json_ld.get("datePublished")
        or parser.meta.get("article:published_time")
        or parser.meta.get("og:published_time")
        or parser.meta.get("pubdate")
        or ""
    )
    source_name = ""
    publisher = json_ld.get("publisher")
    if isinstance(publisher, dict):
        source_name = publisher.get("name") or ""
    body_text = parser.body_text
    if not description and body_text:
        description = body_text[:400].strip()

    return {
        "url": fallback_url,
        "title": _collapse_whitespace(title),
        "description": _collapse_whitespace(description),
        "published_at": _collapse_whitespace(published_at),
        "source": _collapse_whitespace(source_name)
        or urlparse(fallback_url).netloc.lower(),
        "source_domain": urlparse(fallback_url).netloc.lower(),
        "language": parser.meta.get("og:locale", "").split("_")[0].strip() or None,
        "body_text": body_text,
    }


def _fetch_url(session: requests.Session, url: str) -> tuple[dict | None, str | None]:
    response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True)
    response.raise_for_status()
    content_type = (response.headers.get("content-type") or "").lower()
    if "html" not in content_type:
        return None, f"Unsupported content type: {content_type or 'unknown'}"
    article = _extract_article_from_html(
        response.text, fallback_url=response.url or url
    )
    if not article.get("title"):
        return None, "Could not extract a title from the article"
    return article, None


def _sleep_for_domain(
    last_request_at: dict[str, float], domain: str, min_interval_seconds: float
) -> None:
    previous = last_request_at.get(domain)
    if previous is None:
        return
    elapsed = time.time() - previous
    remaining = min_interval_seconds - elapsed
    if remaining > 0:
        time.sleep(remaining)


def fetch_historical_queue(
    limit: int,
    batch_size: int,
    min_domain_interval_seconds: float,
    max_attempts: int,
    dry_run: bool,
    retry_share: float = 0.2,
) -> dict:
    init_cache_db()
    init_corpus_db()
    seed_sources()

    pending_limit = max(1, int(limit * (1.0 - retry_share)))
    retry_limit = max(0, limit - pending_limit)

    pending_rows = get_historical_url_queue_batch(
        limit=pending_limit, statuses=["pending"]
    )
    retry_rows = (
        get_historical_url_queue_batch(limit=retry_limit, statuses=["retry"])
        if retry_limit > 0
        else []
    )
    queue_rows = pending_rows + retry_rows
    if not queue_rows:
        return {
            "requested": limit,
            "processed": 0,
            "inserted_or_updated": 0,
            "succeeded": 0,
            "retry": 0,
            "no_topic": 0,
            "failed": 0,
            "topics": {},
        }

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    last_request_at: dict[str, float] = {}
    articles_by_topic: dict[str, list[dict]] = defaultdict(list)
    fetched_items: list[dict] = []
    summary = {
        "requested": limit,
        "processed": 0,
        "inserted_or_updated": 0,
        "succeeded": 0,
        "retry": 0,
        "no_topic": 0,
        "failed": 0,
        "topics": defaultdict(int),
    }

    for row in queue_rows:
        url = row["url"]
        domain = (
            row.get("source_domain") or urlparse(url).netloc.lower() or "unknown"
        ).lower()
        attempts = int(row.get("attempt_count") or 0) + 1
        summary["processed"] += 1
        _sleep_for_domain(last_request_at, domain, min_domain_interval_seconds)
        last_request_at[domain] = time.time()

        try:
            article, extraction_error = _fetch_url(session, url)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            retryable = status_code in {408, 425, 429, 500, 502, 503, 504}
            fetch_status = (
                "retry" if retryable and attempts < max_attempts else "failed"
            )
            update_historical_url_queue_status(
                url,
                fetch_status,
                attempt_count=attempts,
                payload_patch={
                    "last_error": f"http {status_code}" if status_code else str(exc),
                    "last_http_status": status_code,
                },
            )
            summary["retry" if fetch_status == "retry" else "failed"] += 1
            continue
        except requests.RequestException as exc:
            fetch_status = "retry" if attempts < max_attempts else "failed"
            update_historical_url_queue_status(
                url,
                fetch_status,
                attempt_count=attempts,
                payload_patch={"last_error": str(exc)},
            )
            summary["retry" if fetch_status == "retry" else "failed"] += 1
            continue

        if extraction_error or not article:
            err_lower = (extraction_error or "").lower()
            non_retryable = any(
                pattern in err_lower for pattern in _NON_RETRYABLE_EXTRACTION_ERRORS
            )
            fetch_status = (
                "failed" if non_retryable or attempts >= max_attempts else "retry"
            )
            update_historical_url_queue_status(
                url,
                fetch_status,
                attempt_count=attempts,
                payload_patch={
                    "last_error": extraction_error or "Unknown extraction failure"
                },
            )
            summary["retry" if fetch_status == "retry" else "failed"] += 1
            continue

        article["source"] = row.get("source_name") or article.get("source") or domain
        article["source_domain"] = (
            row.get("source_domain") or article.get("source_domain") or domain
        )
        article["published_at"] = (
            row.get("published_at") or article.get("published_at") or ""
        )
        article["language"] = row.get("language") or article.get("language") or "en"
        if article["language"] == "en":
            text_sample = " ".join(
                filter(
                    None,
                    [
                        article.get("title", ""),
                        article.get("description", ""),
                    ],
                )
            )
            if text_sample and not text_sample.isascii():
                try:
                    from langdetect import detect

                    detected = detect(text_sample)
                    if detected and detected != "en":
                        article["language"] = detected
                except Exception:
                    pass
        article["body_text"] = article.get("body_text") or ""
        article["payload"] = {
            "historical_queue_url": url,
            "historical_discovered_via": row.get("discovered_via"),
            "historical_topic_guess": row.get("topic_guess"),
        }

        if row.get("topic_guess"):
            inferred = infer_article_topics(article)
            if inferred:
                topics = inferred
                if row["topic_guess"] not in topics:
                    pass
            else:
                topics = [row["topic_guess"]]
        else:
            topics = infer_article_topics(article)
        if not topics:
            topics = (
                ["geopolitics"]
                if "conflict" in (article.get("title") or "").lower()
                else []
            )
        if not topics:
            update_historical_url_queue_status(
                url,
                "no_topic",
                attempt_count=attempts,
                payload_patch={
                    "last_error": "Could not infer a topic for fetched article"
                },
            )
            summary["no_topic"] += 1
            if not dry_run:
                summary["inserted_or_updated"] += upsert_articles(
                    [dict(article)],
                    topic="unclassified",
                    provider="historical-fetch",
                    default_analytic_tier="volume",
                )
            continue

        for topic in topics:
            summary["topics"][topic] += 1
            if not dry_run:
                articles_by_topic[topic].append(dict(article))

        fetched_items.append(
            {
                "url": url,
                "attempts": attempts,
                "article": article,
                "topics": topics,
            }
        )
        summary["succeeded"] += 1

    if dry_run:
        summary["topics"] = dict(summary["topics"])
        return summary

    if not dry_run:
        for topic, topic_articles in articles_by_topic.items():
            for index in range(0, len(topic_articles), max(1, batch_size)):
                batch = topic_articles[index : index + max(1, batch_size)]
                summary["inserted_or_updated"] += upsert_articles(
                    batch,
                    topic=topic,
                    provider="historical-fetch",
                    default_analytic_tier="volume",
                )

    for item in fetched_items:
        update_historical_url_queue_status(
            item["url"],
            "fetched",
            attempt_count=item["attempts"],
            payload_patch={
                "last_error": None,
                "fetched_title": item["article"].get("title"),
                "fetched_source": item["article"].get("source"),
                "fetched_body_chars": len(item["article"].get("body_text") or ""),
                "fetched_topics": item["topics"],
            },
        )

    summary["topics"] = dict(summary["topics"])
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch article bodies from the historical URL queue into the article corpus."
    )
    parser.add_argument(
        "--limit", type=int, default=25, help="Maximum queue rows to process."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for article corpus writes.",
    )
    parser.add_argument(
        "--min-domain-interval",
        type=float,
        default=2.5,
        help="Minimum seconds between requests to the same domain.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Attempts before a queue item is marked failed.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and classify articles without writing them into the article corpus.",
    )
    parser.add_argument(
        "--retry-share",
        type=float,
        default=0.2,
        help="Share of queue slots reserved for retry items.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = fetch_historical_queue(
        limit=max(1, args.limit),
        batch_size=max(1, args.batch_size),
        min_domain_interval_seconds=max(0.0, args.min_domain_interval),
        max_attempts=max(1, args.max_attempts),
        dry_run=args.dry_run,
        retry_share=max(0.0, min(1.0, args.retry_share)),
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
