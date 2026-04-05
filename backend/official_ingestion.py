import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests

from corpus import get_source_registry, record_raw_source_documents, upsert_articles, upsert_official_updates
from news import infer_article_topics, should_promote_article


RELIEFWEB_ENDPOINT = "https://api.reliefweb.int/v2/reports"
RELIEFWEB_APPNAME = os.getenv("OTHELLO_RELIEFWEB_APPNAME", "othello_v2")


class AnchorParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self._href = None
        self._text = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        attr_map = dict(attrs)
        self._href = attr_map.get("href")
        self._text = []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() != "a" or self._href is None:
            return
        text = re.sub(r"\s+", " ", "".join(self._text)).strip()
        self.links.append({"href": self._href, "text": text})
        self._href = None
        self._text = []


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


def _normalize_time(raw: str | None) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    raw = raw.strip()
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    try:
        if raw.endswith("Z"):
            return raw
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return raw


def _hash_id(*parts: str, length: int = 24) -> str:
    return hashlib.sha256(" | ".join(parts).encode("utf-8")).hexdigest()[:length]


def _extract_xml_items(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    items = []
    for item in root.findall(".//item"):
        title = item.findtext("title")
        link = item.findtext("link")
        if not title or not link:
            continue
        items.append(
            {
                "title": title.strip(),
                "url": link.strip(),
                "description": (item.findtext("description") or title).strip(),
                "published_at": _normalize_time(item.findtext("pubDate") or item.findtext("published") or item.findtext("updated")),
                "external_id": (item.findtext("guid") or link).strip(),
            }
        )
    atom_ns = "{http://www.w3.org/2005/Atom}"
    for entry in root.findall(f".//{atom_ns}entry"):
        title = entry.findtext(f"{atom_ns}title")
        link_node = entry.find(f"{atom_ns}link")
        link = link_node.attrib.get("href") if link_node is not None else None
        if not title or not link:
            continue
        items.append(
            {
                "title": title.strip(),
                "url": link.strip(),
                "description": (entry.findtext(f"{atom_ns}summary") or title).strip(),
                "published_at": _normalize_time(entry.findtext(f"{atom_ns}updated") or entry.findtext(f"{atom_ns}published")),
                "external_id": (entry.findtext(f"{atom_ns}id") or link).strip(),
            }
        )
    return items


def _raw_document(source: dict, item: dict, adapter: str, payload: dict) -> dict:
    return {
        "document_id": _hash_id(source["source_id"], item.get("external_id") or item.get("url") or item["title"]),
        "source_id": source["source_id"],
        "external_id": item.get("external_id"),
        "url": item.get("url"),
        "title": item["title"],
        "published_at": item.get("published_at"),
        "fetched_at": time.time(),
        "language": source.get("language") or "en",
        "source_type": source["source_type"],
        "trust_tier": source["trust_tier"],
        "content_hash": _hash_id(item["title"], item.get("url") or "", item.get("published_at") or "", length=64),
        "payload": {"adapter": adapter, "source_name": source["source_name"], "item": payload},
        "normalized_ref": item.get("url"),
    }


def _official_update(source: dict, item: dict, update_type: str, payload: dict) -> dict:
    return {
        "update_id": _hash_id(source["source_id"], item.get("external_id") or item.get("url") or item["title"]),
        "issuing_body": source["source_name"],
        "update_type": update_type,
        "title": item["title"],
        "url": item.get("url"),
        "published_at": item.get("published_at"),
        "fetched_at": time.time(),
        "region": source.get("region"),
        "language": source.get("language") or "en",
        "trust_tier": source["trust_tier"],
        "content_hash": _hash_id(source["source_name"], item["title"], item.get("url") or "", item.get("published_at") or "", length=64),
        "payload": payload,
        "summary": item.get("description") or item["title"],
    }


def _page_listing_items(
    session: requests.Session,
    page_url: str,
    *,
    allowed_domains: list[str] | None = None,
    allowed_href_parts: tuple[str, ...] | None = None,
    blocked_href_parts: tuple[str, ...] = (),
    title_keywords: list[str] | None = None,
    limit: int = 30,
) -> list[dict]:
    response = session.get(page_url, timeout=20)
    response.raise_for_status()
    parser = AnchorParser()
    parser.feed(response.text)
    items = []
    seen = set()
    for link in parser.links:
        href = (link.get("href") or "").strip()
        text = re.sub(r"\s+", " ", (link.get("text") or "").strip())
        if not href or not text or len(text) < 12:
            continue
        absolute = urljoin(page_url, href)
        parsed = urlparse(absolute)
        if allowed_domains and parsed.netloc.lower() not in {domain.lower() for domain in allowed_domains}:
            continue
        lower_href = absolute.lower()
        if allowed_href_parts and not any(part in lower_href for part in allowed_href_parts):
            continue
        if any(part in lower_href for part in blocked_href_parts):
            continue
        lower_text = text.lower()
        if title_keywords and not any(keyword in lower_text for keyword in title_keywords):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        items.append(
            {
                "title": unescape(text),
                "url": absolute,
                "description": unescape(text),
                "published_at": datetime.now(timezone.utc).isoformat(),
                "external_id": absolute,
            }
        )
        if len(items) >= limit:
            break
    return items


def _article_from_update(source: dict, update: dict) -> dict:
    return {
        "title": update["title"],
        "description": update.get("summary") or update["title"],
        "source": source["source_name"],
        "source_domain": source.get("source_domain") or urlparse(update.get("url") or "").netloc.lower(),
        "url": update.get("url") or f"https://{source.get('source_domain') or 'local'}/updates/{update['update_id']}",
        "published_at": update.get("published_at") or datetime.now(timezone.utc).isoformat(),
        "language": source.get("language") or "en",
    }


def _discover_imf_rss_urls(session: requests.Session, directory_url: str) -> list[str]:
    response = session.get(directory_url, timeout=20)
    response.raise_for_status()
    parser = AnchorParser()
    parser.feed(response.text)
    urls = []
    for link in parser.links:
        href = link.get("href") or ""
        absolute = urljoin(directory_url, href)
        if absolute.lower().endswith((".xml", ".rss")) or "/rss" in absolute.lower():
            urls.append(absolute)
    unique = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique.append(url)
    return unique[:6]


def fetch_imf_updates(source: dict, limit: int = 30) -> list[dict]:
    session = _session()
    metadata = source.get("metadata") or {}
    feed_urls = metadata.get("feed_urls") or _discover_imf_rss_urls(session, metadata.get("rss_directory_url", "https://www.imf.org/en/news/rss"))
    updates = []
    for feed_url in feed_urls:
        try:
            response = session.get(feed_url, timeout=20)
            response.raise_for_status()
            for item in _extract_xml_items(response.text):
                updates.append(
                    {
                        "item": item,
                        "raw": _raw_document(source, item, "imf_rss", {"feed_url": feed_url, "entry": item}),
                        "update": _official_update(source, item, "rss_release", {"feed_url": feed_url, "entry": item}),
                    }
                )
        except Exception:
            continue
    deduped = {}
    for row in updates:
        deduped[row["update"]["update_id"]] = row
    return list(deduped.values())[:limit]


def fetch_ofac_updates(source: dict, limit: int = 30) -> list[dict]:
    session = _session()
    metadata = source.get("metadata") or {}
    url = metadata.get("recent_actions_url", "https://ofac.treasury.gov/recent-actions")
    response = session.get(url, timeout=20)
    response.raise_for_status()
    parser = AnchorParser()
    parser.feed(response.text)
    items = []
    for link in parser.links:
        href = link.get("href") or ""
        text = (link.get("text") or "").strip()
        if not text:
            continue
        absolute = urljoin(url, href)
        if "/recent-actions/" not in absolute:
            continue
        if absolute.rstrip("/") == url.rstrip("/"):
            continue
        item = {
            "title": unescape(text),
            "url": absolute,
            "description": unescape(text),
            "published_at": datetime.now(timezone.utc).isoformat(),
            "external_id": absolute,
        }
        items.append(
            {
                "item": item,
                "raw": _raw_document(source, item, "ofac_recent_actions", {"page_url": url, "entry": item}),
                "update": _official_update(source, item, "sanctions_action", {"page_url": url, "entry": item}),
            }
        )
    deduped = {}
    for row in items:
        deduped[row["update"]["update_id"]] = row
    return list(deduped.values())[:limit]


def fetch_ofac_sanctions_updates(source: dict, limit: int = 30) -> list[dict]:
    metadata = source.get("metadata") or {}
    keywords = [keyword.lower() for keyword in (metadata.get("match_keywords") or [])]
    rows = []
    for row in fetch_ofac_updates(source, limit=max(limit * 3, 30)):
        title = ((row.get("item") or {}).get("title") or "").lower()
        if keywords and not any(keyword in title for keyword in keywords):
            continue
        row["update"]["update_type"] = "sanctions_list_update"
        row["raw"]["payload"]["adapter"] = "ofac_sanctions_updates"
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def fetch_world_bank_updates(source: dict, limit: int = 30) -> list[dict]:
    session = _session()
    metadata = source.get("metadata") or {}
    url = metadata.get("news_url", "https://www.worldbank.org/en/news")
    response = session.get(url, timeout=20)
    response.raise_for_status()
    parser = AnchorParser()
    parser.feed(response.text)
    items = []
    allowed_parts = ("/en/news/press-release/", "/en/news/statement/", "/en/news/feature/", "/en/news/speech/")
    for link in parser.links:
        href = link.get("href") or ""
        text = (link.get("text") or "").strip()
        if not text:
            continue
        absolute = urljoin(url, href)
        if not any(part in absolute for part in allowed_parts):
            continue
        item = {
            "title": unescape(text),
            "url": absolute,
            "description": unescape(text),
            "published_at": datetime.now(timezone.utc).isoformat(),
            "external_id": absolute,
        }
        items.append(
            {
                "item": item,
                "raw": _raw_document(source, item, "world_bank_news", {"page_url": url, "entry": item}),
                "update": _official_update(source, item, "world_bank_release", {"page_url": url, "entry": item}),
            }
        )
    deduped = {}
    for row in items:
        deduped[row["update"]["update_id"]] = row
    return list(deduped.values())[:limit]


def fetch_reliefweb_updates(source: dict, limit: int = 30) -> list[dict]:
    session = _session()
    metadata = source.get("metadata") or {}
    reliefweb_source = metadata.get("reliefweb_source") or source["source_name"]
    payload = {
        "appname": "othello_v2",
        "limit": limit,
        "profile": "list",
        "sort": ["date.created:desc"],
        "fields": {
            "include": [
                "title",
                "date.created",
                "source",
                "body",
                "primary_country",
                "url_alias",
            ]
        },
        "filter": {
            "operator": "AND",
            "conditions": [
                {"field": "source.name", "value": reliefweb_source},
            ],
        },
    }
    response = session.post(
        RELIEFWEB_ENDPOINT,
        params={"appname": RELIEFWEB_APPNAME},
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    data = response.json().get("data", [])
    rows = []
    for record in data:
        fields = record.get("fields", {})
        url_alias = fields.get("url_alias") or record.get("href")
        url = url_alias if isinstance(url_alias, str) and url_alias.startswith("http") else None
        item = {
            "title": fields.get("title") or "ReliefWeb update",
            "url": url or f"https://reliefweb.int/node/{record.get('id')}",
            "description": re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", fields.get("body") or "")).strip()[:400],
            "published_at": _normalize_time((fields.get("date") or {}).get("created")),
            "external_id": str(record.get("id")),
        }
        rows.append(
            {
                "item": item,
                "raw": _raw_document(source, item, "reliefweb_reports", record),
                "update": _official_update(source, item, "humanitarian_report", record),
            }
        )
    return rows[:limit]


def fetch_official_page_updates(source: dict, limit: int = 30) -> list[dict]:
    session = _session()
    metadata = source.get("metadata") or {}
    pages = metadata.get("pages") or []
    allowed_domains = metadata.get("allowed_domains") or ([source.get("source_domain")] if source.get("source_domain") else None)
    allowed_href_parts = tuple(metadata.get("allowed_href_parts") or ())
    title_keywords = metadata.get("title_keywords") or []
    update_type = f"{(metadata.get('collection') or 'official')}_update"

    rows = []
    for page_url in pages:
        items = _page_listing_items(
            session,
            page_url,
            allowed_domains=allowed_domains,
            allowed_href_parts=allowed_href_parts or None,
            blocked_href_parts=("/search", "/tag/", "/topic/", "/country/"),
            title_keywords=title_keywords,
            limit=limit,
        )
        for item in items:
            rows.append(
                {
                    "item": item,
                    "raw": _raw_document(source, item, "official_page_listing", {"page_url": page_url, "entry": item}),
                    "update": _official_update(source, item, update_type, {"page_url": page_url, "entry": item}),
                }
            )
            if len(rows) >= limit:
                return rows[:limit]
    return rows[:limit]


def fetch_unsc_updates(source: dict, limit: int = 30) -> list[dict]:
    session = _session()
    metadata = source.get("metadata") or {}
    pages = metadata.get("pages") or ["https://press.un.org/en/security-council"]
    rows = []
    for page_url in pages:
        items = _page_listing_items(
            session,
            page_url,
            allowed_domains=["press.un.org"],
            allowed_href_parts=("/en/", "security-council", ".doc", ".htm"),
            blocked_href_parts=("/content/security-council", "/page/", "/search"),
            title_keywords=[
                "security council",
                "members to hold",
                "meeting",
                "statement",
                "briefing",
                "programme of work",
                "request",
            ],
            limit=limit,
        )
        for item in items:
            title_lower = item["title"].lower()
            if "programme of work" in title_lower or "members to hold" in title_lower or "meeting" in title_lower:
                update_type = "security_council_meeting_signal"
            elif "statement" in title_lower:
                update_type = "security_council_statement"
            else:
                update_type = "security_council_document"
            rows.append(
                {
                    "item": item,
                    "raw": _raw_document(source, item, "unsc_press_pages", {"page_url": page_url, "entry": item}),
                    "update": _official_update(source, item, update_type, {"page_url": page_url, "entry": item}),
                }
            )
            if len(rows) >= limit:
                return rows[:limit]
    return rows[:limit]


def fetch_icc_press_updates(source: dict, limit: int = 30) -> list[dict]:
    session = _session()
    metadata = source.get("metadata") or {}
    pages = metadata.get("pages") or ["https://www.icc-cpi.int/news"]
    rows = []
    for page_url in pages:
        items = _page_listing_items(
            session,
            page_url,
            allowed_domains=["www.icc-cpi.int", "asp.icc-cpi.int", "icc-cpi.int"],
            allowed_href_parts=("/news/", "/press-releases", "/cases/"),
            blocked_href_parts=("/node/", "/search"),
            title_keywords=["icc", "court", "prosecutor", "chamber", "warrant", "press release", "statement"],
            limit=limit,
        )
        for item in items:
            rows.append(
                {
                    "item": item,
                    "raw": _raw_document(source, item, "icc_press_pages", {"page_url": page_url, "entry": item}),
                    "update": _official_update(source, item, "icc_press_release", {"page_url": page_url, "entry": item}),
                }
            )
            if len(rows) >= limit:
                return rows[:limit]
    return rows[:limit]


def fetch_icc_filing_updates(source: dict, limit: int = 30) -> list[dict]:
    session = _session()
    metadata = source.get("metadata") or {}
    pages = metadata.get("pages") or ["https://www.icc-cpi.int/case-records?page=0"]
    rows = []
    for page_url in pages:
        items = _page_listing_items(
            session,
            page_url,
            allowed_domains=["www.icc-cpi.int", "asp.icc-cpi.int", "icc-cpi.int"],
            allowed_href_parts=("/case-records", "/cases/", "/documents/", "/record"),
            blocked_href_parts=("/search", "/news"),
            title_keywords=["decision", "filing", "application", "submission", "order", "request", "warrant", "record"],
            limit=limit,
        )
        for item in items:
            rows.append(
                {
                    "item": item,
                    "raw": _raw_document(source, item, "icc_filing_pages", {"page_url": page_url, "entry": item}),
                    "update": _official_update(source, item, "icc_filing", {"page_url": page_url, "entry": item}),
                }
            )
            if len(rows) >= limit:
                return rows[:limit]
    return rows[:limit]


def fetch_gazette_updates(source: dict, limit: int = 30) -> list[dict]:
    session = _session()
    metadata = source.get("metadata") or {}
    pages = metadata.get("pages") or ["https://www.thegazette.co.uk/all-notices"]
    rows = []
    for page_url in pages:
        items = _page_listing_items(
            session,
            page_url,
            allowed_domains=["www.thegazette.co.uk", "thegazette.co.uk"],
            allowed_href_parts=("/notice/", "/all-notices"),
            blocked_href_parts=("/edition/", "/search"),
            title_keywords=["notice", "order", "sanction", "treasury", "foreign", "company", "insolvency", "government", "appointment"],
            limit=limit,
        )
        for item in items:
            rows.append(
                {
                    "item": item,
                    "raw": _raw_document(source, item, "gazette_notices", {"page_url": page_url, "entry": item}),
                    "update": _official_update(source, item, "gazette_notice", {"page_url": page_url, "entry": item}),
                }
            )
            if len(rows) >= limit:
                return rows[:limit]
    return rows[:limit]


def ingest_official_updates(limit_per_source: int = 30) -> dict:
    registry = get_source_registry(source_type="official_update", active_only=True)
    totals = {"sources": 0, "official_updates": 0, "raw_documents": 0, "mirrored_articles": 0, "promoted_mirrored_articles": 0, "rejected_mirrored_articles": 0, "errors": 0}
    results = []

    adapter_map = {
        "imf_rss": fetch_imf_updates,
        "ofac_recent_actions": fetch_ofac_updates,
        "ofac_sanctions_updates": fetch_ofac_sanctions_updates,
        "world_bank_news": fetch_world_bank_updates,
        "reliefweb_reports": fetch_reliefweb_updates,
        "official_page_listing": fetch_official_page_updates,
        "unsc_press_pages": fetch_unsc_updates,
        "icc_press_pages": fetch_icc_press_updates,
        "icc_filing_pages": fetch_icc_filing_updates,
        "gazette_notices": fetch_gazette_updates,
    }

    for source in registry:
        adapter = (source.get("metadata") or {}).get("adapter")
        if adapter not in adapter_map:
            continue
        totals["sources"] += 1
        try:
            fetched = adapter_map[adapter](source, limit=limit_per_source)
            raw_documents = [row["raw"] for row in fetched]
            updates = [row["update"] for row in fetched]
            raw_written = record_raw_source_documents(raw_documents)
            updates_written = upsert_official_updates(updates)
            mirrored = 0
            promoted_mirrored = 0
            rejected_mirrored = 0
            if (source.get("metadata") or {}).get("mirror_to_articles"):
                for update in updates:
                    article = _article_from_update(source, update)
                    topics = infer_article_topics(article)
                    if not should_promote_article(article, topics):
                        rejected_mirrored += 1
                        continue
                    mirrored += upsert_articles([article], topic=topics or ["geopolitics"], provider="official")
                    promoted_mirrored += 1
            totals["raw_documents"] += raw_written
            totals["official_updates"] += updates_written
            totals["mirrored_articles"] += mirrored
            totals["promoted_mirrored_articles"] += promoted_mirrored
            totals["rejected_mirrored_articles"] += rejected_mirrored
            results.append(
                {
                    "source_name": source["source_name"],
                    "adapter": adapter,
                    "status": "ok",
                    "official_updates": updates_written,
                    "raw_documents": raw_written,
                    "mirrored_articles": mirrored,
                    "promoted_mirrored_articles": promoted_mirrored,
                    "rejected_mirrored_articles": rejected_mirrored,
                }
            )
        except Exception as exc:
            totals["errors"] += 1
            results.append(
                {
                    "source_name": source["source_name"],
                    "adapter": adapter,
                    "status": "error",
                    "error": str(exc),
                }
            )

    return {"totals": totals, "results": results}
