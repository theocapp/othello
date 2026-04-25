import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from providers.base import get_http_session, _cooldown_active, _set_provider_cooldown
from sources.source_catalog import (
    SOURCE_SEEDS,
    source_is_blocked,
    source_pack_for,
)
from normalization.articles import _normalize_article, _normalize_feed_timestamp


def _feed_entry_text(node, tag_names: list[str]) -> str | None:
    for tag_name in tag_names:
        match = node.find(tag_name)
        if match is not None:
            text = "".join(match.itertext()).strip()
            if text:
                return text
    return None


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
        if source_is_blocked(seed):
            continue
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
    from news import _dedupe
    from ranking.article_quality import diversify_articles
    
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
