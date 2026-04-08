import hashlib
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import requests

from corpus import (
    get_recent_articles,
    get_source_registry,
    load_ingestion_state,
    record_raw_source_documents,
    save_ingestion_state,
    upsert_articles,
    upsert_official_updates,
)
from news import (
    article_quality_score,
    infer_article_topics,
    normalize_article_description,
    normalize_article_title,
    should_promote_article,
)
from sources.source_catalog import (
    SOURCE_PACKS,
    source_in_pack,
    source_is_blocked,
    source_pack_for,
)

DIRECT_FEED_DEFAULT_LIMIT_PER_SOURCE = int(
    os.getenv("OTHELLO_DIRECT_FEED_LIMIT_PER_SOURCE", "14")
)
DIRECT_FEED_DEFAULT_MAX_AGE_HOURS = int(
    os.getenv("OTHELLO_DIRECT_FEED_MAX_AGE_HOURS", "120")
)
DIRECT_FEED_ERROR_COOLDOWN_MINUTES = int(
    os.getenv("OTHELLO_DIRECT_FEED_ERROR_COOLDOWN_MINUTES", "120")
)
DIRECT_FEED_FORBIDDEN_COOLDOWN_MINUTES = int(
    os.getenv("OTHELLO_DIRECT_FEED_FORBIDDEN_COOLDOWN_MINUTES", "360")
)
DIRECT_FEED_RATE_LIMIT_COOLDOWN_MINUTES = int(
    os.getenv("OTHELLO_DIRECT_FEED_RATE_LIMIT_COOLDOWN_MINUTES", "720")
)


def _http() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "OthelloV2/1.0 (+source ingestion)",
            "Accept": "application/rss+xml,application/xml,text/xml,application/atom+xml,*/*",
        }
    )
    return session


def _normalize_timestamp(raw: str | None) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:
        return raw.strip()


def _parse_timestamp(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = raw.strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except Exception:
        pass
    try:
        parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _text(node, tag_names: list[str]) -> str | None:
    for tag_name in tag_names:
        match = node.find(tag_name)
        if match is not None:
            text = "".join(match.itertext()).strip()
            if text:
                return text
    return None


def _parse_feed(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    entries = []
    for item in root.findall(".//item"):
        entries.append(
            {
                "external_id": _text(item, ["guid"]) 
                or _text(item, ["link"]) 
                or _text(item, ["title"]),
                "title": _text(item, ["title"]),
                "url": _text(item, ["link"]),
                "description": _text(item, ["description"]),
                "published_at": _normalize_timestamp(
                    _text(item, ["pubDate", "published", "updated"])
                ),
                "language": _text(item, ["language"]),
            }
        )
    for item in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        link = item.find("{http://www.w3.org/2005/Atom}link")
        href = link.attrib.get("href") if link is not None else None
        entries.append(
            {
                "external_id": _text(item, ["{http://www.w3.org/2005/Atom}id"]) or href,
                "title": _text(item, ["{http://www.w3.org/2005/Atom}title"]),
                "url": href,
                "description": _text(
                    item,
                    [
                        "{http://www.w3.org/2005/Atom}summary",
                        "{http://www.w3.org/2005/Atom}content",
                    ],
                ),
                "published_at": _normalize_timestamp(
                    _text(
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


def _document_id(
    source_id: str, external_id: str | None, url: str | None, title: str | None
) -> str:
    material = " | ".join([source_id, external_id or "", url or "", title or ""])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def _raw_document_from_entry(source: dict, entry: dict, feed_url: str) -> dict:
    metadata = source.get("metadata") or {}
    url = entry.get("url")
    return {
        "document_id": _document_id(
            source["source_id"], entry.get("external_id"), url, entry.get("title")
        ),
        "source_id": source["source_id"],
        "external_id": entry.get("external_id"),
        "url": url,
        "title": entry.get("title"),
        "published_at": entry.get("published_at"),
        "fetched_at": time.time(),
        "language": entry.get("language") or source.get("language") or "en",
        "source_type": source["source_type"],
        "trust_tier": source["trust_tier"],
        "content_hash": hashlib.sha256(
            " | ".join(
                [entry.get("title") or "", url or "", entry.get("published_at") or ""]
            ).encode("utf-8")
        ).hexdigest(),
        "payload": {
            "source_name": source["source_name"],
            "source_domain": source.get("source_domain"),
            "feed_url": feed_url,
            "entry": entry,
            "metadata": metadata,
        },
        "normalized_ref": url,
    }


def _article_from_entry(source: dict, entry: dict) -> dict:
    title = normalize_article_title(entry.get("title"))
    return {
        "title": title,
        "description": normalize_article_description(entry.get("description"), title)
        or title,
        "source": source["source_name"],
        "source_domain": source.get("source_domain")
        or urlparse(entry["url"]).netloc.lower(),
        "url": entry["url"],
        "published_at": entry.get("published_at"),
        "language": entry.get("language") or source.get("language") or "en",
    }


def _source_state_key(source_id: str) -> str:
    return f"direct-feed-source-{source_id}"


def _source_pack_config(source: dict) -> dict:
    return SOURCE_PACKS.get(source_pack_for(source) or "", {})


def _source_limit(source: dict, limit_per_source: int) -> int:
    configured = int(
        (
            _source_pack_config(source).get("default_limit_per_source")
            or DIRECT_FEED_DEFAULT_LIMIT_PER_SOURCE
        )
    )
    return max(4, min(limit_per_source, configured))


def _source_max_age_hours(source: dict) -> int:
    configured = int(
        (
            _source_pack_config(source).get("default_max_age_hours")
            or DIRECT_FEED_DEFAULT_MAX_AGE_HOURS
        )
    )
    return max(24, configured)


def _source_cooldown_active(source: dict) -> tuple[bool, dict | None]:
    state = load_ingestion_state(_source_state_key(source["source_id"]))
    if not state:
        return False, None
    payload = state.get("payload") or {}
    retry_after = _parse_timestamp(payload.get("retry_after"))
    if retry_after and retry_after > datetime.now(timezone.utc):
        return True, state
    return False, state


def _save_source_state(
    source: dict,
    *,
    status: str,
    error: str | None = None,
    payload: dict | None = None,
) -> None:
    save_ingestion_state(
        _source_state_key(source["source_id"]),
        source.get("source_name") or source["source_id"],
        "directfeeds",
        None,
        None,
        status,
        error=error,
        payload=payload or {},
    )


def _entry_is_fresh(entry: dict, max_age_hours: int) -> bool:
    published_at = _parse_timestamp(entry.get("published_at"))
    if not published_at:
        return True
    return published_at >= datetime.now(timezone.utc) - timedelta(hours=max_age_hours)


def _article_registry_maps() -> dict:
    registry = get_source_registry(source_type="article", active_only=True)
    by_domain = {}
    by_name = {}
    for source in registry:
        domain = (source.get("source_domain") or "").lower()
        if domain:
            by_domain[domain] = source
        name = (source.get("source_name") or "").strip().lower()
        if name:
            by_name[name] = source
    return {"by_domain": by_domain, "by_name": by_name}


def _match_article_source(article: dict, registry_maps: dict) -> dict | None:
    domain = (
        article.get("source_domain") or urlparse(article.get("url", "")).netloc
    ).lower()
    source_name = (article.get("source") or "").strip().lower()
    return registry_maps["by_domain"].get(domain) or registry_maps["by_name"].get(
        source_name
    )


def _raw_document_from_article(
    source: dict, article: dict, provider: str, topic_hint: str | None = None
) -> dict:
    external_id = article.get("url")
    title = article.get("title")
    url = article.get("url")
    topics = infer_article_topics(article)
    return {
        "document_id": _document_id(source["source_id"], external_id, url, title),
        "source_id": source["source_id"],
        "external_id": external_id,
        "url": url,
        "title": title,
        "published_at": article.get("published_at"),
        "fetched_at": time.time(),
        "language": article.get("language") or source.get("language") or "en",
        "source_type": source["source_type"],
        "trust_tier": source["trust_tier"],
        "content_hash": hashlib.sha256(
            " | ".join(
                [title or "", url or "", article.get("published_at") or "", provider]
            ).encode("utf-8")
        ).hexdigest(),
        "payload": {
            "provider": provider,
            "topic_hint": topic_hint,
            "article": article,
            "inferred_topics": topics,
            "quality_score": article_quality_score(article, topics),
            "promotable": should_promote_article(article, topics),
        },
        "normalized_ref": url,
    }


def archive_provider_articles(
    articles: list[dict], provider: str, topic_hint: str | None = None
) -> dict:
    if not articles:
        return {"matched_articles": 0, "documents_written": 0}

    registry_maps = _article_registry_maps()
    documents = []
    matched = 0
    for article in articles:
        source = _match_article_source(article, registry_maps)
        if not source:
            continue
        matched += 1
        documents.append(
            _raw_document_from_article(
                source, article, provider=provider, topic_hint=topic_hint
            )
        )

    written = record_raw_source_documents(documents)
    return {"matched_articles": matched, "documents_written": written}


def _official_update_from_entry(source: dict, entry: dict) -> dict:
    return {
        "update_id": _document_id(
            source["source_id"],
            entry.get("external_id"),
            entry.get("url"),
            entry.get("title"),
        ),
        "issuing_body": source["source_name"],
        "update_type": "feed_update",
        "title": entry["title"],
        "url": entry["url"],
        "published_at": entry["published_at"],
        "fetched_at": time.time(),
        "region": source.get("region"),
        "language": entry.get("language") or source.get("language") or "en",
        "trust_tier": source["trust_tier"],
        "content_hash": hashlib.sha256(
            " | ".join(
                [
                    source["source_name"],
                    entry["title"],
                    entry["url"],
                    entry["published_at"],
                ]
            ).encode("utf-8")
        ).hexdigest(),
        "payload": {"source_id": source["source_id"], "entry": entry},
        "summary": entry.get("description") or entry["title"],
    }


def ingest_registry_sources(
    source_type: str | None = None,
    limit_per_source: int = 25,
    packs: list[str] | tuple[str, ...] | set[str] | None = None,
) -> dict:
    session = _http()
    registry = get_source_registry(source_type=source_type, active_only=True)
    if packs:
        registry = [source for source in registry if source_in_pack(source, packs)]
    results = []
    totals = {
        "sources": 0,
        "documents": 0,
        "articles": 0,
        "inserted_articles": 0,
        "promoted_articles": 0,
        "rejected_articles": 0,
        "official_updates": 0,
        "errors": 0,
        "skipped_sources": 0,
        "packs": {},
    }

    for source in registry:
        metadata = source.get("metadata") or {}
        if source_is_blocked(source):
            continue
        if metadata.get("adapter") != "rss":
            continue
        feeds = metadata.get("feeds") or []
        pack_name = source_pack_for(source) or "unassigned"
        pack_totals = totals["packs"].setdefault(
            pack_name,
            {
                "sources": 0,
                "documents": 0,
                "articles": 0,
                "inserted_articles": 0,
                "promoted_articles": 0,
                "rejected_articles": 0,
                "official_updates": 0,
                "errors": 0,
                "skipped_sources": 0,
            },
        )
        totals["sources"] += 1
        pack_totals["sources"] += 1

        cooldown_active, existing_state = _source_cooldown_active(source)
        if cooldown_active:
            totals["skipped_sources"] += 1
            pack_totals["skipped_sources"] += 1
            payload = (existing_state or {}).get("payload") or {}
            results.append(
                {
                    "source_id": source["source_id"],
                    "source_name": source["source_name"],
                    "pack": pack_name,
                    "status": "cooldown",
                    "retry_after": payload.get("retry_after"),
                    "last_error": (
                        existing_state.get("error") if existing_state else None
                    ),
                }
            )
            continue

        source_documents = []
        source_articles = []
        source_updates = []
        try:
            newest_published_at = None
            max_age_hours = _source_max_age_hours(source)
            per_source_limit = _source_limit(source, limit_per_source)
            for feed in feeds:
                response = session.get(feed["url"], timeout=20)
                response.raise_for_status()
                entries = [
                    entry
                    for entry in _parse_feed(response.text)
                    if _entry_is_fresh(entry, max_age_hours=max_age_hours)
                ][:per_source_limit]
                for entry in entries:
                    entry_published = _parse_timestamp(entry.get("published_at"))
                    if entry_published and (
                        newest_published_at is None
                        or entry_published > newest_published_at
                    ):
                        newest_published_at = entry_published
                    source_documents.append(
                        _raw_document_from_entry(source, entry, feed["url"])
                    )
                    if source["source_type"] == "article":
                        source_articles.append(_article_from_entry(source, entry))
                    elif source["source_type"] == "official_update":
                        source_updates.append(
                            _official_update_from_entry(source, entry)
                        )

            documents_written = record_raw_source_documents(source_documents)
            article_written = 0
            promoted_articles = 0
            rejected_articles = 0
            if source_articles:
                for article in source_articles:
                    topics = infer_article_topics(article)
                    if not should_promote_article(article, topics):
                        rejected_articles += 1
                        continue
                    article_written += upsert_articles(
                        [article],
                        topic=topics or ["geopolitics"],
                        provider="directfeeds",
                    )
                    promoted_articles += 1
            official_written = upsert_official_updates(source_updates)

            totals["documents"] += documents_written
            totals["articles"] += promoted_articles
            totals["inserted_articles"] += article_written
            totals["promoted_articles"] += promoted_articles
            totals["rejected_articles"] += rejected_articles
            totals["official_updates"] += official_written
            pack_totals["documents"] += documents_written
            pack_totals["articles"] += promoted_articles
            pack_totals["inserted_articles"] += article_written
            pack_totals["promoted_articles"] += promoted_articles
            pack_totals["rejected_articles"] += rejected_articles
            pack_totals["official_updates"] += official_written
            _save_source_state(
                source,
                status="ok",
                payload={
                    "pack": pack_name,
                    "documents_written": documents_written,
                    "promoted_articles": promoted_articles,
                    "inserted_articles": article_written,
                    "rejected_articles": rejected_articles,
                    "official_updates": official_written,
                    "feeds_checked": len(feeds),
                    "newest_published_at": (
                        newest_published_at.isoformat() if newest_published_at else None
                    ),
                },
            )
            results.append(
                {
                    "source_id": source["source_id"],
                    "source_name": source["source_name"],
                    "pack": pack_name,
                    "status": "ok",
                    "documents": documents_written,
                    "articles": promoted_articles,
                    "inserted_articles": article_written,
                    "promoted_articles": promoted_articles,
                    "rejected_articles": rejected_articles,
                    "official_updates": official_written,
                    "newest_published_at": (
                        newest_published_at.isoformat() if newest_published_at else None
                    ),
                }
            )
        except Exception as exc:
            message = str(exc)
            retry_minutes = DIRECT_FEED_ERROR_COOLDOWN_MINUTES
            lowered = message.lower()
            if "403" in lowered or "forbidden" in lowered:
                retry_minutes = DIRECT_FEED_FORBIDDEN_COOLDOWN_MINUTES
            elif (
                "429" in lowered
                or "rate limit" in lowered
                or "too many requests" in lowered
            ):
                retry_minutes = DIRECT_FEED_RATE_LIMIT_COOLDOWN_MINUTES
            retry_after = (
                datetime.now(timezone.utc) + timedelta(minutes=retry_minutes)
            ).isoformat()
            _save_source_state(
                source,
                status="error",
                error=message,
                payload={
                    "pack": pack_name,
                    "feeds_checked": len(feeds),
                    "retry_after": retry_after,
                },
            )
            totals["errors"] += 1
            pack_totals["errors"] += 1
            results.append(
                {
                    "source_id": source["source_id"],
                    "source_name": source["source_name"],
                    "pack": pack_name,
                    "status": "error",
                    "error": message,
                    "retry_after": retry_after,
                }
            )

    return {"totals": totals, "results": results}


def registry_sources_with_feed_status() -> list[dict]:
    sources = get_source_registry(active_only=True)
    enriched = []
    for source in sources:
        state = load_ingestion_state(_source_state_key(source["source_id"]))
        enriched.append(
            {
                **source,
                "pack": source_pack_for(source),
                "feed_state": state,
            }
        )
    return enriched


def ingest_direct_feed_layer(limit_per_source: int = 20) -> dict:
    pack_order = ["global_wires", "regional_flagships", "conflict_region_outlets"]
    combined = {
        "status": "ok",
        "packs": [],
        "totals": {
            "sources": 0,
            "documents": 0,
            "articles": 0,
            "inserted_articles": 0,
            "promoted_articles": 0,
            "rejected_articles": 0,
            "official_updates": 0,
            "errors": 0,
            "skipped_sources": 0,
            "packs": {},
        },
    }

    for pack_name in pack_order:
        if pack_name not in SOURCE_PACKS:
            continue
        result = ingest_registry_sources(
            source_type="article", limit_per_source=limit_per_source, packs=[pack_name]
        )
        combined["packs"].append(
            {
                "pack": pack_name,
                "label": SOURCE_PACKS[pack_name]["label"],
                "description": SOURCE_PACKS[pack_name]["description"],
                "totals": result.get("totals", {}),
                "results": result.get("results", []),
            }
        )
        totals = result.get("totals", {})
        combined["totals"]["sources"] += int(totals.get("sources", 0) or 0)
        combined["totals"]["documents"] += int(totals.get("documents", 0) or 0)
        combined["totals"]["articles"] += int(totals.get("articles", 0) or 0)
        combined["totals"]["inserted_articles"] += int(
            totals.get("inserted_articles", 0) or 0
        )
        combined["totals"]["promoted_articles"] += int(
            totals.get("promoted_articles", 0) or 0
        )
        combined["totals"]["rejected_articles"] += int(
            totals.get("rejected_articles", 0) or 0
        )
        combined["totals"]["official_updates"] += int(
            totals.get("official_updates", 0) or 0
        )
        combined["totals"]["errors"] += int(totals.get("errors", 0) or 0)
        combined["totals"]["skipped_sources"] += int(
            totals.get("skipped_sources", 0) or 0
        )
        combined["totals"]["packs"][pack_name] = totals

    if combined["totals"]["errors"] and combined["totals"]["promoted_articles"] == 0:
        combined["status"] = "error"
    elif combined["totals"]["errors"]:
        combined["status"] = "partial"
    return combined


def mirror_corpus_articles_into_registry(hours: int = 336, limit: int = 600) -> dict:
    registry = get_source_registry(source_type="article", active_only=True)
    by_domain = {}
    for source in registry:
        domain = (source.get("source_domain") or "").lower()
        if domain:
            by_domain[domain] = source

    documents = []
    for article in get_recent_articles(limit=limit, hours=hours):
        domain = (article.get("source_domain") or "").lower()
        source = by_domain.get(domain)
        if not source:
            continue
        entry = {
            "external_id": article["url"],
            "title": article["title"],
            "url": article["url"],
            "description": article.get("description"),
            "published_at": article.get("published_at"),
            "language": article.get("language"),
        }
        documents.append(
            _raw_document_from_entry(source, entry, feed_url="corpus-mirror")
        )

    written = record_raw_source_documents(documents)
    return {"matched_articles": len(documents), "documents_written": written}
