import argparse
import csv
import json
import os
import sqlite3
from pathlib import Path

from bootstrap_sources import seed_sources
from cache import clear_headlines, init_db as init_cache_db
from chroma import store_articles
from corpus import init_db as init_corpus_db
from corpus import migrate_sqlite_to_current_backend, upsert_articles
from core.config import TOPICS
from services.ingest_service import _store_entity_mentions_with_translation
from services.briefing_service import build_topic_briefing
from services.headlines_service import rebuild_headlines_cache
from news import infer_article_topics


ARTICLE_KEY_ALIASES = {
    "title": ["title", "headline", "name"],
    "description": ["description", "summary", "snippet", "excerpt", "content"],
    "source": ["source", "source_name", "publisher", "outlet"],
    "source_domain": ["source_domain", "domain", "publisher_domain"],
    "url": ["url", "link", "article_url"],
    "published_at": ["published_at", "published", "publishedAt", "date", "datetime", "timestamp"],
    "language": ["language", "lang"],
}


def _extract_value(record: dict, keys: list[str]) -> str | None:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
        if value:
            return value
    return None


def _normalize_article_record(record: dict) -> dict | None:
    url = _extract_value(record, ARTICLE_KEY_ALIASES["url"])
    title = _extract_value(record, ARTICLE_KEY_ALIASES["title"])
    if not url or not title:
        return None
    return {
        "title": title,
        "description": _extract_value(record, ARTICLE_KEY_ALIASES["description"]) or title,
        "source": _extract_value(record, ARTICLE_KEY_ALIASES["source"]) or "Imported archive",
        "source_domain": _extract_value(record, ARTICLE_KEY_ALIASES["source_domain"]) or "",
        "url": url,
        "published_at": _extract_value(record, ARTICLE_KEY_ALIASES["published_at"]) or "",
        "language": _extract_value(record, ARTICLE_KEY_ALIASES["language"]) or "en",
    }


def _topics_for_record(article: dict, record: dict, forced_topics: list[str], infer_topics: bool) -> list[str]:
    if forced_topics:
        return forced_topics

    direct_topics = record.get("topics") or record.get("topic")
    if isinstance(direct_topics, str):
        direct_topics = [direct_topics]
    if isinstance(direct_topics, list):
        normalized = [topic for topic in direct_topics if topic in TOPICS]
        if normalized:
            return normalized

    if infer_topics:
        return infer_article_topics(article)
    return []


def _load_json_records(path: Path) -> list[dict]:
    payload = json.loads(path.read_text())
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]
    if isinstance(payload, dict):
        for key in ("articles", "items", "data", "sources", "stories", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [record for record in value if isinstance(record, dict)]
    raise ValueError(f"Unsupported JSON structure in {path}")


def _load_jsonl_records(path: Path) -> list[dict]:
    records = []
    with path.open() as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            payload = json.loads(text)
            if isinstance(payload, dict):
                records.append(payload)
    return records


def _load_csv_records(path: Path) -> list[dict]:
    with path.open(newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _load_sqlite_records(path: Path) -> list[tuple[dict, list[str], str]]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    rows: list[tuple[dict, list[str], str]] = []

    if {"articles", "article_topics"} <= tables:
        article_rows = conn.execute("SELECT payload, provider FROM articles").fetchall()
        topic_rows = conn.execute("SELECT article_url, topic FROM article_topics").fetchall()
        topic_map: dict[str, list[str]] = {}
        for row in topic_rows:
            topic_map.setdefault(row["article_url"], []).append(row["topic"])
        for row in article_rows:
            payload = json.loads(row["payload"]) if row["payload"] else {}
            url = payload.get("url")
            if not url:
                continue
            rows.append((payload, topic_map.get(url, []), row["provider"]))

    if "briefing_cache" in tables:
        briefing_rows = conn.execute("SELECT topic, sources FROM briefing_cache").fetchall()
        for row in briefing_rows:
            topic = row["topic"]
            if topic not in TOPICS:
                continue
            try:
                articles = json.loads(row["sources"] or "[]")
            except json.JSONDecodeError:
                continue
            for article in articles:
                if isinstance(article, dict):
                    rows.append((article, [topic], "sqlite-briefing-cache"))

    if "headlines_cache" in tables:
        headline_rows = conn.execute("SELECT stories FROM headlines_cache").fetchall()
        for row in headline_rows:
            try:
                stories = json.loads(row["stories"] or "[]")
            except json.JSONDecodeError:
                continue
            for story in stories:
                topic = story.get("topic")
                topic_list = [topic] if topic in TOPICS else []
                for article in story.get("sources", []):
                    if isinstance(article, dict):
                        rows.append((article, topic_list, "sqlite-headlines-cache"))

    conn.close()
    return rows


def _detect_format(path: Path, explicit: str) -> str:
    if explicit != "auto":
        return explicit
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    if suffix == ".json":
        return "json"
    if suffix in {".sqlite", ".db"}:
        return "sqlite"
    raise ValueError(f"Could not detect format for {path}")


def _batch(iterable: list[dict], size: int) -> list[list[dict]]:
    return [iterable[index:index + size] for index in range(0, len(iterable), size)]


def import_archive(
    path: Path,
    format_name: str,
    provider: str,
    forced_topics: list[str],
    infer_topics: bool,
    skip_chroma: bool,
    skip_entities: bool,
    refresh_snapshots: bool,
    batch_size: int,
) -> dict:
    init_cache_db()
    init_corpus_db()
    seed_sources()

    if format_name == "sqlite" and not forced_topics and infer_topics:
        # If the file is already in Othello's SQLite schema, preserve all existing topic mappings.
        try:
            migrated = migrate_sqlite_to_current_backend(str(path))
            if migrated.get("migrated"):
                return {
                    "path": str(path),
                    "format": format_name,
                    "provider": "preserved-sqlite",
                    "records_seen": migrated["migrated"],
                    "inserted_or_updated": migrated["migrated"],
                    "topics": {},
                    "used_direct_migration": True,
                }
        except Exception:
            pass

    loaded_records: list[tuple[dict, list[str], str]] = []
    if format_name == "json":
        loaded_records = [(record, [], provider) for record in _load_json_records(path)]
    elif format_name == "jsonl":
        loaded_records = [(record, [], provider) for record in _load_jsonl_records(path)]
    elif format_name == "csv":
        loaded_records = [(record, [], provider) for record in _load_csv_records(path)]
    elif format_name == "sqlite":
        loaded_records = _load_sqlite_records(path)
    else:
        raise ValueError(f"Unsupported format: {format_name}")

    topic_buckets: dict[str, list[dict]] = {topic: [] for topic in TOPICS}
    records_seen = 0
    skipped = 0

    for record, sqlite_topics, row_provider in loaded_records:
        article = _normalize_article_record(record)
        if not article:
            skipped += 1
            continue
        topics = [topic for topic in sqlite_topics if topic in TOPICS] or _topics_for_record(article, record, forced_topics, infer_topics)
        if not topics:
            skipped += 1
            continue
        records_seen += 1
        article["provider"] = row_provider
        for topic in topics:
            topic_buckets[topic].append(dict(article))

    inserted_or_updated = 0
    topic_counts = {topic: 0 for topic in TOPICS}
    entity_counts = {topic: 0 for topic in TOPICS}

    for topic, articles in topic_buckets.items():
        if not articles:
            continue
        for batch in _batch(articles, batch_size):
            inserted_or_updated += upsert_articles(batch, topic=topic, provider=provider)
            topic_counts[topic] += len(batch)
            if not skip_chroma:
                store_articles(batch, topic)
            if not skip_entities:
                _store_entity_mentions_with_translation(batch, topic)
                entity_counts[topic] += len(batch)

    if refresh_snapshots:
        clear_headlines()
        rebuild_headlines_cache(use_llm=False)
        for topic in TOPICS:
            if topic_counts[topic]:
                build_topic_briefing(topic, force_refresh=True)

    return {
        "path": str(path),
        "format": format_name,
        "provider": provider,
        "records_seen": records_seen,
        "skipped": skipped,
        "inserted_or_updated": inserted_or_updated,
        "topics": topic_counts,
        "entity_batches": entity_counts,
        "snapshots_refreshed": refresh_snapshots,
        "used_direct_migration": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import historical article archives into the Othello V2 Postgres corpus.")
    parser.add_argument("path", help="Path to a JSON, JSONL, CSV, or SQLite archive.")
    parser.add_argument("--format", default="auto", choices=["auto", "json", "jsonl", "csv", "sqlite"])
    parser.add_argument("--provider", default="archive-import", help="Provider label recorded for imported articles.")
    parser.add_argument("--topic", action="append", choices=TOPICS, help="Force imported records into one or more topics.")
    parser.add_argument("--no-infer-topics", action="store_true", help="Do not infer topics from article text when topics are missing.")
    parser.add_argument("--skip-chroma", action="store_true", help="Skip vector-store writes during import.")
    parser.add_argument("--skip-entities", action="store_true", help="Skip entity extraction during import.")
    parser.add_argument("--refresh-snapshots", action="store_true", help="Rebuild headlines and briefings after import.")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for corpus/chroma/entity writes.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.path).expanduser().resolve()
    if not path.exists():
        raise SystemExit(f"Archive not found: {path}")

    result = import_archive(
        path=path,
        format_name=_detect_format(path, args.format),
        provider=args.provider,
        forced_topics=args.topic or [],
        infer_topics=not args.no_infer_topics,
        skip_chroma=args.skip_chroma,
        skip_entities=args.skip_entities,
        refresh_snapshots=args.refresh_snapshots,
        batch_size=max(1, args.batch_size),
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
