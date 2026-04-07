import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

from analyst import translate_article
from chroma import store_articles
from corpus import (
    init_db,
    record_ingestion_run,
    save_article_translation,
    upsert_articles,
)
from entities import init_db as init_entities_db, store_entity_mentions
from news import fetch_gdelt_historic_articles, is_english_article, probe_gdelt_window

TOPICS = ("geopolitics", "economics")


def _ensure_translations(articles: list[dict]) -> list[dict]:
    if not os.getenv("GROQ_API_KEY"):
        return articles

    for article in articles:
        if is_english_article(article) or article.get("translated_title"):
            continue
        try:
            translation = translate_article(article)
            save_article_translation(
                article_url=article["url"],
                source_language=article.get("language") or "unknown",
                translated_title=translation["translated_title"],
                translated_description=translation.get("translated_description"),
                translation_provider=translation.get("provider", "groq"),
                target_language=translation.get("target_language", "en"),
            )
            article["translated_title"] = translation["translated_title"]
            article["translated_description"] = translation.get(
                "translated_description"
            )
        except Exception:
            continue
    return articles


def _parse_date(value: str, end_of_day: bool = False) -> datetime:
    text = value.strip()
    if "T" in text:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    parsed = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day:
        return parsed + timedelta(days=1)
    return parsed


def ingest_topic(
    topic: str,
    start: datetime,
    end: datetime,
    page_size: int,
    min_window_hours: int,
    write_entities: bool,
    write_chroma: bool,
) -> dict:
    started_at = time.time()
    try:
        articles = fetch_gdelt_historic_articles(
            topic=topic,
            start=start,
            end=end,
            page_size=page_size,
            min_window_hours=min_window_hours,
        )
        if not articles:
            message = (
                f"No historic GDELT articles returned for '{topic}' between "
                f"{start.isoformat()} and {end.isoformat()}."
            )
            record_ingestion_run(topic, "gdelt", 0, started_at, "empty", error=message)
            return {
                "topic": topic,
                "fetched": 0,
                "inserted_or_updated": 0,
                "status": "empty",
                "error": message,
            }

        inserted = upsert_articles(
            articles,
            topic=topic,
            provider="gdelt",
            default_analytic_tier="volume",
        )
        if write_chroma:
            store_articles(articles, topic)
        if write_entities:
            store_entity_mentions(_ensure_translations(articles), topic)
        record_ingestion_run(topic, "gdelt", len(articles), started_at, "ok")
        return {
            "topic": topic,
            "fetched": len(articles),
            "inserted_or_updated": inserted,
            "status": "ok",
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
        }
    except Exception as exc:
        record_ingestion_run(topic, "gdelt", 0, started_at, "error", error=str(exc))
        return {
            "topic": topic,
            "fetched": 0,
            "inserted_or_updated": 0,
            "status": "error",
            "error": str(exc),
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill the Othello V2 Postgres corpus from GDELT by topic and date window."
    )
    parser.add_argument("--topic", choices=[*TOPICS, "all"], default="all")
    parser.add_argument(
        "--start-date",
        required=True,
        help="UTC start date or datetime (YYYY-MM-DD or ISO8601).",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="UTC end date or datetime (YYYY-MM-DD or ISO8601).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=250,
        help="Max GDELT records per request before window splitting.",
    )
    parser.add_argument(
        "--min-window-hours",
        type=int,
        default=6,
        help="Smallest window size used when splitting dense GDELT ranges.",
    )
    parser.add_argument(
        "--skip-entities",
        action="store_true",
        help="Skip entity extraction during backfill.",
    )
    parser.add_argument(
        "--skip-chroma", action="store_true", help="Skip Chroma writes during backfill."
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Check whether GDELT can serve the window without writing to Postgres.",
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date, end_of_day=True)
    if end <= start:
        raise SystemExit("--end-date must be after --start-date")

    init_db()
    init_entities_db()

    topics = TOPICS if args.topic == "all" else (args.topic,)
    if args.probe_only:
        results = [
            probe_gdelt_window(
                topic=topic,
                start=start,
                end=end,
                page_size=min(args.page_size, 25),
            )
            for topic in topics
        ]
        print(
            json.dumps(
                {
                    "topics": list(topics),
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "probe_only": True,
                    "results": results,
                },
                indent=2,
            )
        )
        return

    results = []
    for topic in topics:
        result = ingest_topic(
            topic=topic,
            start=start,
            end=end,
            page_size=args.page_size,
            min_window_hours=args.min_window_hours,
            write_entities=not args.skip_entities,
            write_chroma=not args.skip_chroma,
        )
        results.append(result)

    totals = {
        "topics": list(topics),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "fetched": sum(item["fetched"] for item in results),
        "inserted_or_updated": sum(item["inserted_or_updated"] for item in results),
        "results": results,
    }
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    main()
