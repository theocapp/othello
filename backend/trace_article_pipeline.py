from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from clustering import (  # noqa: E402
    build_article_signatures,
    build_observation_groups,
    cluster_articles,
    relatedness_score,
)
from contradictions import enrich_events  # noqa: E402
from corpus import get_recent_articles  # noqa: E402
from news import article_quality_score, infer_article_topics, should_promote_article  # noqa: E402
from story_materialization import rebuild_materialized_story_clusters  # noqa: E402


def _load_articles(args: argparse.Namespace) -> list[dict]:
    if args.input_json:
        with open(args.input_json, "r", encoding="utf-8") as handle:
            return json.load(handle)
    if args.topic:
        return get_recent_articles(
            topic=args.topic,
            limit=args.limit,
            hours=args.hours,
            headline_corpus_only=args.headline_corpus_only,
        )
    raise SystemExit("Provide either --input-json or --topic")


def _print_article_stage(articles: list[dict], topic: str | None) -> None:
    print("RAW ARTICLES")
    print(f"count={len(articles)}")
    for index, article in enumerate(articles, 1):
        topics = infer_article_topics(article)
        promote = should_promote_article(article, topics or ([topic] if topic else []))
        quality = article_quality_score(article, topics or ([topic] if topic else []))
        print(
            f"{index:02d} promote={promote} quality={quality} "
            f"source={article.get('source')!r} published_at={article.get('published_at')!r}"
        )
        print(f"    title={article.get('title')!r}")
        if article.get("description"):
            print(f"    description={article.get('description')!r}")
        if topics:
            print(f"    inferred_topics={topics}")


def _print_clustering_stage(articles: list[dict], topic: str | None) -> list[dict]:
    signatures = build_article_signatures(articles)
    groups = build_observation_groups(signatures)
    print()
    print("CLUSTERING")
    print(f"groups={len(groups)}")
    for group_index, group in enumerate(groups, 1):
        print(f"group {group_index}: article_indexes={group}")
        cluster = [articles[i] for i in group]
        events = cluster_articles(cluster, topic=topic)
        for event in events[:1]:
            print(
                f"    label={event.get('label')!r} sources={event.get('source_count')} "
                f"articles={event.get('article_count')} cohesion={event.get('cluster_cohesion')}"
            )
    if len(signatures) >= 2:
        print()
        print("PAIRWISE SAMPLE SCORES")
        sample_count = 0
        for left_index in range(len(signatures)):
            for right_index in range(left_index + 1, len(signatures)):
                print(
                    f"{left_index:02d}-{right_index:02d} score={relatedness_score(signatures[left_index], signatures[right_index])}"
                )
                sample_count += 1
                if sample_count >= 10:
                    return cluster_articles(articles, topic=topic)
    return cluster_articles(articles, topic=topic)


def _print_contradiction_stage(events: list[dict]) -> list[dict]:
    enriched = enrich_events(events)
    print()
    print("CONTRADICTIONS")
    print(f"events={len(enriched)}")
    for event in enriched[:10]:
        print(
            f"label={event.get('label')!r} contradiction_count={event.get('contradiction_count')} "
            f"analysis_priority={event.get('analysis_priority')}"
        )
        contradictions = event.get("contradictions") or []
        for contradiction in contradictions[:2]:
            print(
                f"    type={contradiction.get('conflict_type')} sources={contradiction.get('sources_in_conflict')} "
                f"confidence={contradiction.get('confidence')}"
            )
            print(f"    claim_a={contradiction.get('claim_a')!r}")
            print(f"    claim_b={contradiction.get('claim_b')!r}")
    return enriched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trace a raw article list through clustering, contradiction enrichment, and optional materialization."
    )
    parser.add_argument("--topic", help="Topic to load recent articles for.")
    parser.add_argument("--input-json", help="Path to a JSON file containing a raw article list.")
    parser.add_argument("--limit", type=int, default=40, help="Maximum number of articles to load when using --topic.")
    parser.add_argument("--hours", type=int, default=96, help="Lookback window in hours when using --topic.")
    parser.add_argument(
        "--headline-corpus-only",
        action="store_true",
        help="Restrict topic loading to the headline corpus.",
    )
    parser.add_argument(
        "--materialize",
        action="store_true",
        help="Also persist materialized story clusters after the trace run.",
    )
    parser.add_argument(
        "--materialize-window-hours",
        type=int,
        default=96,
        help="Window used when --materialize is enabled.",
    )
    parser.add_argument(
        "--materialize-limit",
        type=int,
        default=120,
        help="Article limit used when --materialize is enabled.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    articles = _load_articles(args)
    _print_article_stage(articles, args.topic)
    events = _print_clustering_stage(articles, args.topic)
    enriched = _print_contradiction_stage(events)

    if args.materialize:
        print()
        print("MATERIALIZATION")
        result = rebuild_materialized_story_clusters(
            topics=[args.topic] if args.topic else None,
            window_hours=args.materialize_window_hours,
            articles_limit=args.materialize_limit,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        print(f"enriched_events={len(enriched)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())