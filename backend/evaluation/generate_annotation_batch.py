"""Generate deterministic JSONL annotation batches from canonical events."""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from corpus import get_canonical_events, load_materialized_story_clusters  # noqa: E402

SUPPORTED_KINDS = {"clustering", "importance", "contradiction", "summary"}


def _draft_label_payload(kind: str) -> dict:
    if kind == "clustering":
        return {
            "observation_key": None,
            "label_decision": None,
            "notes": "",
        }
    if kind == "importance":
        return {
            "expected_rank_bucket": None,
            "label_decision": None,
            "notes": "",
        }
    if kind == "contradiction":
        return {
            "contradiction_id": None,
            "citation_adequate": None,
            "label_decision": None,
            "notes": "",
        }
    return {
        "overall_quality": None,
        "label_decision": None,
        "notes": "",
    }


def generate_batch(
    *,
    kind: str,
    topic: str | None,
    limit: int,
    seed: int,
) -> list[dict]:
    if kind not in SUPPORTED_KINDS:
        raise ValueError(f"Unsupported kind: {kind}")

    limit = max(1, min(int(limit), 1000))
    candidate_count = max(limit * 4, 80)

    # Prefer canonical events; fall back to materialized clusters when local DB
    # schema is behind (for example, missing canonical importance columns).
    try:
        events = get_canonical_events(topic=topic, status=None, limit=candidate_count)
    except Exception:
        materialized = load_materialized_story_clusters(
            topic=topic,
            window_hours=None,
            limit=candidate_count,
        )
        events = [
            {
                "event_id": row.get("cluster_key"),
                "label": row.get("label"),
                "topic": row.get("topic"),
                "importance_score": 0.0,
                "importance_reasons": [],
                "article_count": len(row.get("article_urls") or []),
                "source_count": int(
                    len(
                        {
                            (a.get("source") or "").strip().lower()
                            for a in (row.get("event_payload") or {}).get("articles", [])
                            if (a.get("source") or "").strip()
                        }
                    )
                ),
                "contradiction_count": int(
                    len((row.get("event_payload") or {}).get("contradictions") or [])
                ),
                "status": "developing",
                "last_updated_at": row.get("latest_published_at"),
            }
            for row in materialized
            if row.get("cluster_key")
        ]

    ranked = sorted(
        events,
        key=lambda event: (
            -float(event.get("importance_score") or 0.0),
            str(event.get("event_id") or ""),
        ),
    )
    rng = random.Random(seed)
    rng.shuffle(ranked)

    rows = []
    for event in ranked[:limit]:
        rows.append(
            {
                "schema_version": "v1-draft",
                "kind": kind,
                "event_id": event.get("event_id"),
                "event_label": event.get("label"),
                "topic": event.get("topic"),
                "event_snapshot": {
                    "importance_score": event.get("importance_score"),
                    "importance_reasons": event.get("importance_reasons") or [],
                    "article_count": event.get("article_count"),
                    "source_count": event.get("source_count"),
                    "contradiction_count": event.get("contradiction_count"),
                    "status": event.get("status"),
                    "last_updated_at": event.get("last_updated_at"),
                },
                "annotator_id": None,
                "annotated_at": None,
                **_draft_label_payload(kind),
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate annotation batch JSONL")
    parser.add_argument("--kind", required=True, choices=sorted(SUPPORTED_KINDS))
    parser.add_argument("--output", required=True)
    parser.add_argument("--topic", default="geopolitics")
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--seed", type=int, default=13)
    args = parser.parse_args()

    rows = generate_batch(
        kind=args.kind,
        topic=args.topic,
        limit=args.limit,
        seed=args.seed,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    print(f"wrote {len(rows)} records to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
