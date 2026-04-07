"""Seed valid clustering label rows from live canonical events.

This is a local QA utility for bootstrapping label coverage so scorecards
have non-zero records_considered during strict QA runs.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from evaluation.labels import DECISIONS_BY_KIND, validate_annotation_record  # noqa: E402
from services.events_service import (  # noqa: E402
    get_canonical_event_debug_payload,
    get_canonical_events_payload,
)


def _safe_event_id(event: dict) -> str:
    return str((event or {}).get("event_id") or "").strip()


def seed_labels(
    *,
    topic: str,
    event_limit: int,
    decision: str,
    annotator_id: str,
    output: Path,
    allow_event_fallback: bool,
) -> int:
    if decision not in DECISIONS_BY_KIND["clustering"]:
        raise ValueError(
            "decision must be one of: " + ", ".join(sorted(DECISIONS_BY_KIND["clustering"]))
        )

    selected_limit = max(1, min(int(event_limit), 200))
    events_payload = get_canonical_events_payload(topic=topic, status=None, limit=selected_limit)
    events = events_payload.get("events") or []
    event_ids = [_safe_event_id(event) for event in events if _safe_event_id(event)]

    rows: list[dict] = []
    seen_observation_keys: set[str] = set()
    for event_id in event_ids:
        debug_payload = get_canonical_event_debug_payload(event_id)
        observation_keys = [
            str(key).strip()
            for key in (debug_payload.get("observation_keys") or [])
            if str(key).strip()
        ]
        if not observation_keys and not allow_event_fallback:
            continue

        observation_key = (
            observation_keys[0]
            if observation_keys
            else f"seed_obs:{event_id}"
        )
        if observation_key in seen_observation_keys:
            continue
        seen_observation_keys.add(observation_key)

        row = {
            "schema_version": "v1-draft",
            "kind": "clustering",
            "event_id": event_id,
            "observation_key": observation_key,
            "label_decision": decision,
            "annotator_id": annotator_id,
            "annotated_at": datetime.now(timezone.utc).isoformat(),
            "topic": topic,
            "notes": "seeded local QA label",
        }
        errors = validate_annotation_record(row)
        if errors:
            raise ValueError(
                f"generated row failed validation for event_id={event_id}: {'; '.join(errors)}"
            )
        rows.append(row)

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    print(f"seeded {len(rows)} clustering labels into {output}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed clustering labels from canonical events")
    parser.add_argument("--topic", default="geopolitics")
    parser.add_argument("--event-limit", type=int, default=12)
    parser.add_argument(
        "--decision",
        default="unsure",
        choices=sorted(DECISIONS_BY_KIND["clustering"]),
    )
    parser.add_argument("--annotator-id", default="qa_seed")
    parser.add_argument(
        "--output",
        default=str(_BACKEND_ROOT / "evaluation" / "batches" / "clustering_seeded.jsonl"),
    )
    parser.add_argument(
        "--no-event-fallback",
        action="store_true",
        help="Disable fallback synthetic observation keys when debug payload has none.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return seed_labels(
        topic=str(args.topic or "geopolitics"),
        event_limit=int(args.event_limit),
        decision=str(args.decision or "unsure"),
        annotator_id=str(args.annotator_id or "qa_seed"),
        output=Path(args.output),
        allow_event_fallback=not bool(args.no_event_fallback),
    )


if __name__ == "__main__":
    raise SystemExit(main())