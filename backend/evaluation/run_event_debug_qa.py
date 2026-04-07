"""Run a repeatable QA pass for canonical event debug and scorecard surfaces.

This script validates core Event Debug assumptions over live canonical events:

- Canonical event IDs remain stable across two consecutive fetches.
- Debug payloads resolve for sampled events and expose importance/cohesion fields.
- Scorecard endpoint returns clustering and cohesion operational metrics.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from services.analytics_service import evaluation_scorecard_payload  # noqa: E402
from services.events_service import (  # noqa: E402
    get_canonical_event_debug_payload,
    get_canonical_events_payload,
)


def _safe_event_id(event: dict) -> str:
    return str((event or {}).get("event_id") or "").strip()


def _validate_debug_payload(event_id: str, payload: dict) -> list[str]:
    failures: list[str] = []
    event = payload.get("event") or {}
    importance = event.get("importance") or {}
    counts = payload.get("counts") or {}
    cohesion = event.get("cluster_cohesion") or {}

    if str(event.get("event_id") or "").strip() != event_id:
        failures.append(f"event_id mismatch for {event_id}")
    if "score" not in importance:
        failures.append(f"importance.score missing for {event_id}")
    if not isinstance(importance.get("reasons") or [], list):
        failures.append(f"importance.reasons malformed for {event_id}")
    if not isinstance(payload.get("observation_keys") or [], list):
        failures.append(f"observation_keys missing for {event_id}")
    if not isinstance(payload.get("cluster_assignment_evidence") or [], list):
        failures.append(f"cluster_assignment_evidence missing for {event_id}")
    if "articles" not in counts:
        failures.append(f"counts.articles missing for {event_id}")
    if cohesion and "outlier_ratio" not in cohesion:
        failures.append(f"cluster_cohesion.outlier_ratio missing for {event_id}")
    return failures


def _validate_scorecard_payload(payload: dict) -> list[str]:
    failures: list[str] = []
    kind_summaries = payload.get("kind_summaries") or {}
    operational = payload.get("operational_metrics") or {}
    cohesion = operational.get("cluster_cohesion") or {}

    if not isinstance(kind_summaries, dict):
        failures.append("kind_summaries missing")
    if "cluster_cohesion" not in operational:
        failures.append("operational_metrics.cluster_cohesion missing")
    if cohesion and "high_outlier_event_rate" not in cohesion:
        failures.append("cluster_cohesion.high_outlier_event_rate missing")
    if cohesion and "high_outlier_threshold" not in cohesion:
        failures.append("cluster_cohesion.high_outlier_threshold missing")
    return failures


def run_qa(topic: str, event_limit: int, require_label_records: bool) -> int:
    selected_limit = max(1, min(int(event_limit), 20))

    first = get_canonical_events_payload(topic=topic, status=None, limit=max(40, selected_limit * 4))
    first_events = first.get("events") or []
    first_ids = [_safe_event_id(event) for event in first_events if _safe_event_id(event)]

    if not first_ids:
        print(f"[qa] no canonical events found for topic={topic}; nothing to validate")
        return 0

    sampled_ids = first_ids[:selected_limit]

    second = get_canonical_events_payload(topic=topic, status=None, limit=max(40, selected_limit * 4))
    second_ids = {_safe_event_id(event) for event in (second.get("events") or []) if _safe_event_id(event)}

    failures: list[str] = []
    missing_after_refresh = [event_id for event_id in sampled_ids if event_id not in second_ids]
    if missing_after_refresh:
        failures.append(
            "stability check failed: sampled event ids missing on second fetch: "
            + ", ".join(missing_after_refresh)
        )

    for event_id in sampled_ids:
        debug_payload = get_canonical_event_debug_payload(event_id)
        failures.extend(_validate_debug_payload(event_id, debug_payload))

    scorecard = evaluation_scorecard_payload(topic=topic, kind=None, limit_files=120)
    failures.extend(_validate_scorecard_payload(scorecard))
    records_considered = int(scorecard.get("records_considered") or 0)
    if require_label_records and records_considered <= 0:
        failures.append(
            "scorecard records_considered is 0 while --require-label-records is enabled"
        )

    print(f"[qa] topic={topic} sampled_events={len(sampled_ids)}")
    print(f"[qa] scorecard_records_considered={records_considered}")
    if records_considered <= 0:
        print(
            "[qa] note: no validated label records found; run with --require-label-records to enforce non-zero coverage"
        )

    if failures:
        print("[qa] FAIL")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("[qa] PASS")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Event Debug + scorecard QA checks")
    parser.add_argument("--topic", default="geopolitics")
    parser.add_argument("--event-limit", type=int, default=5)
    parser.add_argument(
        "--require-label-records",
        action="store_true",
        help="Fail QA when scorecard records_considered is zero.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_qa(
        topic=str(args.topic or "geopolitics"),
        event_limit=int(args.event_limit),
        require_label_records=bool(args.require_label_records),
    )


if __name__ == "__main__":
    raise SystemExit(main())