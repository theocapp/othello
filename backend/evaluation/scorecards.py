"""Scorecard aggregation for human annotation labels."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from core.config import EVALUATION_LABELS_DIR
from evaluation.labels import SUPPORTED_KINDS, validate_annotation_record


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _list_label_files(labels_dir: Path, limit_files: int) -> list[Path]:
    if not labels_dir.exists() or not labels_dir.is_dir():
        return []
    files = sorted(path for path in labels_dir.glob("*.jsonl") if path.is_file())
    safe_limit = max(1, min(_safe_int(limit_files, 50), 5000))
    if len(files) <= safe_limit:
        return files
    return files[-safe_limit:]


def _agreement_metrics(records: list[dict]) -> dict:
    by_event: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        event_id = str(record.get("event_id") or "").strip()
        if not event_id:
            continue
        by_event[event_id].append(record)

    per_event_scores = []
    for event_id, event_rows in sorted(by_event.items()):
        # Keep one label per annotator per event to avoid accidental duplicates.
        latest_by_annotator: dict[str, dict] = {}
        for row in event_rows:
            annotator = str(row.get("annotator_id") or "").strip()
            if not annotator:
                continue
            latest_by_annotator[annotator] = row
        deduped_rows = list(latest_by_annotator.values())
        if len(deduped_rows) < 2:
            continue

        decision_counts = Counter(
            str(row.get("label_decision") or "").strip() for row in deduped_rows
        )
        total = sum(decision_counts.values())
        if total <= 0:
            continue
        event_agreement = max(decision_counts.values()) / total
        per_event_scores.append((event_id, event_agreement))

    if not per_event_scores:
        return {
            "comparable_event_count": 0,
            "agreement_rate": None,
        }

    agreement_rate = sum(score for _, score in per_event_scores) / len(per_event_scores)
    return {
        "comparable_event_count": len(per_event_scores),
        "agreement_rate": round(agreement_rate, 4),
    }


def build_scorecard_snapshot(
    *,
    kind: str | None = None,
    topic: str | None = None,
    labels_dir: str | None = None,
    limit_files: int = 80,
    include_error_samples: bool = False,
) -> dict:
    requested_kind = (kind or "").strip().lower() or None
    requested_topic = (topic or "").strip().lower() or None
    labels_path = Path(labels_dir or EVALUATION_LABELS_DIR)

    files = _list_label_files(labels_path, limit_files)

    records_scanned = 0
    valid_records = 0
    invalid_records = 0
    considered: list[dict] = []
    error_samples: list[dict] = []

    for file_path in files:
        with file_path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                text = raw_line.strip()
                if not text:
                    continue
                records_scanned += 1

                try:
                    payload = json.loads(text)
                except json.JSONDecodeError as exc:
                    invalid_records += 1
                    if include_error_samples and len(error_samples) < 50:
                        error_samples.append(
                            {
                                "file": file_path.name,
                                "line": line_number,
                                "error": f"invalid json: {exc.msg}",
                            }
                        )
                    continue

                if not isinstance(payload, dict):
                    invalid_records += 1
                    if include_error_samples and len(error_samples) < 50:
                        error_samples.append(
                            {
                                "file": file_path.name,
                                "line": line_number,
                                "error": "record is not a JSON object",
                            }
                        )
                    continue

                errors = validate_annotation_record(payload)
                if errors:
                    invalid_records += 1
                    if include_error_samples and len(error_samples) < 50:
                        error_samples.append(
                            {
                                "file": file_path.name,
                                "line": line_number,
                                "error": "; ".join(errors),
                            }
                        )
                    continue

                valid_records += 1
                payload_kind = str(payload.get("kind") or "").strip().lower()
                payload_topic = str(payload.get("topic") or "").strip().lower() or None
                if requested_kind and payload_kind != requested_kind:
                    continue
                if requested_topic and payload_topic != requested_topic:
                    continue
                considered.append(payload)

    by_kind: dict[str, list[dict]] = defaultdict(list)
    for row in considered:
        by_kind[str(row.get("kind") or "unknown").strip().lower()].append(row)

    kind_summaries: dict[str, dict] = {}
    for k in sorted(by_kind):
        rows = by_kind[k]
        decisions = Counter(str(row.get("label_decision") or "").strip() for row in rows)
        topics = Counter(str(row.get("topic") or "unknown").strip().lower() for row in rows)
        annotators = {
            str(row.get("annotator_id") or "").strip() for row in rows if str(row.get("annotator_id") or "").strip()
        }

        summary = {
            "record_count": len(rows),
            "event_count": len(
                {
                    str(row.get("event_id") or "").strip()
                    for row in rows
                    if str(row.get("event_id") or "").strip()
                }
            ),
            "annotator_count": len(annotators),
            "decision_counts": dict(sorted(decisions.items())),
            "topic_counts": dict(sorted(topics.items())),
            **_agreement_metrics(rows),
        }

        if k == "summary":
            quality_values = [
                int(row.get("overall_quality"))
                for row in rows
                if isinstance(row.get("overall_quality"), int)
            ]
            summary["avg_overall_quality"] = (
                round(sum(quality_values) / len(quality_values), 4)
                if quality_values
                else None
            )

        if k == "contradiction":
            citation_values = [
                bool(row.get("citation_adequate"))
                for row in rows
                if isinstance(row.get("citation_adequate"), bool)
            ]
            summary["citation_adequate_rate"] = (
                round(sum(1 for value in citation_values if value) / len(citation_values), 4)
                if citation_values
                else None
            )

        if k == "importance":
            buckets = Counter(
                str(row.get("expected_rank_bucket") or "").strip() for row in rows
            )
            summary["expected_rank_bucket_counts"] = dict(sorted(buckets.items()))

        kind_summaries[k] = summary

    topic_counts = Counter(
        str(row.get("topic") or "unknown").strip().lower() for row in considered
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "labels_dir": str(labels_path),
        "filters": {
            "kind": requested_kind,
            "topic": requested_topic,
        },
        "files_scanned": len(files),
        "file_names": [path.name for path in files],
        "records_scanned": records_scanned,
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "records_considered": len(considered),
        "supported_kinds": sorted(SUPPORTED_KINDS),
        "kind_summaries": kind_summaries,
        "topic_counts": dict(sorted(topic_counts.items())),
        "error_samples": error_samples if include_error_samples else [],
    }
