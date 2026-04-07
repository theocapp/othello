"""Validation helpers for human annotation labels."""

from __future__ import annotations

from datetime import datetime

COMMON_REQUIRED_FIELDS = (
    "schema_version",
    "kind",
    "event_id",
    "label_decision",
    "annotator_id",
    "annotated_at",
)

SUPPORTED_KINDS = {
    "clustering",
    "importance",
    "contradiction",
    "summary",
}

DECISIONS_BY_KIND = {
    "clustering": {
        "correct_cluster",
        "false_merge",
        "false_split",
        "duplicate",
        "unsure",
    },
    "importance": {
        "should_be_top_n",
        "ranked_too_high",
        "ranked_too_low",
        "unsure",
    },
    "contradiction": {
        "true_contradiction",
        "framing_difference",
        "noise",
        "unclear",
    },
    "summary": {
        "faithful",
        "missing_critical_facts",
        "hallucinated",
        "mixed",
    },
}


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_iso_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def validate_annotation_record(record: dict) -> list[str]:
    errors: list[str] = []

    for field in COMMON_REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"missing required field: {field}")

    kind = record.get("kind")
    if kind not in SUPPORTED_KINDS:
        errors.append(
            "kind must be one of: " + ", ".join(sorted(SUPPORTED_KINDS))
        )
        return errors

    if not _is_non_empty_string(record.get("schema_version")):
        errors.append("schema_version must be a non-empty string")
    if not _is_non_empty_string(record.get("event_id")):
        errors.append("event_id must be a non-empty string")
    if not _is_non_empty_string(record.get("annotator_id")):
        errors.append("annotator_id must be a non-empty string")
    if not _is_iso_timestamp(record.get("annotated_at")):
        errors.append("annotated_at must be an ISO-8601 timestamp")

    decision = record.get("label_decision")
    if decision not in DECISIONS_BY_KIND.get(kind, set()):
        errors.append(
            f"label_decision must be one of: {', '.join(sorted(DECISIONS_BY_KIND[kind]))}"
        )

    if kind == "clustering":
        if not _is_non_empty_string(record.get("observation_key")):
            errors.append("clustering labels require observation_key")

    if kind == "importance":
        expected_bucket = record.get("expected_rank_bucket")
        allowed = {"top_5", "top_10", "top_20", "not_top_20"}
        if expected_bucket not in allowed:
            errors.append(
                "importance labels require expected_rank_bucket in {top_5, top_10, top_20, not_top_20}"
            )

    if kind == "contradiction":
        citation_adequate = record.get("citation_adequate")
        if not isinstance(citation_adequate, bool):
            errors.append("contradiction labels require citation_adequate boolean")

    if kind == "summary":
        overall_quality = record.get("overall_quality")
        if not isinstance(overall_quality, int) or not (1 <= overall_quality <= 5):
            errors.append("summary labels require overall_quality integer in [1, 5]")

    return errors
