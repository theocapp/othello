from __future__ import annotations

from datetime import datetime, timezone

from core.runtime import parse_timestamp


IMPORTANCE_BUCKETS = (
    (85.0, "critical"),
    (65.0, "high"),
    (40.0, "medium"),
    (0.0, "low"),
)


def _freshness_points(event: dict) -> tuple[float, str]:
    latest = parse_timestamp(event.get("latest_update"))
    if not latest:
        return 6.0, "freshness unknown"

    age_hours = max(0.0, (datetime.now(timezone.utc) - latest).total_seconds() / 3600)
    if age_hours <= 6:
        return 22.0, "updated within 6h"
    if age_hours <= 12:
        return 18.0, "updated within 12h"
    if age_hours <= 24:
        return 13.0, "updated within 24h"
    if age_hours <= 48:
        return 8.0, "updated within 48h"
    return 3.0, "older than 48h"


def _bucket_for(score: float) -> str:
    for threshold, label in IMPORTANCE_BUCKETS:
        if score >= threshold:
            return label
    return "low"


def compute_event_importance(event: dict) -> dict:
    source_count = int(event.get("source_count", 0) or 0)
    article_count = int(event.get("article_count", 0) or 0)
    tier_1_source_count = int(event.get("tier_1_source_count", 0) or 0)
    contradiction_count = int(event.get("contradiction_count", 0) or 0)
    entity_focus_count = len(event.get("entity_focus") or [])

    freshness_points, freshness_label = _freshness_points(event)
    source_points = min(30.0, source_count * 5.0)
    article_points = min(18.0, article_count * 1.5)
    tier_1_points = min(14.0, tier_1_source_count * 4.0)
    contradiction_points = min(10.0, contradiction_count * 2.5)
    entity_points = min(6.0, entity_focus_count * 1.5)

    score = round(
        freshness_points
        + source_points
        + article_points
        + tier_1_points
        + contradiction_points
        + entity_points,
        2,
    )
    bucket = _bucket_for(score)

    reasons = [freshness_label]
    if source_count:
        reasons.append(f"{source_count} sources")
    if tier_1_source_count:
        reasons.append(f"{tier_1_source_count} tier-1 sources")
    if contradiction_count:
        reasons.append(f"{contradiction_count} contradiction flags")

    return {
        "importance_score": score,
        "importance_bucket": bucket,
        "importance_reason": ", ".join(reasons[:4]),
        "importance_breakdown": {
            "freshness": round(freshness_points, 2),
            "sources": round(source_points, 2),
            "articles": round(article_points, 2),
            "tier_1_sources": round(tier_1_points, 2),
            "contradictions": round(contradiction_points, 2),
            "entities": round(entity_points, 2),
        },
    }


def annotate_event_importance(event: dict) -> dict:
    return {
        **event,
        **compute_event_importance(event),
    }


def annotate_event_collection(events: list[dict]) -> list[dict]:
    return [annotate_event_importance(event) for event in events]
