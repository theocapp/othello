from fastapi import HTTPException

from core.config import TOPICS
from corpus import (
    get_articles_by_urls,
    get_canonical_event,
    get_canonical_events,
    get_event_perspectives,
    list_observation_keys_for_event,
    load_cluster_assignment_evidence,
    load_claim_resolution_for_event_key,
    load_contradiction_record,
    load_event_identity_history,
    load_framing_signals_for_article_urls,
    load_materialized_story_clusters,
)
from services.headlines_service import _build_global_events, _build_topic_events
from structured_story_rollups import build_structured_story_clusters


def get_events_payload(limit: int = 12) -> dict:
    safe_limit = max(limit, 1)
    events = _build_global_events(limit=safe_limit)
    return {"events": events[:safe_limit], "count": len(events)}


def get_structured_events_payload(
    days: int = 3,
    limit: int = 12,
    country: str | None = None,
    event_type: str | None = None,
) -> dict:
    safe_days = max(1, min(days, 30))
    safe_limit = max(1, min(limit, 30))
    clusters = build_structured_story_clusters(
        days=safe_days,
        limit=safe_limit,
        country=country,
        event_type=event_type,
    )
    return {
        "dataset": "acled",
        "days": safe_days,
        "country": country,
        "event_type": event_type,
        "clusters": clusters,
        "count": len(clusters),
    }


def get_materialized_story_clusters_payload(
    topic: str | None = None,
    window_hours: int | None = None,
    limit: int = 40,
) -> dict:
    wh = int(window_hours) if window_hours is not None else None
    rows = load_materialized_story_clusters(
        topic=topic,
        window_hours=wh,
        limit=max(1, min(limit, 200)),
    )
    return {"topic": topic, "window_hours": wh, "clusters": rows, "count": len(rows)}


def get_topic_events_payload(topic: str, limit: int = 8) -> dict:
    if topic not in TOPICS:
        raise HTTPException(status_code=400, detail=f"Topic must be one of {TOPICS}")
    safe_limit = max(limit, 1)
    events = _build_topic_events(topic, limit=safe_limit)
    return {"topic": topic, "events": events[:safe_limit], "count": len(events)}


def get_canonical_events_payload(
    topic: str | None = None,
    status: str | None = None,
    limit: int = 40,
) -> dict:
    events = get_canonical_events(
        topic=topic,
        status=status,
        limit=max(1, min(limit, 200)),
    )
    return {"topic": topic, "status": status, "events": events, "count": len(events)}


def get_canonical_event_payload(event_id: str) -> dict:
    event = get_canonical_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    perspectives = get_event_perspectives(event_id)
    # Summarize perspective diversity for the response
    frames = [p["dominant_frame"] for p in perspectives if p.get("dominant_frame")]
    frame_distribution: dict[str, int] = {}
    for f in frames:
        frame_distribution[f] = frame_distribution.get(f, 0) + 1
    sources_agreeing = [
        p["source_name"]
        for p in perspectives
        if p.get("claim_resolution_status") == "corroborated"
    ]
    sources_dissenting = [
        p["source_name"]
        for p in perspectives
        if p.get("claim_resolution_status") == "contradicted"
    ]
    return {
        **event,
        "perspectives": perspectives,
        "frame_distribution": frame_distribution,
        "sources_agreeing": sources_agreeing,
        "sources_dissenting": sources_dissenting,
    }


def get_event_perspectives_payload(event_id: str) -> dict:
    event = get_canonical_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    perspectives = get_event_perspectives(event_id)
    return {
        "event_id": event_id,
        "event_label": event["label"],
        "topic": event["topic"],
        "perspectives": perspectives,
        "count": len(perspectives),
    }


def get_canonical_event_debug_payload(event_id: str) -> dict:
    event = get_canonical_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    perspectives = get_event_perspectives(event_id)
    article_urls = [
        str(url).strip()
        for url in (event.get("article_urls") or [])
        if str(url).strip()
    ]
    if not article_urls:
        article_urls = [
            str(p.get("article_url")).strip()
            for p in perspectives
            if str(p.get("article_url") or "").strip()
        ]

    articles_by_url = get_articles_by_urls(article_urls, limit=160)
    framing_by_url = load_framing_signals_for_article_urls(article_urls)

    observation_keys = list_observation_keys_for_event(event_id, limit=40)
    identity_history = load_event_identity_history(event_id, limit=80)
    evidence_by_observation = load_cluster_assignment_evidence(
        observation_keys,
        limit_per_observation=120,
    )

    contradiction_records: list[dict] = []
    claim_records: list[dict] = []
    cluster_assignment_evidence: list[dict] = []
    seen_claim_ids: set[str] = set()
    for obs_key in observation_keys:
        cluster_assignment_evidence.extend(evidence_by_observation.get(obs_key) or [])
        contradiction = load_contradiction_record(obs_key)
        if contradiction:
            contradiction_records.append(
                {
                    **contradiction,
                    "observation_key": obs_key,
                }
            )
        for claim in load_claim_resolution_for_event_key(obs_key):
            claim_id = str(claim.get("claim_record_key") or "").strip()
            if claim_id and claim_id in seen_claim_ids:
                continue
            if claim_id:
                seen_claim_ids.add(claim_id)
            claim_records.append({**claim, "observation_key": obs_key})

    debug_articles = []
    for url in article_urls:
        article = articles_by_url.get(url)
        if article is None:
            continue
        debug_articles.append(
            {
                "url": article.get("url"),
                "title": article.get("title"),
                "description": article.get("description"),
                "source": article.get("source"),
                "source_domain": article.get("source_domain"),
                "published_at": article.get("published_at"),
                "language": article.get("language"),
            }
        )

    return {
        "event": {
            **event,
            "importance": {
                "score": event.get("importance_score") or 0,
                "reasons": event.get("importance_reasons") or [],
                "breakdown": ((event.get("payload") or {}).get("importance") or {}).get("breakdown")
                or {},
            },
        },
        "observation_keys": observation_keys,
        "identity_history": identity_history,
        "articles": debug_articles,
        "perspectives": perspectives,
        "framing_by_article_url": framing_by_url,
        "cluster_assignment_evidence": cluster_assignment_evidence,
        "claims": claim_records,
        "contradictions": contradiction_records,
        "counts": {
            "articles": len(debug_articles),
            "perspectives": len(perspectives),
            "cluster_assignment_evidence": len(cluster_assignment_evidence),
            "claims": len(claim_records),
            "contradictions": len(contradiction_records),
            "identity_events": len(identity_history),
            "observation_keys": len(observation_keys),
        },
    }
