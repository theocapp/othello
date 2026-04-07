"""Persist cross-source story clusters for analytics and APIs."""

from __future__ import annotations

import hashlib
import math
import re
import time
from datetime import datetime, timedelta, timezone

from clustering import cluster_articles, event_cluster_key
from contradictions import enrich_events
from corpus import (
    append_event_identity_events,
    get_latest_canonical_event_observation,
    get_recent_articles,
    get_event_id_for_observation_key,
    get_source_registry,
    get_structured_event_coordinates_by_ids,
    list_structured_event_ids_in_date_range,
    list_canonical_identity_candidates,
    load_claim_resolution_for_event_key,
    load_framing_signals_for_article_urls,
    load_latest_source_reliability,
    replace_materialized_story_clusters,
    upsert_cluster_assignment_evidence,
    upsert_canonical_events,
    upsert_canonical_event_observations,
    upsert_event_identity_mappings,
    upsert_event_perspectives,
)
from event_identity import resolve_canonical_event_id
from causal import CausalGraph

DEFAULT_TOPICS = ("geopolitics", "economics")

_SOURCE_TIER_WEIGHTS = {
    "tier_1": 1.0,
    "tier_2": 0.72,
    "tier_3": 0.45,
}

_ESCALATION_ANCHORS = {
    "strike",
    "attack",
    "missile",
    "detention",
    "sanctions",
    "ceasefire",
    "filing",
    "vote",
}

_ASSIGNMENT_STOPWORDS = frozenset(
    {
        "the",
        "and",
        "for",
        "from",
        "with",
        "this",
        "that",
        "have",
        "has",
        "had",
        "into",
        "over",
        "under",
        "after",
        "before",
        "about",
        "amid",
        "amidst",
        "says",
        "said",
        "will",
        "were",
        "been",
        "their",
        "they",
        "them",
        "would",
        "could",
        "should",
        "also",
        "more",
    }
)

_ASSIGNMENT_TOKEN_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{1,}")


_COUNTRY_ALIASES = {
    "usa": "united states",
    "us": "united states",
    "u.s.": "united states",
    "u.s.a.": "united states",
    "united states of america": "united states",
    "uk": "united kingdom",
    "u.k.": "united kingdom",
    "great britain": "united kingdom",
    "britain": "united kingdom",
    "england": "united kingdom",
    "london": "united kingdom",
    "russian federation": "russia",
    "moscow": "russia",
    "iran": "iran",
    "tehran": "iran",
    "china": "china",
    "people's republic of china": "china",
    "prc": "china",
    "beijing": "china",
    "syrian arab republic": "syria",
    "damascus": "syria",
    "kyiv": "ukraine",
    "kiev": "ukraine",
    "ankara": "turkey",
    "uae": "united arab emirates",
    "ivory coast": "cote d'ivoire",
    "côte d'ivoire": "cote d'ivoire",
    "drc": "dr congo",
    "zaire": "dr congo",
    "kinshasa": "dr congo",
    "republic of the congo": "congo",
    "brazzaville": "congo",
    "south korea": "south korea",
    "republic of korea": "south korea",
    "rok": "south korea",
    "north korea": "north korea",
    "pyongyang": "north korea",
}

_COUNTRY_CANONICAL_NAMES = frozenset(_COUNTRY_ALIASES.values())
_COUNTRY_NAME_PATTERN = re.compile(
    r"\b("
    + "|".join(
        sorted(
            {
                re.escape(name)
                for name in set(_COUNTRY_ALIASES) | _COUNTRY_CANONICAL_NAMES
            },
            key=len,
            reverse=True,
        )
    )
    + r")\b",
    flags=re.I,
)


def _normalize_country(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = str(name).strip().lower()
    if cleaned.startswith("the "):
        cleaned = cleaned[4:]
    return _COUNTRY_ALIASES.get(cleaned, cleaned)


def _to_utc_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _tokenize_assignment_text(text: str | None) -> set[str]:
    if not text:
        return set()
    return {
        token
        for token in _ASSIGNMENT_TOKEN_PATTERN.findall(str(text).lower())
        if len(token) > 2 and token not in _ASSIGNMENT_STOPWORDS
    }


def _assignment_rule_for_features(
    *,
    entity_overlap: int,
    anchor_overlap: int,
    keyword_overlap: int,
    time_gap_hours: float | None,
    final_score: float,
) -> str:
    within_window = time_gap_hours is None or time_gap_hours <= 120
    compact_window = time_gap_hours is None or time_gap_hours <= 72
    if anchor_overlap >= 1 and entity_overlap >= 1 and within_window:
        return "anchor_entity_temporal"
    if entity_overlap >= 2 and keyword_overlap >= 2 and within_window:
        return "entity_keyword_temporal"
    if anchor_overlap >= 1 and keyword_overlap >= 3 and compact_window:
        return "anchor_keyword_compact"
    if final_score >= 4.1:
        return "score_threshold"
    return "fallback_in_cluster"


def _build_cluster_assignment_evidence_rows(
    *,
    event_id: str,
    topic: str,
    observation_key: str,
    event: dict,
) -> list[dict]:
    articles = event.get("articles") or []
    if not articles:
        return []

    entity_focus = [
        str(entity).strip().lower()
        for entity in (event.get("entity_focus") or [])
        if str(entity).strip()
    ]
    anchor_focus = sorted(_event_anchor_set(event))

    event_tokens = _tokenize_assignment_text(event.get("label"))
    event_tokens.update(_tokenize_assignment_text(event.get("summary")))
    for anchor in anchor_focus:
        event_tokens.update(_tokenize_assignment_text(anchor))
    for entity in entity_focus:
        event_tokens.update(_tokenize_assignment_text(entity))

    latest_dt = _to_utc_datetime(event.get("latest_update"))
    rows: list[dict] = []
    for article in articles:
        article_url = str(article.get("url") or "").strip()
        if not article_url:
            continue

        article_text = " ".join(
            [
                str(article.get("title") or "").strip(),
                str(article.get("description") or "").strip(),
            ]
        ).lower()
        article_tokens = _tokenize_assignment_text(article_text)
        matched_entities = [entity for entity in entity_focus if entity in article_text]
        matched_anchors = [anchor for anchor in anchor_focus if anchor in article_text]
        matched_keywords = sorted(article_tokens & event_tokens)

        entity_overlap = len(matched_entities)
        anchor_overlap = len(matched_anchors)
        keyword_overlap = len(matched_keywords)

        article_dt = _to_utc_datetime(article.get("published_at"))
        time_gap_hours = None
        if latest_dt is not None and article_dt is not None:
            time_gap_hours = abs((latest_dt - article_dt).total_seconds() / 3600)

        recency_component = (
            0.0
            if time_gap_hours is None
            else max(0.0, 2.8 - (time_gap_hours / 30.0))
        )
        final_score = round(
            (entity_overlap * 1.9)
            + (anchor_overlap * 2.1)
            + (keyword_overlap * 0.7)
            + recency_component,
            3,
        )
        rule = _assignment_rule_for_features(
            entity_overlap=entity_overlap,
            anchor_overlap=anchor_overlap,
            keyword_overlap=keyword_overlap,
            time_gap_hours=time_gap_hours,
            final_score=final_score,
        )

        rows.append(
            {
                "observation_key": observation_key,
                "event_id": event_id,
                "topic": topic,
                "article_url": article_url,
                "rule": rule,
                "entity_overlap": entity_overlap,
                "anchor_overlap": anchor_overlap,
                "keyword_overlap": keyword_overlap,
                "time_gap_hours": (
                    round(time_gap_hours, 3)
                    if time_gap_hours is not None
                    else None
                ),
                "final_score": final_score,
                "payload": {
                    "source": article.get("source"),
                    "source_domain": article.get("source_domain"),
                    "published_at": article.get("published_at"),
                    "matched_entities": matched_entities[:8],
                    "matched_anchors": matched_anchors[:8],
                    "matched_keywords": matched_keywords[:16],
                },
            }
        )
    return rows


def _event_anchor_set(event: dict) -> set[str]:
    anchors = event.get("story_anchor_focus") or event.get("anchors") or []
    return {str(a).strip().lower() for a in anchors if str(a).strip()}


def _build_importance_scoring_artifacts(
    event: dict,
    *,
    linked_structured_ids: list[str],
    reliability_by_source: dict[str, dict],
    registry_by_domain: dict[str, dict],
    latest_observation: dict | None,
    structured_meta_by_id: dict[str, dict],
) -> tuple[float, list[str], dict]:
    articles = event.get("articles") or []
    source_rows: dict[str, dict] = {}
    region_set: set[str] = set()
    language_set: set[str] = set()

    for article in articles:
        source_name = (article.get("source") or "").strip()
        if not source_name:
            continue
        source_key = source_name.lower()
        source_domain = (article.get("source_domain") or "").strip().lower()
        reg = registry_by_domain.get(source_domain) or {}
        reliability = reliability_by_source.get(source_key) or {}
        tier = str(reg.get("trust_tier") or "tier_3").strip().lower()
        if tier not in _SOURCE_TIER_WEIGHTS:
            tier = "tier_3"
        empirical_score = float(reliability.get("empirical_score") or 0.5)
        tier_weight = _SOURCE_TIER_WEIGHTS.get(tier, 0.45)
        region = str(reg.get("region") or "global").strip().lower() or "global"
        language = (
            str(article.get("language") or reg.get("language") or "unknown")
            .strip()
            .lower()
            or "unknown"
        )

        region_set.add(region)
        language_set.add(language)

        if source_key not in source_rows:
            source_rows[source_key] = {
                "tier": tier,
                "empirical_score": empirical_score,
                "credibility": empirical_score * tier_weight,
            }

    source_count = len(source_rows)
    tier_1_source_count = sum(
        1 for row in source_rows.values() if row.get("tier") == "tier_1"
    )
    credibility_values = [float(row["credibility"]) for row in source_rows.values()]
    avg_credibility = (
        sum(credibility_values) / len(credibility_values) if credibility_values else 0.0
    )

    source_credibility_score = min(
        35.0,
        (source_count * 3.2) + (tier_1_source_count * 4.0) + (avg_credibility * 18.0),
    )
    diversity_score = min(
        15.0,
        (max(0, len(region_set) - 1) * 3.5)
        + (max(0, len(language_set) - 1) * 1.7)
        + min(source_count, 3),
    )

    entities = [e for e in (event.get("entity_focus") or []) if str(e).strip()]
    entity_prominence_score = min(10.0, len(entities) * 1.4)

    anchors = _event_anchor_set(event)
    escalation_hits = sorted(anchor for anchor in anchors if anchor in _ESCALATION_ANCHORS)
    contradiction_count = int(len(event.get("contradictions") or []))
    total_fatalities = sum(
        int(meta.get("fatalities") or 0) for meta in structured_meta_by_id.values()
    )

    fatality_score = min(14.0, math.log1p(total_fatalities) * 3.5) if total_fatalities > 0 else 0.0
    anchor_score = min(8.0, len(escalation_hits) * 2.0)
    contradiction_score = min(6.0, contradiction_count * 1.8)
    structured_signal_score = min(5.0, len(linked_structured_ids) * 1.25)
    severity_escalation_score = min(
        25.0,
        fatality_score + anchor_score + contradiction_score + structured_signal_score,
    )

    article_count = int(len([a for a in articles if (a.get("url") or "").strip()]))
    growth_base = 0.0
    growth_reasons: list[str] = []
    if latest_observation is None:
        growth_base = 6.0
        growth_reasons.append("newly observed canonical event")
    else:
        article_delta = max(0, article_count - int(latest_observation.get("article_count") or 0))
        source_delta = max(0, source_count - int(latest_observation.get("source_count") or 0))
        contradiction_delta = max(
            0,
            contradiction_count - int(latest_observation.get("contradiction_count") or 0),
        )
        growth_base = min(
            12.0,
            (article_delta * 1.8)
            + (source_delta * 2.5)
            + (contradiction_delta * 1.2),
        )
        if article_delta > 0 or source_delta > 0:
            growth_reasons.append(
                f"coverage growth (+{article_delta} articles, +{source_delta} sources)"
            )

    recency_bonus = 0.0
    latest_dt = _to_utc_datetime(event.get("latest_update"))
    if latest_dt is not None:
        age_hours = max(0.0, (datetime.now(timezone.utc) - latest_dt).total_seconds() / 3600)
        if age_hours <= 6:
            recency_bonus = 3.0
        elif age_hours <= 24:
            recency_bonus = 2.0
        elif age_hours <= 48:
            recency_bonus = 1.0
    growth_novelty_score = min(15.0, growth_base + recency_bonus)

    raw_score = (
        source_credibility_score
        + diversity_score
        + entity_prominence_score
        + severity_escalation_score
        + growth_novelty_score
    )
    importance_score = round(max(0.0, min(100.0, raw_score)), 2)

    weighted_reasons = [
        (
            source_credibility_score,
            f"{source_count} distinct sources ({tier_1_source_count} tier-1)",
        ),
        (
            diversity_score,
            f"source diversity across {len(region_set)} regions and {len(language_set)} languages",
        ),
        (
            severity_escalation_score,
            (
                f"escalation signals: {', '.join(escalation_hits[:3])}"
                if escalation_hits
                else "escalation/severity context from linked signals"
            ),
        ),
        (
            growth_novelty_score,
            growth_reasons[0] if growth_reasons else "recent activity in the last 48h",
        ),
    ]
    if total_fatalities > 0:
        weighted_reasons.append((fatality_score, f"{total_fatalities} linked fatalities"))
    if contradiction_count > 0:
        weighted_reasons.append(
            (contradiction_score, f"{contradiction_count} cross-source contradictions")
        )

    weighted_reasons.sort(key=lambda item: item[0], reverse=True)
    importance_reasons = [text for score, text in weighted_reasons if score > 0][:5]

    breakdown = {
        "source_credibility": round(source_credibility_score, 2),
        "source_diversity": round(diversity_score, 2),
        "entity_prominence": round(entity_prominence_score, 2),
        "severity_escalation": round(severity_escalation_score, 2),
        "growth_novelty": round(growth_novelty_score, 2),
        "metrics": {
            "source_count": source_count,
            "tier_1_source_count": tier_1_source_count,
            "avg_source_credibility": round(avg_credibility, 4),
            "region_count": len(region_set),
            "language_count": len(language_set),
            "entity_count": len(entities),
            "linked_structured_event_count": len(linked_structured_ids),
            "total_linked_fatalities": total_fatalities,
            "contradiction_count": contradiction_count,
            "escalation_anchor_hits": escalation_hits,
        },
    }
    return importance_score, importance_reasons, breakdown


def _countries_from_text(text: str | None) -> list[str]:
    if not text:
        return []
    countries: list[str] = []
    for raw in _COUNTRY_NAME_PATTERN.findall(text):
        normalized = _normalize_country(raw)
        if normalized and normalized not in countries:
            countries.append(normalized)
    return countries


def _date_range_for_cluster(
    event: dict, padding_days: int = 2
) -> tuple[str | None, str | None]:
    def to_date(raw: str | None):
        if not raw or not str(raw).strip():
            return None
        text = str(raw).strip()
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt.date()
        except ValueError:
            return None

    earliest = to_date(event.get("earliest_update")) or to_date(
        event.get("latest_update")
    )
    latest = to_date(event.get("latest_update")) or earliest
    if earliest is None:
        return None, None
    if latest is None:
        latest = earliest
    start = earliest - timedelta(days=padding_days)
    end = latest + timedelta(days=padding_days)
    return start.isoformat(), end.isoformat()


def _story_country_preferences(event: dict) -> list[str]:
    preferences: list[str] = []
    for raw_focus in event.get("entity_focus") or []:
        if not raw_focus or not str(raw_focus).strip():
            continue
        normalized = _normalize_country(str(raw_focus))
        canonical = _COUNTRY_ALIASES.get(normalized, normalized)
        if canonical in _COUNTRY_CANONICAL_NAMES and canonical not in preferences:
            preferences.append(canonical)

    for country in _countries_from_text(event.get("label")) + _countries_from_text(
        event.get("summary")
    ):
        if country not in preferences:
            preferences.append(country)

    if "united states" in preferences and len(preferences) > 1:
        preferences = [country for country in preferences if country != "united states"]
    return preferences


def _link_structured_ids(event: dict) -> list[str]:
    start, end = _date_range_for_cluster(event)
    if not start or not end:
        return []

    candidate_ids = list_structured_event_ids_in_date_range(start, end, limit=80)
    if not candidate_ids:
        return []

    preferred_countries = _story_country_preferences(event)
    if not preferred_countries:
        return candidate_ids

    coord_by_id = get_structured_event_coordinates_by_ids(candidate_ids)
    filtered_ids = []
    for event_id, meta in coord_by_id.items():
        country = _normalize_country(meta.get("country"))
        if country and country in preferred_countries:
            filtered_ids.append(event_id)
    return filtered_ids or candidate_ids


def _perspective_id(event_id: str, article_url: str) -> str:
    return hashlib.sha256(f"{event_id}|{article_url}".encode()).hexdigest()


def _infer_event_type(event: dict) -> str | None:
    label = (event.get("label") or "").lower()
    anchors = _event_anchor_set(event)
    if (
        "strike" in anchors
        or "ceasefire" in anchors
        or any(
            w in label
            for w in ("war", "attack", "conflict", "military", "troops", "missile")
        )
    ):
        return "conflict"
    if (
        "sanctions" in anchors
        or "market" in anchors
        or any(
            w in label
            for w in ("trade", "tariff", "economy", "inflation", "rates", "gdp")
        )
    ):
        return "economic"
    if (
        "meeting" in anchors
        or "vote" in anchors
        or any(
            w in label for w in ("election", "summit", "diplomacy", "treaty", "talks")
        )
    ):
        return "diplomatic"
    if "aid" in anchors or any(
        w in label for w in ("humanitarian", "relief", "refugee")
    ):
        return "humanitarian"
    if (
        "filing" in anchors
        or "detention" in anchors
        or any(w in label for w in ("legal", "court", "indictment", "arrest"))
    ):
        return "legal"
    return "political"


def _build_canonical_row(
    event: dict,
    topic: str,
    linked: list[str],
    urls: list[str],
    cluster_key: str,
    *,
    importance_score: float,
    importance_reasons: list[str],
    importance_breakdown: dict,
) -> dict:
    articles = event.get("articles") or []
    sources = {
        (a.get("source") or "").strip()
        for a in articles
        if (a.get("source") or "").strip()
    }
    published_dates = [a.get("published_at") for a in articles if a.get("published_at")]
    first_reported = min(published_dates) if published_dates else None
    last_updated = max(published_dates) if published_dates else None
    return {
        "event_id": cluster_key,
        "topic": topic,
        "label": event.get("label") or "",
        "event_type": _infer_event_type(event),
        "status": "developing",
        "first_reported_at": first_reported,
        "last_updated_at": last_updated,
        "article_count": len(urls),
        "source_count": len(sources),
        "perspective_count": 0,
        "contradiction_count": len(event.get("contradictions") or []),
        "importance_score": importance_score,
        "importance_reasons": importance_reasons,
        "linked_structured_event_ids": linked,
        "article_urls": urls,
        "payload": {
            "summary": event.get("summary"),
            "entity_focus": event.get("entity_focus") or [],
            "anchors": sorted(_event_anchor_set(event)),
            "cluster_cohesion": event.get("cluster_cohesion") or {},
            "importance": {
                "score": importance_score,
                "reasons": importance_reasons,
                "breakdown": importance_breakdown,
            },
        },
    }


def _build_perspective_rows(
    event_id: str,
    articles: list[dict],
    framing_by_url: dict[str, dict],
    claims_by_source: dict[str, dict],
    reliability_by_source: dict[str, dict],
    registry_by_domain: dict[str, dict],
) -> list[dict]:
    rows = []
    for article in articles:
        url = (article.get("url") or "").strip()
        if not url:
            continue
        source_name = (article.get("source") or "").strip()
        source_domain = (article.get("source_domain") or "").strip()
        framing = framing_by_url.get(url) or {}
        reliability = reliability_by_source.get(source_name.lower()) or {}
        reg = registry_by_domain.get(source_domain) or {}
        claim = claims_by_source.get(source_name.lower()) or {}
        rows.append(
            {
                "perspective_id": _perspective_id(event_id, url),
                "event_id": event_id,
                "article_url": url,
                "source_name": source_name,
                "source_domain": source_domain or None,
                "source_reliability_score": reliability.get("empirical_score"),
                "source_trust_tier": reg.get("trust_tier"),
                "source_region": reg.get("region"),
                "dominant_frame": framing.get("dominant_frame"),
                "frame_counts": framing.get("frame_counts") or {},
                "matched_terms": framing.get("matched_terms") or [],
                "claim_text": claim.get("claim_text"),
                "claim_type": claim.get("claim_type"),
                "claim_resolution_status": claim.get("resolution_status"),
                "published_at": article.get("published_at"),
                "analyzed_at": time.time(),
                "payload": {},
            }
        )
    return rows


def rebuild_materialized_story_clusters(
    *,
    topics: list[str] | None = None,
    window_hours: int = 96,
    articles_limit: int = 120,
) -> dict:
    topic_list = list(topics or DEFAULT_TOPICS)
    window_hours = max(1, int(window_hours))
    total_rows = 0
    detail: list[dict] = []

    # load source metadata once for all topics
    reliability_by_source = load_latest_source_reliability()
    registry_entries = get_source_registry(active_only=False)
    registry_by_domain = {
        (e.get("source_domain") or "").lower(): e
        for e in registry_entries
        if e.get("source_domain")
    }

    for topic in topic_list:
        articles = get_recent_articles(
            topic=topic,
            limit=articles_limit,
            hours=window_hours,
            headline_corpus_only=True,
        )
        if not articles:
            replace_materialized_story_clusters(
                topic=topic, window_hours=window_hours, rows=[]
            )
            detail.append({"topic": topic, "clusters": 0})
            continue
        events = enrich_events(cluster_articles(articles, topic=topic))
        identity_candidates = list_canonical_identity_candidates(topic=topic, limit=600)
        legacy_rows = []
        canonical_rows = []
        all_perspective_rows = []
        identity_map_rows = []
        identity_event_rows = []
        canonical_observation_rows = []
        cluster_evidence_rows = []

        for event in events:
            cluster_key = event_cluster_key(event)
            linked = _link_structured_ids(event)
            urls = sorted(
                {
                    (a.get("url") or "").strip()
                    for a in event.get("articles", [])
                    if (a.get("url") or "").strip()
                }
            )

            # Resolve stable canonical event_id for this volatile observation key.
            mapped = get_event_id_for_observation_key(cluster_key)
            decision = None
            if mapped:
                event_id = mapped
            else:
                observation = {
                    "label": event.get("label") or "",
                    "article_urls": urls,
                    "linked_structured_event_ids": linked,
                    "entity_focus": event.get("entity_focus") or [],
                }
                event_id, decision = resolve_canonical_event_id(
                    observation_key=cluster_key,
                    observation=observation,
                    candidates=identity_candidates,
                )
                # Make newly created events eligible as candidates within the same run.
                identity_candidates.insert(
                    0,
                    {
                        "event_id": event_id,
                        "label": observation.get("label"),
                        "article_urls": urls,
                        "linked_structured_event_ids": linked,
                        "payload": {"entity_focus": observation.get("entity_focus") or []},
                    },
                )

            identity_map_rows.append(
                {
                    "observation_key": cluster_key,
                    "event_id": event_id,
                    "topic": topic,
                    "identity_confidence": (decision or {}).get("confidence"),
                    "identity_reasons": (decision or {}).get("reasons") or {},
                }
            )
            if decision is not None:
                identity_event_rows.append(
                    {
                        "observation_key": cluster_key,
                        "event_id": event_id,
                        "action": decision.get("action"),
                        "confidence": decision.get("confidence"),
                        "reasons": decision.get("reasons") or {},
                    }
                )

                for merge_candidate in decision.get("merge_candidates") or []:
                    candidate_event_id = str(
                        merge_candidate.get("event_id") or ""
                    ).strip()
                    if not candidate_event_id:
                        continue
                    identity_event_rows.append(
                        {
                            "observation_key": cluster_key,
                            "event_id": event_id,
                            "action": "merge_candidate",
                            "confidence": merge_candidate.get("score"),
                            "reasons": {
                                "selected_event_id": event_id,
                                "candidate_event_id": candidate_event_id,
                                "candidate_score": merge_candidate.get("score"),
                                "resolver_action": decision.get("action"),
                            },
                        }
                    )

                split_candidate = decision.get("split_candidate") or {}
                if isinstance(split_candidate, dict) and split_candidate:
                    identity_event_rows.append(
                        {
                            "observation_key": cluster_key,
                            "event_id": event_id,
                            "action": "split_candidate",
                            "confidence": (
                                split_candidate.get("score")
                                or decision.get("confidence")
                            ),
                            "reasons": {
                                **split_candidate,
                                "selected_event_id": event_id,
                                "resolver_action": decision.get("action"),
                            },
                        }
                    )

            latest_observation = get_latest_canonical_event_observation(event_id)
            structured_meta_by_id = get_structured_event_coordinates_by_ids(linked)
            importance_score, importance_reasons, importance_breakdown = (
                _build_importance_scoring_artifacts(
                    event,
                    linked_structured_ids=linked,
                    reliability_by_source=reliability_by_source,
                    registry_by_domain=registry_by_domain,
                    latest_observation=latest_observation,
                    structured_meta_by_id=structured_meta_by_id,
                )
            )

            canonical_observation_rows.append(
                {
                    "event_id": event_id,
                    "topic": topic,
                    "observation_key": cluster_key,
                    "article_count": len(urls),
                    "source_count": int(event.get("source_count") or 0),
                    "contradiction_count": len(event.get("contradictions") or []),
                    "tier_1_source_count": int(event.get("tier_1_source_count") or 0),
                    "importance_score": importance_score,
                    "payload": {
                        "cluster_cohesion": event.get("cluster_cohesion") or {},
                        "importance_breakdown": importance_breakdown,
                        "importance_reasons": importance_reasons,
                    },
                }
            )

            cluster_evidence_rows.extend(
                _build_cluster_assignment_evidence_rows(
                    event_id=event_id,
                    topic=topic,
                    observation_key=cluster_key,
                    event=event,
                )
            )

            # legacy table (unchanged)
            legacy_rows.append(
                {
                    "cluster_key": cluster_key,
                    "label": event.get("label") or "",
                    "summary": event.get("summary"),
                    "earliest_published_at": event.get("earliest_update"),
                    "latest_published_at": event.get("latest_update"),
                    "article_urls": urls,
                    "linked_structured_event_ids": linked,
                    "event_payload": event,
                }
            )

            # canonical event
            canonical_rows.append(
                _build_canonical_row(
                    event,
                    topic,
                    linked,
                    urls,
                    event_id,
                    importance_score=importance_score,
                    importance_reasons=importance_reasons,
                    importance_breakdown=importance_breakdown,
                )
            )

            # perspectives: load framing + claim resolution for this cluster
            framing_by_url = load_framing_signals_for_article_urls(urls)
            claim_records = load_claim_resolution_for_event_key(cluster_key)
            claims_by_source = {r["source_name"].lower(): r for r in claim_records}
            event_articles = event.get("articles") or []
            perspective_rows = _build_perspective_rows(
                event_id,
                event_articles,
                framing_by_url,
                claims_by_source,
                reliability_by_source,
                registry_by_domain,
            )
            all_perspective_rows.extend(perspective_rows)

        total_rows += replace_materialized_story_clusters(
            topic=topic, window_hours=window_hours, rows=legacy_rows
        )
        upsert_canonical_events(canonical_rows)
        upsert_canonical_event_observations(canonical_observation_rows)
        upsert_cluster_assignment_evidence(cluster_evidence_rows)
        upsert_event_identity_mappings(identity_map_rows)
        append_event_identity_events(identity_event_rows)
        upsert_event_perspectives(all_perspective_rows)

        detail.append({"topic": topic, "clusters": len(legacy_rows)})

    return {
        "topics": topic_list,
        "window_hours": window_hours,
        "rows_written": total_rows,
        "detail": detail,
    }


def build_causal_graph_for_topic(
    topic: str,
    window_hours: int = 96,
    articles_limit: int = 240,
    max_lag_days: int = 14,
    min_score: float = 0.35,
) -> dict:
    """Build a lightweight causal graph across event clusters for a topic.

    This function is intentionally non-destructive — it computes a suggested
    DAG of causal edges based on temporal ordering, shared entities, and
    lexical cues using the `CausalGraph` scaffold.
    """
    articles = get_recent_articles(
        topic=topic, limit=articles_limit, hours=window_hours, headline_corpus_only=True
    )
    if not articles:
        return {"topic": topic, "nodes": [], "edges": []}

    events = enrich_events(cluster_articles(articles, topic=topic))
    prepared = []
    for ev in events:
        cluster_key = event_cluster_key(ev)
        published_at = ev.get("earliest_update") or ev.get("latest_update")
        prepared.append(
            {
                "id": cluster_key,
                "title": ev.get("label") or "",
                "published_at": published_at,
                "summary": ev.get("summary") or "",
                "entities": [e for e in (ev.get("entity_focus") or [])],
                "country": None,
            }
        )

    graph = CausalGraph().build_from_events(
        prepared, max_lag_days=max_lag_days, min_score=min_score
    )

    return {
        "topic": topic,
        "nodes": [
            {
                "id": n.id,
                "title": n.title,
                "published_at": (
                    n.published_at.isoformat() if n.published_at else None
                ),
                "entities": n.entities,
            }
            for n in graph.nodes.values()
        ],
        "edges": graph.edges,
    }
