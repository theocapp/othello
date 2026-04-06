import math
import re
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlparse


def _clean_text(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _safe_int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _source_domain(item: dict) -> str:
    payload = item.get("payload") or {}
    source = _clean_text(payload.get("source"))
    if source and "." in source and " " not in source:
        return source.lower()
    source_urls = item.get("source_urls") or payload.get("source_urls") or []
    for url in source_urls:
        host = urlparse(str(url)).netloc.lower()
        if host:
            return host
    return source.lower()


def _source_role(item: dict) -> str:
    dataset = _clean_text(item.get("dataset")).lower()
    payload = item.get("payload") or {}
    source_type = _clean_text(payload.get("source_type")).lower()
    trust_tier = _clean_text(payload.get("trust_tier")).lower()
    if dataset == "acled":
        return "structured"
    if trust_tier in {"official", "gov", "government"}:
        return "official"
    if source_type in {"wire", "official", "structured"}:
        return "verification"
    if source_type in {"state_media", "advocacy", "partisan"}:
        return "narrative"
    return "detector"


def _event_sources(items: list[dict]) -> list[dict]:
    sources = []
    for item in items:
        domain = _source_domain(item)
        if not domain:
            continue
        sources.append(
            {
                "domain": domain,
                "role": _source_role(item),
                "label": domain.replace("www.", ""),
            }
        )
    return sources


def _independent_source_families(items: list[dict]) -> list[dict]:
    families = {}
    for source in _event_sources(items):
        key = source["domain"]
        entry = families.setdefault(
            key,
            {
                "domain": key,
                "role": source["role"],
                "labels": set(),
                "count": 0,
            },
        )
        entry["labels"].add(source["label"])
        entry["count"] += 1
        priority = {
            "official": 5,
            "structured": 4,
            "verification": 3,
            "detector": 2,
            "narrative": 1,
        }
        if priority.get(source["role"], 0) > priority.get(entry["role"], 0):
            entry["role"] = source["role"]
    normalized = []
    for value in families.values():
        normalized.append(
            {
                "domain": value["domain"],
                "role": value["role"],
                "labels": sorted(value["labels"]),
                "count": value["count"],
            }
        )
    normalized.sort(key=lambda item: (item["role"], item["count"], item["domain"]), reverse=True)
    return normalized


def _range_label(values: list[int]) -> str | None:
    if not values:
        return None
    if len(set(values)) <= 1:
        return None
    low = min(values)
    high = max(values)
    if low == high:
        return None
    return f"{low}–{high}"


def build_disputed_points(event: dict, items: list[dict]) -> list[str]:
    disputed = []
    fatalities = [_safe_int(item.get("fatalities")) for item in items if _safe_int(item.get("fatalities")) > 0]
    fatality_range = _range_label(fatalities)
    if fatality_range:
        disputed.append(f"Reported fatalities vary across sources ({fatality_range}).")

    actor_pairs = {
        " / ".join(
            [
                value for value in [
                    _clean_text(item.get("actor_primary")),
                    _clean_text(item.get("actor_secondary")),
                ] if value
            ]
        )
        for item in items
    }
    actor_pairs.discard("")
    if len(actor_pairs) >= 3:
        disputed.append("Different reports emphasize different actors or pairings around the same incident.")

    locations = {
        _clean_text(item.get("location"))
        for item in items
        if _clean_text(item.get("location"))
    }
    if len(locations) >= 3:
        disputed.append("Location references vary between reports, suggesting partial or evolving situational detail.")

    source_roles = Counter(source["role"] for source in _independent_source_families(items))
    if source_roles.get("narrative", 0) >= 2 and source_roles.get("verification", 0) == 0:
        disputed.append("Coverage is being driven more by narrative-style sources than by strong independent verification.")

    return disputed[:4]


def _severity_score(event: dict, items: list[dict]) -> float:
    fatalities = _safe_int(event.get("fatality_total"))
    event_type = _clean_text(event.get("primary_event_type"))
    structured_count = _safe_int(event.get("structured_event_count"))
    base = 0.0
    if event_type == "Battles":
        base += 20.0
    elif event_type == "Violence against civilians":
        base += 22.0
    elif event_type == "Explosions/Remote violence":
        base += 18.0
    elif event_type == "Riots":
        base += 10.0
    elif event_type == "Strategic developments":
        base += 9.0
    elif event_type == "Protests":
        base += 7.0
    base += min(fatalities, 25) * 1.8
    base += min(structured_count, 10) * 1.2
    if len({_clean_text(item.get("country")) for item in items if _clean_text(item.get("country"))}) >= 2:
        base += 6.0
    return base


def calculate_importance(event: dict, items: list[dict]) -> float:
    families = _independent_source_families(items)
    family_count = len(families)
    source_role_weights = {
        "official": 1.35,
        "structured": 1.25,
        "verification": 1.1,
        "detector": 0.75,
        "narrative": 0.55,
    }
    weighted_families = sum(source_role_weights.get(item["role"], 0.5) for item in families)
    severity = _severity_score(event, items)
    locations = len({_clean_text(item.get("location")) for item in items if _clean_text(item.get("location"))})
    actors = len(
        {
            _clean_text(item.get("actor_primary"))
            for item in items
            if _clean_text(item.get("actor_primary"))
        }
    )
    recency_bonus = 0.0
    latest = _parse_dt(event.get("latest_update"))
    if latest:
        age_hours = max(0.0, (datetime.now(timezone.utc) - latest).total_seconds() / 3600.0)
        if age_hours <= 12:
            recency_bonus = 16.0
        elif age_hours <= 24:
            recency_bonus = 11.0
        elif age_hours <= 48:
            recency_bonus = 6.0
        elif age_hours <= 72:
            recency_bonus = 2.0
    ambiguity_penalty = 4.0 * max(0, len(build_disputed_points(event, items)) - 1)
    score = (
        severity
        + (weighted_families * 8.0)
        + (family_count * 3.0)
        + min(locations, 4) * 2.0
        + min(actors, 4) * 1.5
        + recency_bonus
        - ambiguity_penalty
    )
    return round(max(0.0, min(score, 100.0)), 2)


def calculate_confidence(event: dict, items: list[dict]) -> float:
    families = _independent_source_families(items)
    family_count = len(families)
    roles = Counter(item["role"] for item in families)
    structured_bonus = 16.0 if roles.get("structured", 0) else 0.0
    official_bonus = 12.0 if roles.get("official", 0) else 0.0
    verification_bonus = min(roles.get("verification", 0) * 8.0, 20.0)
    independence_bonus = min(family_count * 7.0, 28.0)
    disputed_penalty = len(build_disputed_points(event, items)) * 7.0
    score = 22.0 + structured_bonus + official_bonus + verification_bonus + independence_bonus - disputed_penalty
    return round(max(0.0, min(score, 100.0)), 2)


def calculate_narrative_divergence(event: dict, items: list[dict]) -> float:
    fatalities = [_safe_int(item.get("fatalities")) for item in items if _safe_int(item.get("fatalities")) > 0]
    actors = {
        " / ".join(
            [
                value for value in [
                    _clean_text(item.get("actor_primary")),
                    _clean_text(item.get("actor_secondary")),
                ] if value
            ]
        )
        for item in items
    }
    actors.discard("")
    roles = Counter(item["role"] for item in _independent_source_families(items))
    fatality_spread = (max(fatalities) - min(fatalities)) if len(fatalities) >= 2 else 0
    score = 8.0
    score += min(len(actors), 5) * 6.0
    score += min(fatality_spread, 20) * 1.2
    score += roles.get("narrative", 0) * 9.0
    if roles.get("verification", 0) == 0 and roles.get("official", 0) == 0 and len(items) >= 3:
        score += 12.0
    return round(max(0.0, min(score, 100.0)), 2)


def classify_status(importance_score: float, confidence_score: float, divergence_score: float) -> str:
    if confidence_score >= 72 and divergence_score <= 22:
        return "confirmed"
    if confidence_score >= 58 and divergence_score <= 38:
        return "likely"
    if importance_score >= 55 and divergence_score >= 30:
        return "contested"
    return "developing"


def build_what_probably_happened(event: dict, items: list[dict]) -> list[str]:
    event_type = _clean_text(event.get("primary_event_type")) or "incident"
    sub_type = _clean_text(event.get("primary_sub_event_type"))
    countries = [_clean_text(value) for value in (event.get("country_focus") or []) if _clean_text(value)]
    locations = [_clean_text(value) for value in (event.get("location_focus") or []) if _clean_text(value)]
    actors = [_clean_text(value) for value in (event.get("entity_focus") or []) if _clean_text(value)]
    earliest = _clean_text(event.get("earliest_update"))
    latest = _clean_text(event.get("latest_update"))
    fatalities = _safe_int(event.get("fatality_total"))

    points = []
    if sub_type and sub_type.lower() != event_type.lower():
        points.append(f"Available reporting points to {sub_type.lower()} within the broader category of {event_type.lower()}.")
    else:
        points.append(f"Available reporting converges on a {event_type.lower()} event.")

    if locations:
        loc_text = locations[0]
        if countries and countries[0].lower() not in loc_text.lower():
            loc_text = f"{loc_text}, {countries[0]}"
        points.append(f"The incident is centered on {loc_text}.")
    elif countries:
        points.append(f"The incident is centered in {countries[0]}.")

    if actors:
        actor_text = actors[0] if len(actors) == 1 else f"{actors[0]} and {actors[1]}"
        points.append(f"The main actors referenced most often are {actor_text}.")

    if fatalities > 0:
        points.append(f"At least {fatalities} fatalities are reflected in the currently merged incident data.")

    if earliest and latest and earliest != latest:
        points.append(f"The reporting window for this cluster runs from {earliest} to {latest}.")
    elif latest:
        points.append(f"The latest update attached to this cluster is {latest}.")

    return points[:4]


def build_event_intelligence(event: dict, items: list[dict]) -> dict:
    families = _independent_source_families(items)
    importance = calculate_importance(event, items)
    confidence = calculate_confidence(event, items)
    divergence = calculate_narrative_divergence(event, items)
    disputed = build_disputed_points(event, items)
    return {
        "importance_score": importance,
        "confidence_score": confidence,
        "narrative_divergence_score": divergence,
        "status": classify_status(importance, confidence, divergence),
        "independent_source_count": len(families),
        "evidence_families": families[:8],
        "disputed_points": disputed,
        "what_probably_happened": build_what_probably_happened(event, items),
    }
