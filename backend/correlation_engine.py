"""Cross-domain correlation engine — detect signal convergence across data streams.

Identifies when multiple independent signal types converge on the same country
or geographic area, suggesting an escalating or significant situation.

Domains:
  - Military/Conflict: ACLED + GDELT structured events
  - Media: article volume spikes
  - Information fog: contradiction density
  - Entity activity: surging entities tied to a location
  - Narrative instability: framing volatility

When 3+ domains show elevated signals for the same country within the same
time window, a ConvergenceCard is generated with a composite score.
"""

import time
from collections import defaultdict
from datetime import datetime, timezone

from country_instability import (
    _normalize_country,
    _score_conflict,
    _score_media_attention,
    _score_contradictions,
    _score_event_severity,
    _score_entity_activity,
    _score_narrative_volatility,
)
from corpus import get_recent_structured_events
from geo_constants import COUNTRY_CENTROIDS

# ── Correlation thresholds ───────────────────────────────────────────────────

# Minimum component score to count as "active signal" in a domain
DOMAIN_ACTIVATION_THRESHOLD = 20.0

# Minimum number of active domains to generate a convergence card
MIN_CONVERGING_DOMAINS = 2

# Score weights for convergence card composite
CONVERGENCE_WEIGHTS = {
    "conflict": 0.30,
    "media_attention": 0.20,
    "contradiction": 0.15,
    "event_severity": 0.20,
    "entity_activity": 0.10,
    "narrative_volatility": 0.05,
}

# ── Cache ────────────────────────────────────────────────────────────────────

_CORRELATION_CACHE: tuple[float, dict] | None = None
_CORRELATION_CACHE_TTL = 900  # 15 minutes


# ── Convergence card builder ─────────────────────────────────────────────────


def _classify_convergence(active_domains: list[str]) -> str:
    """Classify the type of convergence based on which domains are active."""
    has_conflict = "conflict" in active_domains or "event_severity" in active_domains
    has_media = "media_attention" in active_domains
    has_info_fog = "contradiction" in active_domains
    has_entity = "entity_activity" in active_domains
    has_narrative = "narrative_volatility" in active_domains

    if has_conflict and has_media and has_info_fog:
        return "crisis_escalation"
    if has_conflict and has_entity:
        return "military_escalation"
    if has_media and has_info_fog and has_narrative:
        return "information_crisis"
    if has_conflict and has_media:
        return "conflict_spotlight"
    if has_media and has_entity:
        return "emerging_situation"
    if has_info_fog and has_narrative:
        return "narrative_instability"
    return "multi_signal"


def _trend_label(score: float, prev_scores: dict[str, float], country: str) -> str:
    prev = prev_scores.get(country)
    if prev is None:
        return "new"
    delta = score - prev
    if delta > 8:
        return "escalating"
    if delta < -8:
        return "de-escalating"
    return "stable"


_CONVERGENCE_DESCRIPTIONS = {
    "crisis_escalation": "Conflict events, media surge, and contradictory reporting converge — possible developing crisis.",
    "military_escalation": "Armed conflict events with elevated actor activity — escalation risk.",
    "information_crisis": "Media attention spike with high contradiction density and narrative instability — fog of war pattern.",
    "conflict_spotlight": "Active conflict with significant media coverage — situation under global scrutiny.",
    "emerging_situation": "Media attention rising alongside entity activity spikes — developing story.",
    "narrative_instability": "Contradictory reporting and volatile framing — unclear ground truth.",
    "multi_signal": "Multiple signal domains active — elevated monitoring warranted.",
}


# ── Previous scores for trend detection ──────────────────────────────────────

_prev_convergence_scores: dict[str, float] = {}


# ── Main correlation function ────────────────────────────────────────────────


def compute_correlations(days: int = 3) -> dict:
    """Detect cross-domain signal convergence per country.

    Returns convergence cards for countries where multiple domains
    show elevated signals simultaneously.
    """
    global _CORRELATION_CACHE, _prev_convergence_scores

    now_ts = time.time()
    if _CORRELATION_CACHE and (now_ts - _CORRELATION_CACHE[0]) < _CORRELATION_CACHE_TTL:
        return _CORRELATION_CACHE[1]

    hours = days * 24

    # Gather structured events
    events = get_recent_structured_events(days=days, limit=5000)
    events_by_country: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        country = _normalize_country(ev.get("country"))
        if country:
            events_by_country[country].append(ev)

    # Compute all component scores (reuse CII scoring functions)
    conflict = _score_conflict(events_by_country)
    media = _score_media_attention(hours=hours)
    contradictions = _score_contradictions(hours=hours)
    severity = _score_event_severity(events_by_country)
    entity = _score_entity_activity()
    narrative = _score_narrative_volatility()

    all_components = {
        "conflict": conflict,
        "media_attention": media,
        "contradiction": contradictions,
        "event_severity": severity,
        "entity_activity": entity,
        "narrative_volatility": narrative,
    }

    # Collect all countries
    all_countries: set[str] = set()
    for component in all_components.values():
        all_countries.update(component.keys())

    # Build convergence cards
    cards = []
    new_scores: dict[str, float] = {}

    for country in all_countries:
        domain_scores = {}
        active_domains = []

        for domain, scores in all_components.items():
            score = scores.get(country, {}).get("score", 0.0)
            domain_scores[domain] = score
            if score >= DOMAIN_ACTIVATION_THRESHOLD:
                active_domains.append(domain)

        if len(active_domains) < MIN_CONVERGING_DOMAINS:
            continue

        # Composite convergence score
        composite = sum(
            domain_scores.get(k, 0.0) * CONVERGENCE_WEIGHTS[k]
            for k in CONVERGENCE_WEIGHTS
        )
        # Boost for more converging domains
        domain_boost = 1.0 + (len(active_domains) - MIN_CONVERGING_DOMAINS) * 0.15
        composite = round(min(100.0, composite * domain_boost), 1)

        new_scores[country] = composite

        convergence_type = _classify_convergence(active_domains)
        trend = _trend_label(composite, _prev_convergence_scores, country)

        centroid = COUNTRY_CENTROIDS.get(country)
        lat = centroid["latitude"] if centroid else None
        lon = centroid["longitude"] if centroid else None
        label = centroid["label"] if centroid else country.title()

        # Top signals — the specific evidence driving each active domain
        signals = []
        if "conflict" in active_domains:
            country_events = events_by_country.get(country, [])
            top_events = sorted(
                country_events, key=lambda e: e.get("fatalities") or 0, reverse=True
            )[:3]
            for ev in top_events:
                signals.append(
                    {
                        "domain": "conflict",
                        "type": ev.get("event_type", "Unknown"),
                        "summary": (ev.get("summary") or "")[:200],
                        "date": ev.get("event_date"),
                        "severity": (
                            "high" if (ev.get("fatalities") or 0) > 0 else "medium"
                        ),
                    }
                )

        if "media_attention" in active_domains:
            media_info = media.get(country, {})
            signals.append(
                {
                    "domain": "media_attention",
                    "type": "Article surge",
                    "summary": f"{media_info.get('weighted_articles', 0):.0f} weighted articles from {media_info.get('source_count', 0)} sources",
                    "severity": "high" if media_info.get("score", 0) > 60 else "medium",
                }
            )

        if "contradiction" in active_domains:
            contra_info = contradictions.get(country, {})
            signals.append(
                {
                    "domain": "contradiction",
                    "type": "Information fog",
                    "summary": f"{contra_info.get('contradiction_count', 0)} contradictions across {contra_info.get('event_count', 0)} events",
                    "severity": (
                        "high" if contra_info.get("score", 0) > 50 else "medium"
                    ),
                }
            )

        cards.append(
            {
                "country": country,
                "label": label,
                "score": composite,
                "convergence_type": convergence_type,
                "convergence_description": _CONVERGENCE_DESCRIPTIONS.get(
                    convergence_type, ""
                ),
                "trend": trend,
                "active_domains": active_domains,
                "domain_count": len(active_domains),
                "domain_scores": {k: round(v, 1) for k, v in domain_scores.items()},
                "signals": signals,
                "latitude": lat,
                "longitude": lon,
            }
        )

    # Sort by composite score
    cards.sort(key=lambda c: -c["score"])

    _prev_convergence_scores = new_scores

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "window_days": days,
        "card_count": len(cards),
        "cards": cards,
        "domain_activation_threshold": DOMAIN_ACTIVATION_THRESHOLD,
    }
    _CORRELATION_CACHE = (now_ts, result)
    return result
