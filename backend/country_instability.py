"""Country Instability Index (CII) — composite risk scoring per country.

Aggregates signals from multiple data streams into a 0-100 score per country:
  1. Conflict intensity    — ACLED/GDELT structured events, fatalities, event density
  2. Media attention       — article volume + recency weighting
  3. Contradiction density — conflicting claims across sources (information fog)
  4. Entity activity       — spike detection in entity mentions (actors moving)
  5. Event severity        — fatality-weighted conflict scoring
  6. Narrative volatility  — framing instability from drift analysis

Each component is scored 0-100, then combined via weighted average.
A 24h trend (rising / stable / falling) is computed from cached snapshots.
"""

import math
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from corpus import (
    get_recent_structured_events,
    get_recent_contradiction_records,
    get_articles_with_regions,
    using_postgres,
    _connect,
)
from geo_constants import COUNTRY_CENTROIDS

# ── Component weights (must sum to 1.0) ─────────────────────────────────────

COMPONENT_WEIGHTS = {
    "conflict": 0.30,
    "media_attention": 0.20,
    "contradiction": 0.15,
    "event_severity": 0.20,
    "entity_activity": 0.10,
    "narrative_volatility": 0.05,
}

# ── Normalization parameters ─────────────────────────────────────────────────

# Conflict: event counts that map to score 50 / 100
CONFLICT_EVENTS_MID = 15
CONFLICT_EVENTS_HIGH = 60

# Media attention: article counts that map to score 50 / 100
MEDIA_ARTICLES_MID = 30
MEDIA_ARTICLES_HIGH = 120

# Contradictions: counts that map to score 50 / 100
CONTRADICTION_MID = 3
CONTRADICTION_HIGH = 12

# Entity activity: spike ratio thresholds
ENTITY_SPIKE_MID = 2.0
ENTITY_SPIKE_HIGH = 5.0

# ── Snapshot cache for trend detection ───────────────────────────────────────

_score_snapshots: list[tuple[float, dict[str, float]]] = []
_SNAPSHOT_MAX_AGE = 48 * 3600  # keep 48h of snapshots
_SNAPSHOT_TREND_WINDOW = 24 * 3600  # compare against 24h ago

# ── Country name normalization ───────────────────────────────────────────────

_COUNTRY_ALIASES = {
    "united states of america": "united states",
    "usa": "united states",
    "us": "united states",
    "uk": "united kingdom",
    "great britain": "united kingdom",
    "republic of korea": "south korea",
    "dprk": "north korea",
    "democratic republic of the congo": "dr congo",
    "democratic republic of congo": "dr congo",
    "drc": "dr congo",
    "turkiye": "turkey",
    "türkiye": "turkey",
    "ivory coast": "cote d'ivoire",
    "myanmar (burma)": "myanmar",
    "occupied palestinian territory": "palestine",
    "state of palestine": "palestine",
}


def _normalize_country(name: str | None) -> str | None:
    if not name:
        return None
    key = name.strip().lower()
    return _COUNTRY_ALIASES.get(key, key)


# ── Sigmoid-style normalization ──────────────────────────────────────────────

def _sigmoid_score(value: float, midpoint: float, steepness: float = 4.0) -> float:
    """Map a raw value to 0-100 using a sigmoid centered at midpoint."""
    if value <= 0:
        return 0.0
    x = (value - midpoint) / max(midpoint, 1)
    return min(100.0, max(0.0, 100.0 / (1.0 + math.exp(-steepness * x))))


# ── Component scorers ────────────────────────────────────────────────────────

def _score_conflict(events_by_country: dict[str, list[dict]]) -> dict[str, dict]:
    """Score conflict intensity from structured events."""
    scores = {}
    for country, events in events_by_country.items():
        event_count = len(events)
        total_fatalities = sum(e.get("fatalities") or 0 for e in events)

        # Weight by event type severity
        severity_weights = {
            "Battles": 3.0,
            "Violence against civilians": 4.0,
            "Explosions/Remote violence": 3.5,
            "Protests": 1.0,
            "Riots": 1.5,
            "Strategic developments": 2.0,
        }
        weighted_count = sum(
            severity_weights.get(e.get("event_type", ""), 1.0) for e in events
        )

        raw = weighted_count + (total_fatalities * 0.5)
        score = _sigmoid_score(raw, CONFLICT_EVENTS_MID * 2)

        scores[country] = {
            "score": round(score, 1),
            "event_count": event_count,
            "fatalities": total_fatalities,
            "weighted_count": round(weighted_count, 1),
        }
    return scores


def _score_media_attention(hours: int = 72) -> dict[str, dict]:
    """Score media attention from article volume per country/region."""
    rows = get_articles_with_regions(hours=hours)
    now = datetime.now(timezone.utc)

    country_counts: dict[str, float] = defaultdict(float)
    country_sources: dict[str, set] = defaultdict(set)

    for row in rows:
        region = (row.get("region") or "global").strip().lower()
        if region == "global":
            continue

        # Map regions to approximate countries where possible
        published_at = row.get("published_at")
        source = (row.get("source_domain") or row.get("source") or "").strip().lower()

        # Recency weighting
        age_hours = hours
        if published_at:
            try:
                dt = datetime.fromisoformat(str(published_at).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                age_hours = max(0.0, (now - dt).total_seconds() / 3600)
            except (ValueError, TypeError):
                pass
        recency = max(0.0, 1.0 - min(age_hours / max(hours, 1), 1.0))
        weight = 1.0 + (recency * 1.5)

        country_counts[region] += weight
        if source:
            country_sources[region].add(source)

    scores = {}
    for country, weighted_count in country_counts.items():
        score = _sigmoid_score(weighted_count, MEDIA_ARTICLES_MID)
        scores[country] = {
            "score": round(score, 1),
            "weighted_articles": round(weighted_count, 1),
            "source_count": len(country_sources.get(country, set())),
        }
    return scores


def _score_contradictions(hours: int = 72) -> dict[str, dict]:
    """Score information fog from contradiction density per country."""
    records = get_recent_contradiction_records(hours=hours)

    country_contradictions: dict[str, int] = defaultdict(int)
    country_events: dict[str, int] = defaultdict(int)

    for rec in records:
        label = (rec.get("event_label") or "").lower()
        count = rec.get("contradiction_count") or 0

        # Try to extract country from event label
        matched_country = None
        for country_key in COUNTRY_CENTROIDS:
            if country_key in label:
                matched_country = _normalize_country(country_key)
                break

        if matched_country:
            country_contradictions[matched_country] += count
            country_events[matched_country] += 1

    scores = {}
    for country, total in country_contradictions.items():
        score = _sigmoid_score(total, CONTRADICTION_MID)
        scores[country] = {
            "score": round(score, 1),
            "contradiction_count": total,
            "event_count": country_events.get(country, 0),
        }
    return scores


def _score_event_severity(events_by_country: dict[str, list[dict]]) -> dict[str, dict]:
    """Score based on fatality-weighted event severity."""
    scores = {}
    for country, events in events_by_country.items():
        total_fatalities = sum(e.get("fatalities") or 0 for e in events)
        high_fatality_events = sum(
            1 for e in events if (e.get("fatalities") or 0) >= 10
        )

        raw = total_fatalities + (high_fatality_events * 10)
        score = _sigmoid_score(raw, 20)

        scores[country] = {
            "score": round(score, 1),
            "total_fatalities": total_fatalities,
            "high_fatality_events": high_fatality_events,
        }
    return scores


def _score_entity_activity() -> dict[str, dict]:
    """Score entity mention spikes per country using the entities DB."""
    try:
        from entities import get_entity_frequencies
        spikes = get_entity_frequencies(days_recent=2, days_baseline=7)
    except Exception:
        return {}

    country_spike_scores: dict[str, float] = defaultdict(float)
    country_spike_counts: dict[str, int] = defaultdict(int)

    for spike in spikes:
        entity = (spike.get("entity") or "").lower()
        entity_type = (spike.get("type") or spike.get("entity_type") or "").upper()
        ratio = spike.get("spike_ratio") or spike.get("ratio") or 1.0

        if entity_type != "GPE" or ratio < 1.5:
            continue

        normalized = _normalize_country(entity)
        if normalized and normalized in COUNTRY_CENTROIDS:
            country_spike_scores[normalized] += ratio
            country_spike_counts[normalized] += 1

    scores = {}
    for country, total_ratio in country_spike_scores.items():
        avg_ratio = total_ratio / max(country_spike_counts[country], 1)
        score = _sigmoid_score(avg_ratio, ENTITY_SPIKE_MID)
        scores[country] = {
            "score": round(score, 1),
            "avg_spike_ratio": round(avg_ratio, 2),
            "spike_count": country_spike_counts[country],
        }
    return scores


def _score_narrative_volatility() -> dict[str, dict]:
    """Score narrative instability from drift analysis (if available)."""
    try:
        placeholder = "%s" if using_postgres() else "?"
        cutoff = time.time() - (72 * 3600)
        with _connect() as conn:
            rows = conn.execute(
                f"""
                SELECT subject_key, framing_label, COUNT(*) as cnt
                FROM article_framing_signals
                WHERE published_at >= {placeholder}
                GROUP BY subject_key, framing_label
                ORDER BY subject_key, cnt DESC
                """,
                (datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat(),),
            ).fetchall()
    except Exception:
        return {}

    # Count distinct framings per subject — more framings = more volatility
    subject_framings: dict[str, set] = defaultdict(set)
    for row in rows:
        row_dict = dict(row)
        subject = (row_dict.get("subject_key") or "").lower()
        framing = row_dict.get("framing_label") or ""
        if subject and framing:
            subject_framings[subject].add(framing)

    country_volatility: dict[str, int] = defaultdict(int)
    for subject, framings in subject_framings.items():
        for country_key in COUNTRY_CENTROIDS:
            if country_key in subject:
                normalized = _normalize_country(country_key)
                if normalized:
                    country_volatility[normalized] = max(
                        country_volatility[normalized], len(framings)
                    )
                break

    scores = {}
    for country, framing_count in country_volatility.items():
        score = _sigmoid_score(framing_count, 3)
        scores[country] = {
            "score": round(score, 1),
            "distinct_framings": framing_count,
        }
    return scores


# ── Trend detection ──────────────────────────────────────────────────────────

def _compute_trend(country: str, current_score: float) -> str:
    """Compare current score against the closest snapshot from ~24h ago."""
    now = time.time()
    target_ts = now - _SNAPSHOT_TREND_WINDOW
    best_snapshot = None
    best_dist = float("inf")

    for ts, scores in _score_snapshots:
        dist = abs(ts - target_ts)
        if dist < best_dist:
            best_dist = dist
            best_snapshot = scores

    if best_snapshot is None or best_dist > _SNAPSHOT_TREND_WINDOW * 0.5:
        return "new"

    prev_score = best_snapshot.get(country, 0.0)
    delta = current_score - prev_score

    if delta > 5:
        return "rising"
    elif delta < -5:
        return "falling"
    return "stable"


def _prune_snapshots():
    cutoff = time.time() - _SNAPSHOT_MAX_AGE
    while _score_snapshots and _score_snapshots[0][0] < cutoff:
        _score_snapshots.pop(0)


# ── Main scoring function ────────────────────────────────────────────────────

def compute_country_instability(days: int = 3) -> dict:
    """Compute CII scores for all countries with available data.

    Returns dict with:
      - countries: list of scored country dicts
      - components: raw component scores per country
      - metadata: computation stats
    """
    hours = days * 24

    # Gather structured events grouped by country
    events = get_recent_structured_events(days=days, limit=5000)
    events_by_country: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        country = _normalize_country(ev.get("country"))
        if country:
            events_by_country[country].append(ev)

    # Compute all components
    conflict_scores = _score_conflict(events_by_country)
    media_scores = _score_media_attention(hours=hours)
    contradiction_scores = _score_contradictions(hours=hours)
    severity_scores = _score_event_severity(events_by_country)
    entity_scores = _score_entity_activity()
    narrative_scores = _score_narrative_volatility()

    # Collect all countries mentioned in any component
    all_countries: set[str] = set()
    for component in [
        conflict_scores, media_scores, contradiction_scores,
        severity_scores, entity_scores, narrative_scores,
    ]:
        all_countries.update(component.keys())

    # Compute composite scores
    country_results = []
    score_map: dict[str, float] = {}

    for country in sorted(all_countries):
        components = {
            "conflict": conflict_scores.get(country, {}).get("score", 0.0),
            "media_attention": media_scores.get(country, {}).get("score", 0.0),
            "contradiction": contradiction_scores.get(country, {}).get("score", 0.0),
            "event_severity": severity_scores.get(country, {}).get("score", 0.0),
            "entity_activity": entity_scores.get(country, {}).get("score", 0.0),
            "narrative_volatility": narrative_scores.get(country, {}).get("score", 0.0),
        }

        composite = sum(
            components[k] * COMPONENT_WEIGHTS[k] for k in COMPONENT_WEIGHTS
        )
        composite = round(min(100.0, composite), 1)

        # Determine risk level
        if composite >= 75:
            level = "critical"
        elif composite >= 50:
            level = "high"
        elif composite >= 25:
            level = "elevated"
        else:
            level = "low"

        trend = _compute_trend(country, composite)
        score_map[country] = composite

        # Look up coordinates
        centroid = COUNTRY_CENTROIDS.get(country)
        lat = centroid["latitude"] if centroid else None
        lon = centroid["longitude"] if centroid else None
        label = centroid["label"] if centroid else country.title()

        component_details = {
            "conflict": conflict_scores.get(country, {}),
            "media_attention": media_scores.get(country, {}),
            "contradiction": contradiction_scores.get(country, {}),
            "event_severity": severity_scores.get(country, {}),
            "entity_activity": entity_scores.get(country, {}),
            "narrative_volatility": narrative_scores.get(country, {}),
        }

        country_results.append({
            "country": country,
            "label": label,
            "score": composite,
            "level": level,
            "trend": trend,
            "components": components,
            "component_details": component_details,
            "latitude": lat,
            "longitude": lon,
        })

    # Sort by score descending
    country_results.sort(key=lambda c: -c["score"])

    # Save snapshot for trend detection
    _prune_snapshots()
    _score_snapshots.append((time.time(), score_map))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "window_days": days,
        "country_count": len(country_results),
        "countries": country_results,
        "weights": COMPONENT_WEIGHTS,
    }
