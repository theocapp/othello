import hashlib
import re
from collections import Counter
from datetime import datetime, timezone

from corpus import get_recent_structured_events
from event_ranker import build_event_intelligence


EVENT_TYPE_WEIGHTS = {
    "Battles": 8.0,
    "Violence against civilians": 7.5,
    "Explosions/Remote violence": 7.0,
    "Riots": 5.0,
    "Strategic developments": 4.0,
    "Protests": 3.0,
}


def _clean_text(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _slug(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "-", _clean_text(value).lower()).strip("-")


def _parse_event_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _actor_tokens(event: dict) -> set[str]:
    raw = " ".join(
        [
            _clean_text(event.get("actor_primary")),
            _clean_text(event.get("actor_secondary")),
        ]
    ).lower()
    tokens = re.findall(r"[a-z][a-z-]{2,}", raw)
    stop = {
        "the", "and", "for", "with", "from", "against", "group", "groups", "forces", "force",
        "military", "armed", "civilian", "civilians", "government", "state", "security",
        "police", "protesters", "protester", "people", "residents", "supporters",
        "fighters", "party", "parties", "members", "students", "union",
    }
    return {token for token in tokens if token not in stop}


def _event_signature(event: dict) -> dict:
    actors = {
        _clean_text(event.get("actor_primary")).lower(),
        _clean_text(event.get("actor_secondary")).lower(),
    }
    actors.discard("")
    return {
        "country": _clean_text(event.get("country")),
        "event_type": _clean_text(event.get("event_type")),
        "sub_event_type": _clean_text(event.get("sub_event_type")),
        "admin1": _clean_text(event.get("admin1")),
        "admin2": _clean_text(event.get("admin2")),
        "location": _clean_text(event.get("location")),
        "actors": actors,
        "actor_tokens": _actor_tokens(event),
        "event_dt": _parse_event_date(event.get("event_date")),
        "fatalities": int(event.get("fatalities") or 0),
    }


def _days_apart(left: dict, right: dict) -> float | None:
    if left["event_dt"] is None or right["event_dt"] is None:
        return None
    return abs((left["event_dt"] - right["event_dt"]).total_seconds()) / 86400.0


def _same_non_empty(left: str, right: str) -> bool:
    return bool(left and right and left == right)


def _hard_no_merge(left: dict, right: dict) -> bool:
    if left["country"] != right["country"]:
        return True
    if left["event_type"] != right["event_type"]:
        return True

    gap_days = _days_apart(left, right)
    if gap_days is not None and gap_days > 3.0:
        return True

    same_location = _same_non_empty(left["location"], right["location"])
    same_admin1 = _same_non_empty(left["admin1"], right["admin1"])
    actor_overlap = len(left["actors"] & right["actors"])
    token_overlap = len(left["actor_tokens"] & right["actor_tokens"])

    if gap_days is not None and gap_days > 1.0 and not same_location and not same_admin1 and actor_overlap == 0 and token_overlap == 0:
        return True

    if left["event_type"] in {"Protests", "Riots"} and gap_days is not None and gap_days > 1.0 and not same_location:
        return True

    if same_admin1 and not same_location and actor_overlap == 0 and token_overlap == 0 and gap_days is not None and gap_days > 0.0:
        return True

    fatality_gap = abs(left["fatalities"] - right["fatalities"])
    if fatality_gap >= 20 and not same_location and actor_overlap == 0:
        return True

    return False


def _related_score(left: dict, right: dict) -> float:
    if _hard_no_merge(left, right):
        return -100.0

    gap_days = _days_apart(left, right)
    score = 0.0

    if _same_non_empty(left["sub_event_type"], right["sub_event_type"]):
        score += 1.6
    if _same_non_empty(left["admin1"], right["admin1"]):
        score += 1.4
    if _same_non_empty(left["admin2"], right["admin2"]):
        score += 1.2
    if _same_non_empty(left["location"], right["location"]):
        score += 2.4

    score += len(left["actors"] & right["actors"]) * 2.0
    score += min(len(left["actor_tokens"] & right["actor_tokens"]), 3) * 0.8

    if gap_days is not None:
        if gap_days <= 1.0:
            score += 1.0
        elif gap_days <= 2.0:
            score += 0.5

    return round(score, 3)


def _is_related(left: dict, right: dict) -> bool:
    if _hard_no_merge(left, right):
        return False
    if _same_non_empty(left["location"], right["location"]) and _same_non_empty(left["sub_event_type"], right["sub_event_type"]):
        return True
    if _related_score(left, right) >= 3.8:
        return True
    gap_days = _days_apart(left, right)
    return len(left["actors"] & right["actors"]) >= 1 and _same_non_empty(left["admin1"], right["admin1"]) and (gap_days is None or gap_days <= 2.0)


def _top_values(counter: Counter, limit: int = 6) -> list[str]:
    return [value for value, _ in counter.most_common(limit) if value]


def _cluster_label(country: str, event_type: str, sub_event_type: str, actors: list[str], locations: list[str]) -> str:
    base = sub_event_type if sub_event_type and not sub_event_type.isdigit() else event_type or "Incident"
    if actors:
        actor_text = actors[0] if len(actors) == 1 else f"{actors[0]} and {actors[1]}"
        if locations:
            return f"{base} involving {actor_text} in {locations[0]}"
        return f"{base} involving {actor_text} in {country}"
    if locations:
        return f"{base} reported in {locations[0]}"
    return f"{base} reported in {country}"


def _cluster_summary(cluster: list[dict], country: str, event_type: str, locations: list[str], fatalities: int, dataset_label: str) -> str:
    incident_count = len(cluster)
    sample = max(
        cluster,
        key=lambda item: (
            int(item.get("fatalities") or 0),
            len(_clean_text(item.get("summary"))),
            item.get("event_date") or "",
        ),
    )
    summary = _clean_text(sample.get("summary"))
    if summary:
        summary = re.sub(r"\s*\(CAMEO[^)]*\)", "", summary).strip()
    location_note = f"in {locations[0]}" if locations else f"in {country}"
    fatality_note = f" {fatalities} fatalities reported." if fatalities else ""
    if summary:
        return f"{incident_count} {dataset_label} incidents {location_note} tied to {event_type.lower() if event_type else 'conflict activity'}.{fatality_note} {summary}"
    return f"{incident_count} incidents of {event_type.lower() if event_type else 'conflict activity'} {location_note}.{fatality_note}"


def _cluster_priority(cluster: list[dict], event_type: str, fatalities: int, source_count: int) -> float:
    base = EVENT_TYPE_WEIGHTS.get(event_type, 4.0)
    return round(base + (fatalities * 1.8) + (len(cluster) * 1.1) + (source_count * 0.6), 2)


def build_map_structured_story_clusters(*, structured_days: int, limit: int = 36, dataset: str | None = None) -> list[dict]:
    days = max(1, min(int(structured_days), 60))
    source_limit = max(3200, days * 120)
    return build_structured_story_clusters(days=days, limit=limit, source_limit=source_limit, dataset=dataset)


def build_structured_story_clusters(
    *,
    days: int = 3,
    limit: int = 12,
    country: str | None = None,
    event_type: str | None = None,
    source_limit: int = 3000,
    dataset: str | None = "acled",
) -> list[dict]:
    structured_events = get_recent_structured_events(
        days=max(1, days),
        limit=max(limit * 80, source_limit),
        dataset=dataset,
        country=country,
        event_type=event_type,
    )
    if not structured_events:
        return []

    signatures = [_event_signature(event) for event in structured_events]
    groups: list[list[int]] = []
    for index, signature in enumerate(signatures):
        placed = False
        for group in groups:
            if any(_is_related(signature, signatures[other]) for other in group):
                group.append(index)
                placed = True
                break
        if not placed:
            groups.append([index])

    clusters = []
    for group_index, group in enumerate(groups, 1):
        cluster = [structured_events[i] for i in group]
        country_counter = Counter(_clean_text(item.get("country")) for item in cluster if _clean_text(item.get("country")))
        event_type_counter = Counter(_clean_text(item.get("event_type")) for item in cluster if _clean_text(item.get("event_type")))
        sub_event_counter = Counter(_clean_text(item.get("sub_event_type")) for item in cluster if _clean_text(item.get("sub_event_type")) and not str(item.get("sub_event_type")).isdigit())
        actor_counter = Counter(
            value
            for item in cluster
            for value in [_clean_text(item.get("actor_primary")), _clean_text(item.get("actor_secondary"))]
            if value
        )
        location_counter = Counter(
            value
            for item in cluster
            for value in [_clean_text(item.get("location")), _clean_text(item.get("admin2")), _clean_text(item.get("admin1"))]
            if value and not re.match(r"^[A-Z]{2}[A-Z0-9]{0,4}$", value)
        )
        source_counter = Counter(
            _clean_text((item.get("payload") or {}).get("source"))
            for item in cluster
            if _clean_text((item.get("payload") or {}).get("source"))
        )
        dataset_counter = Counter(_clean_text(item.get("dataset")) for item in cluster if _clean_text(item.get("dataset")))

        primary_country = _top_values(country_counter, limit=1)[0] if country_counter else "Unknown country"
        primary_event_type = _top_values(event_type_counter, limit=1)[0] if event_type_counter else "Structured event"
        primary_sub_event_type = _top_values(sub_event_counter, limit=1)[0] if sub_event_counter else primary_event_type
        actor_focus = _top_values(actor_counter, limit=6)
        location_focus = _top_values(location_counter, limit=6)
        sources = _top_values(source_counter, limit=6)
        fatalities = sum(int(item.get("fatalities") or 0) for item in cluster)
        earliest = min((item.get("event_date") for item in cluster if item.get("event_date")), default=None)
        latest = max((item.get("event_date") for item in cluster if item.get("event_date")), default=None)

        material = "|".join(sorted(item["event_id"] for item in cluster))
        cluster_id = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
        primary_dataset = _top_values(dataset_counter, limit=1)[0] if dataset_counter else "acled"
        dataset_label = "ACLED" if primary_dataset == "acled" else primary_dataset.upper()

        cluster_payload = {
            "event_id": f"structured-{_slug(primary_country)}-{group_index}-{cluster_id}",
            "dataset": primary_dataset,
            "topic": "geopolitics",
            "label": _cluster_label(primary_country, primary_event_type, primary_sub_event_type, actor_focus, location_focus),
            "primary_event_type": primary_event_type,
            "primary_sub_event_type": primary_sub_event_type,
            "summary": _cluster_summary(cluster, primary_country, primary_event_type, location_focus, fatalities, dataset_label),
            "entity_focus": actor_focus,
            "country_focus": _top_values(country_counter, limit=4),
            "location_focus": location_focus,
            "story_anchor_focus": [value for value in [primary_event_type, primary_sub_event_type] if value][:4],
            "source_count": len(source_counter),
            "article_count": 0,
            "structured_event_count": len(cluster),
            "fatality_total": fatalities,
            "latest_update": latest,
            "earliest_update": earliest,
            "analysis_priority": _cluster_priority(cluster, primary_event_type, fatalities, len(source_counter)),
            "sources": sources,
            "events": [
                {
                    "event_id": item["event_id"],
                    "event_date": item.get("event_date"),
                    "country": item.get("country"),
                    "admin1": item.get("admin1"),
                    "admin2": item.get("admin2"),
                    "location": item.get("location"),
                    "latitude": item.get("latitude"),
                    "longitude": item.get("longitude"),
                    "dataset": item.get("dataset"),
                    "event_type": item.get("event_type"),
                    "sub_event_type": item.get("sub_event_type"),
                    "actor_primary": item.get("actor_primary"),
                    "actor_secondary": item.get("actor_secondary"),
                    "fatalities": item.get("fatalities"),
                    "source_count": item.get("source_count"),
                    "source_urls": item.get("source_urls") or [],
                    "summary": item.get("summary"),
                    "payload": item.get("payload") or {},
                }
                for item in cluster[:120]
            ],
        }
        cluster_payload.update(build_event_intelligence(cluster_payload, cluster_payload["events"]))
        clusters.append(cluster_payload)

    clusters.sort(
        key=lambda item: (
            item.get("importance_score", 0),
            item["analysis_priority"],
            item["fatality_total"],
            item["structured_event_count"],
            item.get("latest_update") or "",
        ),
        reverse=True,
    )
    return clusters[:limit]
