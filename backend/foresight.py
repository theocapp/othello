import hashlib
import re
import time
from datetime import datetime, timezone

from corpus import (
    load_before_news_archive,
    load_event_observation_records,
    load_prediction_records,
    search_recent_articles_by_keywords,
    upsert_event_observations,
    upsert_prediction_records,
)

SECTION_NAMES = ("PREDICTIONS", "WHAT TO WATCH")
ALL_BRIEFING_SECTIONS = (
    "SITUATION REPORT",
    "KEY DEVELOPMENTS",
    "CRITICAL ACTORS",
    "SIGNAL vs NOISE",
    "PREDICTIONS",
    "DEEPER CONTEXT",
    "WHAT TO WATCH",
    "SOURCE CONTRADICTIONS",
)
GENERIC_SUBJECTS = {
    "monitor",
    "expect",
    "articles",
    "signals",
    "reporting",
    "briefing",
    "sources",
    "events",
}
MAJOR_SOURCES = {
    "reuters",
    "associated press",
    "the associated press",
    "ap",
    "afp",
    "bbc",
    "financial times",
    "le monde",
}
PREDICTION_TYPE_KEYWORDS = {
    "military": {
        "strike",
        "offensive",
        "escalation",
        "incursion",
        "retaliation",
        "ceasefire",
        "missile",
    },
    "diplomacy": {
        "talks",
        "negotiation",
        "summit",
        "ceasefire",
        "deal",
        "agreement",
        "mediation",
    },
    "sanctions": {"sanction", "tariff", "restriction", "blacklist", "embargo"},
    "markets": {"rate", "inflation", "market", "recession", "oil", "currency", "yield"},
    "political": {
        "election",
        "cabinet",
        "parliament",
        "coalition",
        "protest",
        "leadership",
    },
}
CONFIDENCE_MARKERS = {
    "high": {"very likely", "high confidence", "highly likely", "almost certain"},
    "medium": {"likely", "probable", "expected", "should"},
    "low": {"possible", "may", "could", "watch for", "monitor"},
}


def _hash_key(*parts: str) -> str:
    return hashlib.sha256(
        " | ".join((part or "").strip() for part in parts).encode("utf-8")
    ).hexdigest()


def _parse_section(text: str, section_name: str) -> str:
    escaped = re.escape(section_name)
    next_sections = "|".join(re.escape(name) for name in ALL_BRIEFING_SECTIONS)
    pattern = rf"(?:^|\n){escaped}:\s*(.*?)(?=\n(?:{next_sections}):|\Z)"
    match = re.search(pattern, text or "", flags=re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _extract_prediction_lines(text: str) -> list[str]:
    lines = []
    for raw_line in (text or "").splitlines():
        cleaned = raw_line.strip()
        if not cleaned:
            continue
        cleaned = re.sub(r"^[-*]\s*", "", cleaned)
        cleaned = re.sub(r"^\d+\.\s*", "", cleaned)
        cleaned = cleaned.strip(" .")
        if cleaned.endswith(":"):
            continue
        if cleaned.lower().startswith(("articles included", "event clusters analyzed")):
            continue
        if len(cleaned) < 12:
            continue
        lines.append(cleaned)
    if lines:
        return lines
    fragments = [
        fragment.strip(" .")
        for fragment in re.split(r"[;\n]", text or "")
        if len(fragment.strip()) >= 12
    ]
    return fragments[:6]


def _prediction_horizon_days(text: str, default_days: int = 30) -> int:
    lowered = (text or "").lower()
    day_match = re.search(r"within\s+(\d+)\s+day", lowered)
    if day_match:
        return max(1, min(365, int(day_match.group(1))))
    week_match = re.search(r"within\s+(\d+)\s+week", lowered)
    if week_match:
        return max(7, min(365, int(week_match.group(1)) * 7))
    if "next 48 hours" in lowered:
        return 2
    if "next 72 hours" in lowered:
        return 3
    if "next week" in lowered:
        return 7
    if "coming weeks" in lowered:
        return 21
    if "coming days" in lowered:
        return 7
    if "next month" in lowered:
        return 30
    return default_days


def _prediction_confidence(text: str, section_name: str) -> str:
    lowered = (text or "").lower()
    for label, markers in CONFIDENCE_MARKERS.items():
        if any(marker in lowered for marker in markers):
            return label
    return "low" if section_name == "WHAT TO WATCH" else "medium"


def _prediction_type(text: str) -> str:
    lowered = (text or "").lower()
    for label, markers in PREDICTION_TYPE_KEYWORDS.items():
        if any(marker in lowered for marker in markers):
            return label
    return "general"


def _extract_subjects(text: str, events: list[dict] | None = None) -> list[str]:
    subjects = []
    for event in events or []:
        for entity in event.get("entity_focus", [])[:6]:
            if entity and entity in text:
                subjects.append(entity)
    title_case_hits = re.findall(r"\b(?:[A-Z][a-z]+\s){0,3}[A-Z][a-z]+\b", text or "")
    for hit in title_case_hits:
        cleaned = hit.strip()
        if len(cleaned) >= 4:
            subjects.append(cleaned)
    seen = set()
    ordered = []
    for subject in subjects:
        key = subject.lower()
        if key in GENERIC_SUBJECTS:
            continue
        if key in seen:
            continue
        seen.add(key)
        ordered.append(subject)
    return ordered[:6]


def extract_predictions_from_briefing(
    topic: str,
    briefing_text: str,
    source_ref: str,
    generated_at: float | None = None,
    events: list[dict] | None = None,
) -> list[dict]:
    created_at = generated_at or time.time()
    records = []
    for section_name in SECTION_NAMES:
        section = _parse_section(briefing_text, section_name)
        if not section:
            continue
        for line in _extract_prediction_lines(section):
            horizon_days = _prediction_horizon_days(
                line, default_days=14 if section_name == "WHAT TO WATCH" else 30
            )
            prediction_key = _hash_key(topic, source_ref, line.lower())
            records.append(
                {
                    "prediction_key": prediction_key,
                    "topic": topic,
                    "source_type": "briefing",
                    "source_ref": source_ref,
                    "prediction_text": line,
                    "prediction_horizon_days": horizon_days,
                    "prediction_type": _prediction_type(line),
                    "extracted_subjects": _extract_subjects(line, events=events),
                    "status": "pending",
                    "confidence": _prediction_confidence(line, section_name),
                    "created_at": created_at,
                    "horizon_at": created_at + (horizon_days * 86400),
                    "resolved_at": None,
                    "outcome_summary": None,
                    "payload": {
                        "section": section_name,
                        "topic": topic,
                        "source_ref": source_ref,
                    },
                }
            )
    return records


def _prediction_query(record: dict) -> str:
    subjects = record.get("extracted_subjects") or []
    if subjects:
        query = " ".join(subjects[:3])
    else:
        words = [
            word
            for word in re.findall(
                r"[A-Za-z][A-Za-z-]{3,}", record.get("prediction_text", "")
            )
            if word.lower()
            not in {
                "likely",
                "within",
                "monitor",
                "expect",
                "watch",
                "current",
                "signals",
            }
        ]
        query = " ".join(words[:6])
    type_keywords = list(
        PREDICTION_TYPE_KEYWORDS.get(record.get("prediction_type"), set())
    )
    if type_keywords:
        query = f"{query} {' '.join(type_keywords[:2])}".strip()
    return query.strip()


def _article_within_window(article: dict, start_ts: float, end_ts: float) -> bool:
    published = article.get("published_at")
    if not published:
        return False
    parsed = _parse_timestamp(published)
    if not parsed:
        return False
    ts = parsed.timestamp()
    return start_ts <= ts <= end_ts


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    compact = re.match(r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$", text)
    if compact:
        year, month, day, hour, minute, second = compact.groups()
        return datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second),
            tzinfo=timezone.utc,
        )
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def evaluate_prediction_ledger(
    topic: str | None = None, limit: int = 200, grace_days: int = 3
) -> dict:
    records = load_prediction_records(topic=topic, limit=limit)
    now = time.time()
    updated = []
    counts = {
        "pending": 0,
        "resolved_hit": 0,
        "resolved_miss": 0,
        "expired_unresolved": 0,
    }

    for record in records:
        if record.get("status") not in {"pending", "expired_unresolved"}:
            counts[record.get("status", "pending")] = (
                counts.get(record.get("status", "pending"), 0) + 1
            )
            continue

        end_ts = float(record["horizon_at"]) + (grace_days * 86400)
        if now < float(record["horizon_at"]):
            counts["pending"] += 1
            continue

        query = _prediction_query(record)
        search_hours = max(
            72, int(((end_ts - float(record["created_at"])) / 3600) + 24)
        )
        candidate_articles = search_recent_articles_by_keywords(
            query, topic=record.get("topic"), limit=40, hours=search_hours
        )
        relevant = [
            article
            for article in candidate_articles
            if _article_within_window(article, float(record["created_at"]), end_ts)
        ]
        unique_sources = sorted(
            {
                (article.get("source") or "").strip()
                for article in relevant
                if article.get("source")
            }
        )

        if len(unique_sources) >= 2:
            record["status"] = "resolved_hit"
            record["resolved_at"] = now
            record["outcome_summary"] = (
                f"Corroborated by {len(relevant)} articles across {len(unique_sources)} sources inside the prediction window."
            )
            counts["resolved_hit"] += 1
        elif relevant:
            record["status"] = "expired_unresolved"
            record["resolved_at"] = now
            record["outcome_summary"] = (
                "Only weak single-source evidence surfaced inside the prediction window."
            )
            counts["expired_unresolved"] += 1
        else:
            broader_articles = search_recent_articles_by_keywords(
                " ".join((record.get("extracted_subjects") or [])[:3])
                or record.get("prediction_text", ""),
                topic=record.get("topic"),
                limit=20,
                hours=search_hours,
            )
            if broader_articles:
                record["status"] = "resolved_miss"
                record["resolved_at"] = now
                record["outcome_summary"] = (
                    "The subject stayed active, but the predicted development did not clearly materialize inside the stated window."
                )
                counts["resolved_miss"] += 1
            else:
                record["status"] = "expired_unresolved"
                record["resolved_at"] = now
                record["outcome_summary"] = (
                    "No clear corroborating reporting surfaced before the prediction horizon expired."
                )
                counts["expired_unresolved"] += 1

        payload = dict(record.get("payload") or {})
        payload["evaluation_query"] = query
        payload["matched_urls"] = [
            article.get("url") for article in relevant if article.get("url")
        ]
        record["payload"] = payload
        updated.append(record)

    if updated:
        upsert_prediction_records(updated)

    latest = load_prediction_records(topic=topic, limit=limit)
    return {
        "topic": topic,
        "counts": counts,
        "predictions": latest,
    }


def observe_events(events: list[dict], observed_at: float | None = None) -> int:
    timestamp = observed_at or time.time()
    records = []
    for event in events or []:
        articles = event.get("articles") or []
        if not articles:
            continue
        parsed_articles = []
        for article in articles:
            published = _parse_timestamp(article.get("published_at"))
            if not published:
                continue
            parsed_articles.append((published, article))
        if not parsed_articles:
            continue
        parsed_articles.sort(key=lambda item: item[0])
        first_article_time, first_article = parsed_articles[0]
        major_articles = [
            item
            for item in parsed_articles
            if (item[1].get("source") or "").strip().lower() in MAJOR_SOURCES
        ]
        first_major_time, first_major_article = (
            major_articles[0] if major_articles else (None, None)
        )
        article_urls = [
            article.get("url") for _, article in parsed_articles if article.get("url")
        ]
        source_names = sorted(
            {
                article.get("source")
                for _, article in parsed_articles
                if article.get("source")
            }
        )
        event_key = event.get("event_id") or _hash_key(
            event.get("topic", ""), event.get("label", ""), "|".join(article_urls[:12])
        )
        records.append(
            {
                "event_key": event_key,
                "topic": event.get("topic"),
                "event_label": event.get("label") or "Untitled event",
                "first_othello_seen_at": timestamp,
                "latest_othello_seen_at": timestamp,
                "first_article_published_at": first_article_time.isoformat(),
                "first_major_source_published_at": (
                    first_major_time.isoformat() if first_major_time else None
                ),
                "earliest_source": first_article.get("source"),
                "earliest_major_source": (
                    first_major_article.get("source") if first_major_article else None
                ),
                "article_urls": article_urls,
                "source_names": source_names,
                "payload": {
                    "summary": event.get("summary"),
                    "latest_update": event.get("latest_update"),
                    "article_count": event.get("article_count"),
                    "source_count": event.get("source_count"),
                    "contradiction_count": event.get("contradiction_count"),
                },
            }
        )
    return upsert_event_observations(records)


def load_prediction_ledger(
    topic: str | None = None, refresh: bool = False, limit: int = 100
) -> dict:
    if refresh:
        return evaluate_prediction_ledger(topic=topic, limit=limit)
    predictions = load_prediction_records(topic=topic, limit=limit)
    counts = {}
    for record in predictions:
        status = record.get("status", "pending")
        counts[status] = counts.get(status, 0) + 1
    return {"topic": topic, "counts": counts, "predictions": predictions}


def load_early_signal_archive(limit: int = 100, minimum_gap_hours: int = 4) -> dict:
    archive = load_before_news_archive(limit=limit, minimum_gap_hours=minimum_gap_hours)
    if len(archive) < limit:
        threshold_seconds = minimum_gap_hours * 3600
        now = time.time()
        seen = {record["event_key"] for record in archive}
        for record in load_event_observation_records(limit=limit * 3):
            if record["event_key"] in seen:
                continue
            lead_time_seconds = now - float(record["first_othello_seen_at"] or now)
            if lead_time_seconds < threshold_seconds or record.get(
                "first_major_source_published_at"
            ):
                continue
            archive.append(
                {
                    **record,
                    "lead_time_hours": round(lead_time_seconds / 3600, 2),
                    "status": "awaiting_major_pickup",
                }
            )
            if len(archive) >= limit:
                break
    archive.sort(key=lambda item: item.get("lead_time_hours", 0), reverse=True)
    return {
        "count": len(archive),
        "minimum_gap_hours": minimum_gap_hours,
        "records": archive[:limit],
    }
