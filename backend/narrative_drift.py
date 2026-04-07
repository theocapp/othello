import re
from collections import Counter, defaultdict
from datetime import datetime, timezone

from corpus import (
    get_recent_articles,
    load_narrative_drift_snapshot,
    save_article_framing_signals,
    save_narrative_drift_snapshot,
)

FRAME_LEXICON = {
    "terrorism": [
        "terrorist",
        "terrorists",
        "terror group",
        "terror groups",
        "extremist",
        "extremists",
        "jihadist",
        "jihadists",
        "designated terrorist",
        "foreign terrorist organization",
    ],
    "militancy": [
        "militant",
        "militants",
        "armed group",
        "armed groups",
        "fighters",
        "gunmen",
        "operatives",
        "paramilitary",
        "militia",
        "militias",
    ],
    "rebellion": [
        "rebel",
        "rebels",
        "insurgent",
        "insurgents",
        "uprising",
        "opposition fighter",
        "opposition fighters",
        "anti-government",
        "anti government",
    ],
    "governance": [
        "government",
        "administration",
        "authorities",
        "state media",
        "officials",
        "regime",
        "junta",
        "state security",
        "security forces",
        "interior ministry",
    ],
    "resistance": [
        "resistance",
        "freedom fighter",
        "freedom fighters",
        "liberation",
        "movement",
    ],
    "criminality": [
        "criminal",
        "criminals",
        "gang",
        "gangs",
        "cartel",
        "cartels",
        "smuggler",
        "smugglers",
    ],
    "separatism": [
        "separatist",
        "separatists",
        "breakaway region",
        "self-proclaimed republic",
        "self declared republic",
    ],
    "proxy": [
        "proxy",
        "proxy force",
        "proxy forces",
        "proxy militia",
        "proxy militias",
        "proxy war",
        "iran-backed",
        "iran backed",
        "russia-backed",
        "russia backed",
        "turkey-backed",
        "turkey backed",
        "us-backed",
        "u.s.-backed",
        "western-backed",
        "western backed",
    ],
    "occupation": [
        "occupation",
        "occupying force",
        "occupying forces",
        "occupier",
        "occupiers",
        "annexed",
        "annexation",
    ],
    "legitimacy": [
        "recognized government",
        "internationally recognized",
        "de facto authority",
        "de facto authorities",
        "self-styled",
        "self styled",
        "self-proclaimed",
        "self proclaimed",
        "interim government",
    ],
    "diplomacy": [
        "ceasefire",
        "truce",
        "talks",
        "negotiations",
        "peace talks",
        "mediated",
        "backchannel",
        "envoy",
    ],
    "humanitarian": [
        "aid convoy",
        "humanitarian",
        "displaced",
        "refugees",
        "civilians",
        "relief effort",
        "famine",
        "starvation",
    ],
}


def _normalize_subject_key(subject: str) -> str:
    return " ".join((subject or "").strip().lower().split())


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


def _article_text(article: dict) -> str:
    return " ".join(
        [
            article.get("translated_title") or article.get("title") or "",
            article.get("translated_description") or article.get("description") or "",
        ]
    ).strip()


def _mentions_subject(article: dict, subject: str) -> bool:
    text = _article_text(article).lower()
    normalized = subject.lower()
    if normalized in text:
        return True
    pieces = [piece for piece in re.split(r"\s+", normalized) if len(piece) >= 4]
    return bool(pieces) and sum(1 for piece in pieces if piece in text) >= max(
        1, min(2, len(pieces))
    )


def _matched_terms(text: str) -> dict[str, list[str]]:
    lowered = text.lower()
    matches: dict[str, list[str]] = {}
    for frame, terms in FRAME_LEXICON.items():
        found = []
        for term in terms:
            if term in lowered:
                found.append(term)
        if found:
            matches[frame] = sorted(set(found))
    return matches


def _article_signal(
    article: dict, subject: str, topic: str | None = None
) -> dict | None:
    if not _mentions_subject(article, subject):
        return None
    text = _article_text(article)
    matched = _matched_terms(text)
    counts = {frame: len(terms) for frame, terms in matched.items()}
    dominant = None
    if counts:
        dominant = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
    return {
        "article_url": article["url"],
        "subject_key": _normalize_subject_key(subject),
        "subject_label": subject.strip(),
        "topic": topic,
        "source": article.get("source"),
        "published_at": article.get("published_at"),
        "dominant_frame": dominant,
        "frame_counts": counts,
        "matched_terms": matched,
        "payload": {
            "title": article.get("title"),
            "original_title": article.get("original_title"),
            "description": article.get("description"),
            "original_description": article.get("original_description"),
            "language": article.get("language"),
            "provider": article.get("provider"),
        },
        "analyzed_at": datetime.now(timezone.utc).timestamp(),
    }


def build_article_framing_signals(
    subject: str, topic: str | None = None, days: int = 180, limit: int = 600
) -> list[dict]:
    articles = get_recent_articles(topic=topic, limit=limit, hours=days * 24)
    signals = []
    for article in articles:
        signal = _article_signal(article, subject, topic=topic)
        if signal:
            signals.append(signal)
    save_article_framing_signals(signals)
    return signals


def _bucket_label(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _period_summary(signals: list[dict]) -> dict:
    frames = Counter()
    terms = Counter()
    sources = Counter()
    examples = defaultdict(list)

    for signal in signals:
        for frame, count in (signal.get("frame_counts") or {}).items():
            frames[frame] += int(count or 0)
        for frame, matched in (signal.get("matched_terms") or {}).items():
            for term in matched:
                terms[(frame, term)] += 1
        if signal.get("source"):
            sources[signal["source"]] += 1
        dominant = signal.get("dominant_frame")
        if dominant and len(examples[dominant]) < 3:
            examples[dominant].append(
                {
                    "source": signal.get("source"),
                    "published_at": signal.get("published_at"),
                    "url": signal.get("article_url"),
                }
            )

    total_frame_mentions = sum(frames.values()) or 1
    top_frames = [
        {
            "frame": frame,
            "mentions": count,
            "share": round(count / total_frame_mentions, 3),
        }
        for frame, count in frames.most_common(5)
    ]
    top_terms = [
        {"frame": frame, "term": term, "mentions": count}
        for (frame, term), count in terms.most_common(8)
    ]
    return {
        "article_count": len(signals),
        "top_frames": top_frames,
        "top_terms": top_terms,
        "top_sources": [
            {"source": source, "count": count}
            for source, count in sources.most_common(6)
        ],
        "examples": dict(examples),
    }


def _share_map(period: dict) -> dict[str, float]:
    return {
        item["frame"]: float(item["share"]) for item in period.get("top_frames", [])
    }


def _frame_share_from_counts(counts: Counter) -> list[dict]:
    total = sum(counts.values()) or 1
    return [
        {"frame": frame, "mentions": count, "share": round(count / total, 3)}
        for frame, count in counts.most_common(6)
    ]


def _detect_shifts(early: dict, recent: dict) -> list[dict]:
    early_shares = _share_map(early)
    recent_shares = _share_map(recent)
    frames = sorted(set(early_shares) | set(recent_shares))
    shifts = []
    for frame in frames:
        delta = round(recent_shares.get(frame, 0.0) - early_shares.get(frame, 0.0), 3)
        if abs(delta) < 0.12:
            continue
        direction = "rising" if delta > 0 else "falling"
        shifts.append(
            {
                "frame": frame,
                "direction": direction,
                "delta_share": delta,
                "early_share": round(early_shares.get(frame, 0.0), 3),
                "recent_share": round(recent_shares.get(frame, 0.0), 3),
            }
        )
    shifts.sort(key=lambda item: abs(item["delta_share"]), reverse=True)
    return shifts


def _source_frame_profiles(signals: list[dict]) -> list[dict]:
    by_source: dict[str, dict] = {}
    for signal in signals:
        source = signal.get("source") or "Unknown source"
        bucket = by_source.setdefault(
            source,
            {
                "source": source,
                "article_count": 0,
                "frame_counts": Counter(),
                "terms": Counter(),
                "examples": [],
            },
        )
        bucket["article_count"] += 1
        for frame, count in (signal.get("frame_counts") or {}).items():
            bucket["frame_counts"][frame] += int(count or 0)
        for frame, terms in (signal.get("matched_terms") or {}).items():
            for term in terms:
                bucket["terms"][(frame, term)] += 1
        if len(bucket["examples"]) < 3:
            bucket["examples"].append(
                {
                    "published_at": signal.get("published_at"),
                    "url": signal.get("article_url"),
                    "dominant_frame": signal.get("dominant_frame"),
                }
            )

    profiles = []
    for source, bucket in by_source.items():
        profiles.append(
            {
                "source": source,
                "article_count": bucket["article_count"],
                "top_frames": _frame_share_from_counts(bucket["frame_counts"]),
                "top_terms": [
                    {"frame": frame, "term": term, "mentions": count}
                    for (frame, term), count in bucket["terms"].most_common(6)
                ],
                "examples": bucket["examples"],
            }
        )
    profiles.sort(
        key=lambda item: (item["article_count"], item["source"]), reverse=True
    )
    return profiles[:12]


def _source_shift_analysis(
    early_signals: list[dict], recent_signals: list[dict]
) -> list[dict]:
    early_profiles = {
        item["source"]: item for item in _source_frame_profiles(early_signals)
    }
    recent_profiles = {
        item["source"]: item for item in _source_frame_profiles(recent_signals)
    }
    shared_sources = sorted(set(early_profiles) & set(recent_profiles))

    comparisons = []
    for source in shared_sources:
        early_profile = early_profiles[source]
        recent_profile = recent_profiles[source]
        early_map = _share_map(early_profile)
        recent_map = _share_map(recent_profile)
        frames = sorted(set(early_map) | set(recent_map))
        deltas = []
        for frame in frames:
            delta = round(recent_map.get(frame, 0.0) - early_map.get(frame, 0.0), 3)
            if abs(delta) < 0.12:
                continue
            deltas.append(
                {
                    "frame": frame,
                    "delta_share": delta,
                    "direction": "rising" if delta > 0 else "falling",
                    "early_share": round(early_map.get(frame, 0.0), 3),
                    "recent_share": round(recent_map.get(frame, 0.0), 3),
                }
            )
        deltas.sort(key=lambda item: abs(item["delta_share"]), reverse=True)
        comparisons.append(
            {
                "source": source,
                "early_article_count": early_profile["article_count"],
                "recent_article_count": recent_profile["article_count"],
                "early_top_frames": early_profile["top_frames"][:4],
                "recent_top_frames": recent_profile["top_frames"][:4],
                "shifts": deltas[:5],
            }
        )

    comparisons.sort(
        key=lambda item: (
            abs(item["shifts"][0]["delta_share"]) if item["shifts"] else 0,
            item["recent_article_count"],
        ),
        reverse=True,
    )
    return comparisons[:10]


def analyze_narrative_drift(
    subject: str,
    topic: str | None = None,
    days: int = 180,
    refresh: bool = False,
) -> dict:
    if not refresh:
        cached = load_narrative_drift_snapshot(
            subject, topic=topic, window_days=days, max_age_hours=12
        )
        if cached:
            payload = cached["payload"] or {}
            payload.setdefault("source_profiles", [])
            payload.setdefault("source_shifts", [])
            return {**payload, "cached": True}

    signals = build_article_framing_signals(subject, topic=topic, days=days)
    if not signals:
        payload = {
            "subject": subject.strip(),
            "subject_key": _normalize_subject_key(subject),
            "topic": topic,
            "window_days": days,
            "article_count": 0,
            "earliest_published_at": None,
            "latest_published_at": None,
            "timeline": [],
            "early_period": {
                "article_count": 0,
                "top_frames": [],
                "top_terms": [],
                "top_sources": [],
                "examples": {},
            },
            "recent_period": {
                "article_count": 0,
                "top_frames": [],
                "top_terms": [],
                "top_sources": [],
                "examples": {},
            },
            "shifts": [],
            "source_profiles": [],
            "source_shifts": [],
            "reference_note": "Heuristic framing tracker based on article wording over time. It does not affect Othello scoring or contradiction detection.",
        }
        save_narrative_drift_snapshot(
            subject, topic=topic, window_days=days, payload=payload
        )
        return {**payload, "cached": False}

    dated_signals = []
    for signal in signals:
        parsed = _parse_timestamp(signal.get("published_at"))
        if parsed is None:
            continue
        dated_signals.append((parsed, signal))

    dated_signals.sort(key=lambda item: item[0])
    if not dated_signals:
        payload = {
            "subject": subject.strip(),
            "subject_key": _normalize_subject_key(subject),
            "topic": topic,
            "window_days": days,
            "article_count": 0,
            "earliest_published_at": None,
            "latest_published_at": None,
            "timeline": [],
            "early_period": {
                "article_count": 0,
                "top_frames": [],
                "top_terms": [],
                "top_sources": [],
                "examples": {},
            },
            "recent_period": {
                "article_count": 0,
                "top_frames": [],
                "top_terms": [],
                "top_sources": [],
                "examples": {},
            },
            "shifts": [],
            "source_profiles": [],
            "source_shifts": [],
            "reference_note": "Heuristic framing tracker based on article wording over time. It does not affect Othello scoring or contradiction detection.",
        }
        save_narrative_drift_snapshot(
            subject, topic=topic, window_days=days, payload=payload
        )
        return {**payload, "cached": False}

    midpoint = max(1, len(dated_signals) // 2)
    early_signals = [signal for _, signal in dated_signals[:midpoint]]
    recent_signals = [signal for _, signal in dated_signals[midpoint:]]
    if not recent_signals:
        recent_signals = early_signals

    timeline_map: dict[str, Counter] = {}
    source_examples: dict[str, list[dict]] = defaultdict(list)
    for parsed, signal in dated_signals:
        label = _bucket_label(parsed)
        timeline_map.setdefault(label, Counter())
        for frame, count in (signal.get("frame_counts") or {}).items():
            timeline_map[label][frame] += int(count or 0)
        dominant = signal.get("dominant_frame")
        if dominant and len(source_examples[label]) < 3:
            source_examples[label].append(
                {
                    "source": signal.get("source"),
                    "published_at": signal.get("published_at"),
                    "frame": dominant,
                    "url": signal.get("article_url"),
                }
            )

    timeline = []
    for label, counts in sorted(timeline_map.items()):
        total = sum(counts.values()) or 1
        timeline.append(
            {
                "bucket": label,
                "frames": [
                    {
                        "frame": frame,
                        "mentions": count,
                        "share": round(count / total, 3),
                    }
                    for frame, count in counts.most_common()
                ],
                "examples": source_examples.get(label, []),
            }
        )

    early_period = _period_summary(early_signals)
    recent_period = _period_summary(recent_signals)
    shifts = _detect_shifts(early_period, recent_period)
    source_profiles = _source_frame_profiles([signal for _, signal in dated_signals])
    source_shifts = _source_shift_analysis(early_signals, recent_signals)

    payload = {
        "subject": subject.strip(),
        "subject_key": _normalize_subject_key(subject),
        "topic": topic,
        "window_days": days,
        "article_count": len(dated_signals),
        "signal_count": len(signals),
        "earliest_published_at": dated_signals[0][1].get("published_at"),
        "latest_published_at": dated_signals[-1][1].get("published_at"),
        "timeline": timeline,
        "early_period": early_period,
        "recent_period": recent_period,
        "shifts": shifts,
        "source_profiles": source_profiles,
        "source_shifts": source_shifts,
        "reference_note": "Heuristic framing tracker based on article wording over time. It does not affect Othello scoring or contradiction detection.",
    }
    save_narrative_drift_snapshot(
        subject, topic=topic, window_days=days, payload=payload
    )
    payload.setdefault("source_profiles", [])
    payload.setdefault("source_shifts", [])
    return {**payload, "cached": False}
