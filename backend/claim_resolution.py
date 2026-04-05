import hashlib
import re
from collections import defaultdict
from datetime import datetime, timezone

from corpus import (
    get_recent_articles,
    get_recent_contradiction_records,
    load_latest_source_reliability,
    replace_claim_resolution_snapshot,
    save_source_reliability_snapshot,
)


CLAIM_STOPWORDS = {
    "the", "and", "that", "with", "from", "this", "into", "after", "over", "under", "amid", "against",
    "about", "their", "there", "which", "would", "could", "should", "while", "where", "when", "what", "than",
    "been", "have", "has", "were", "will", "says", "said", "say", "report", "reports", "reported", "according",
}

CLAIM_VERB_HINTS = {
    "says", "said", "warns", "warned", "claims", "claimed", "announces", "announced", "vows", "vowed",
    "confirms", "confirmed", "denies", "denied", "approves", "approved", "rejects", "rejected", "threatens",
    "threatened", "expects", "expected", "targets", "targeted", "strikes", "struck", "imposes", "imposed",
    "halts", "halted", "resumes", "resumed", "cuts", "cut", "raises", "raised", "launches", "launched",
}

CLAIM_TYPE_KEYWORDS = {
    "timeline": ["today", "tomorrow", "weeks", "days", "deadline", "by monday", "by friday", "soon", "delayed"],
    "scale": ["dozens", "hundreds", "thousands", "surge", "record", "largest", "smallest", "major", "minor"],
    "causality": ["because", "after", "following", "in response", "prompting", "leading to", "caused by"],
    "intent": ["aims", "seeks", "plans", "intends", "goal", "objective", "strategy", "wants to"],
    "sanctions": ["sanction", "blacklist", "designation", "asset freeze", "penalty", "ofac"],
    "markets": ["stocks", "shares", "oil", "prices", "market", "yield", "inflation", "rates", "gdp"],
    "military": ["strike", "missile", "troops", "drone", "airstrike", "attack", "ceasefire", "offensive"],
    "diplomacy": ["talks", "negotiation", "envoy", "mediation", "summit", "meeting", "agreement"],
}


def _normalize_source_name(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())


def _normalize_claim_text(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _content_tokens(text: str) -> set[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z\-]{2,}", (text or "").lower())
    return {token for token in tokens if token not in CLAIM_STOPWORDS}


def _claim_signature(text: str) -> str:
    tokens = sorted(_content_tokens(text))
    return " ".join(tokens[:12])


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    compact = re.match(r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$", text)
    if compact:
        year, month, day, hour, minute, second = compact.groups()
        return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _source_key(source_name: str) -> str:
    return _normalize_source_name(source_name)


def _claim_key(group_key: str, source_name: str, claim_text: str) -> str:
    return hashlib.sha256(
        " | ".join([group_key, _normalize_source_name(source_name), _normalize_claim_text(claim_text)]).encode("utf-8")
    ).hexdigest()


def _article_text(article: dict) -> str:
    return " ".join(
        [
            article.get("translated_title") or article.get("title") or "",
            article.get("translated_description") or article.get("description") or "",
        ]
    ).strip()


def _classify_claim_type(text: str, fallback: str = "fact") -> str:
    lowered = text.lower()
    for claim_type, keywords in CLAIM_TYPE_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return claim_type
    return fallback


def _extract_claim_candidates(article: dict, topic: str | None) -> list[dict]:
    text = _article_text(article)
    if not text:
        return []

    raw_sentences = re.split(r"(?<=[.!?])\s+|(?<=:)\s+", text)
    sentences = []
    for sentence in raw_sentences:
        cleaned = _normalize_claim_text(sentence)
        if len(cleaned) < 35:
            continue
        if len(_content_tokens(cleaned)) < 4:
            continue
        lowered = cleaned.lower()
        if not any(hint in lowered for hint in CLAIM_VERB_HINTS) and article.get("title") and cleaned != _normalize_claim_text(article.get("title")):
            continue
        sentences.append(cleaned)

    if article.get("title"):
        normalized_title = _normalize_claim_text(article["title"])
        if normalized_title and normalized_title not in sentences and len(_content_tokens(normalized_title)) >= 4:
            sentences.insert(0, normalized_title)

    deduped = []
    seen = set()
    for sentence in sentences[:4]:
        signature = _claim_signature(sentence)
        if not signature or signature in seen:
            continue
        seen.add(signature)
        deduped.append(
            {
                "group_key": f"article:{topic or 'global'}:{signature}",
                "event_key": None,
                "topic": topic,
                "event_label": article.get("title"),
                "source_name": article.get("source") or "Unknown source",
                "claim_text": sentence,
                "opposing_claim_text": None,
                "conflict_type": _classify_claim_type(sentence),
                "resolution_status": "unresolved",
                "confidence": 0.45,
                "evidence_url": article.get("url"),
                "published_at": article.get("published_at"),
                "payload": {
                    "origin": "article_direct",
                    "provider": article.get("provider"),
                    "language": article.get("language"),
                    "signature": signature,
                },
            }
        )
    return deduped


def _claims_are_similar(left: str, right: str) -> bool:
    left_tokens = _content_tokens(left)
    right_tokens = _content_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    overlap = len(left_tokens & right_tokens)
    return overlap >= 4 or overlap >= min(len(left_tokens), len(right_tokens)) * 0.6


def _resolve_contradiction_status(source_name: str, credible_source: str | None, unresolved: bool) -> str:
    if unresolved or not credible_source:
        return "unresolved"
    return "corroborated" if _normalize_source_name(source_name) == credible_source else "contradicted"


def _source_record_for(contradiction: dict, source_name: str) -> dict | None:
    target = _normalize_source_name(source_name)
    for record in contradiction.get("source_records") or []:
        if _normalize_source_name(record.get("source")) == target:
            return record
    return None


def _build_contradiction_claim_rows(topic: str | None, days: int, generated_at: float) -> list[dict]:
    rows = []
    records = get_recent_contradiction_records(topic=topic, hours=days * 24, limit=800)
    for record in records:
        event_key = record["event_key"]
        event_label = record.get("event_label")
        latest_update = record.get("latest_update")
        for contradiction in record.get("contradictions") or []:
            sources = contradiction.get("sources_in_conflict") or []
            if len(sources) < 2:
                continue

            claim_a = _normalize_claim_text(contradiction.get("claim_a"))
            claim_b = _normalize_claim_text(contradiction.get("claim_b"))
            if not claim_a or not claim_b:
                continue

            credible_source = _normalize_source_name(contradiction.get("most_credible_source"))
            unresolved = not credible_source or credible_source == "unresolved"
            confidence = float(contradiction.get("confidence", 0) or 0)
            conflict_type = contradiction.get("conflict_type") or _classify_claim_type(f"{claim_a} {claim_b}")

            for source_name, claim_text, opposing_claim in [
                (sources[0], claim_a, claim_b),
                (sources[1], claim_b, claim_a),
            ]:
                source_record = _source_record_for(contradiction, source_name)
                rows.append(
                    {
                        "group_key": f"contradiction:{event_key}:{_claim_signature(claim_text)}",
                        "claim_record_key": _claim_key(event_key, source_name, claim_text),
                        "event_key": event_key,
                        "topic": record.get("topic"),
                        "event_label": event_label,
                        "source_name": source_name,
                        "claim_text": claim_text,
                        "opposing_claim_text": opposing_claim,
                        "conflict_type": conflict_type,
                        "resolution_status": _resolve_contradiction_status(source_name, credible_source, unresolved),
                        "confidence": confidence,
                        "evidence_url": (source_record or {}).get("url"),
                        "published_at": (source_record or {}).get("published_at") or latest_update,
                        "payload": {
                            "origin": "contradiction",
                            "most_credible_source": contradiction.get("most_credible_source"),
                            "reasoning": contradiction.get("reasoning"),
                            "sources_in_conflict": sources,
                        },
                        "generated_at": generated_at,
                    }
                )
    return rows


def _build_direct_claim_rows(topic: str | None, days: int, generated_at: float) -> list[dict]:
    articles = get_recent_articles(topic=topic, limit=700, hours=days * 24)
    rows = []
    for article in articles:
        rows.extend(_extract_claim_candidates(article, topic))
    for row in rows:
        row["claim_record_key"] = _claim_key(row["group_key"], row["source_name"], row["claim_text"])
        row["generated_at"] = generated_at
    return rows


def _resolve_direct_claim_rows(rows: list[dict]) -> list[dict]:
    by_group = defaultdict(list)
    for row in rows:
        by_group[row["group_key"]].append(row)

    resolved = []
    for group_key, group_rows in by_group.items():
        group_rows.sort(key=lambda row: _parse_timestamp(row.get("published_at")) or datetime.max.replace(tzinfo=timezone.utc))
        canonical_claims: list[dict] = []
        for row in group_rows:
            matched_group = None
            for canonical in canonical_claims:
                if _claims_are_similar(canonical["claim_text"], row["claim_text"]):
                    matched_group = canonical
                    break
            if matched_group is None:
                canonical_claims.append({"claim_text": row["claim_text"], "rows": [row]})
            else:
                matched_group["rows"].append(row)

        if len(canonical_claims) == 1:
            cluster = canonical_claims[0]["rows"]
            unique_sources = {_source_key(row["source_name"]) for row in cluster}
            unique_sources.discard("")
            status = "unresolved"
            if len(unique_sources) >= 2:
                earliest = min((_parse_timestamp(row.get("published_at")) for row in cluster), default=None)
                latest = max((_parse_timestamp(row.get("published_at")) for row in cluster), default=None)
                if earliest and latest and (latest - earliest).total_seconds() >= 6 * 3600:
                    status = "vindicated_later"
                else:
                    status = "corroborated"
            for row in cluster:
                resolved.append(
                    {
                        **row,
                        "resolution_status": status if len(unique_sources) >= 2 else "unresolved",
                    }
                )
            continue

        dominant = max(canonical_claims, key=lambda item: len({_source_key(row["source_name"]) for row in item["rows"]}))
        dominant_sources = {_source_key(row["source_name"]) for row in dominant["rows"]}
        for cluster in canonical_claims:
            cluster_sources = {_source_key(row["source_name"]) for row in cluster["rows"]}
            if cluster is dominant and len(dominant_sources) >= 2:
                earliest = min((_parse_timestamp(row.get("published_at")) for row in cluster["rows"]), default=None)
                latest = max((_parse_timestamp(row.get("published_at")) for row in cluster["rows"]), default=None)
                cluster_status = "vindicated_later" if earliest and latest and (latest - earliest).total_seconds() >= 12 * 3600 else "corroborated"
            elif cluster is dominant:
                cluster_status = "unresolved"
            else:
                cluster_status = "contradicted"

            opposing_claim = dominant["claim_text"] if cluster is not dominant else None
            for row in cluster["rows"]:
                resolved.append(
                    {
                        **row,
                        "opposing_claim_text": opposing_claim,
                        "resolution_status": cluster_status,
                    }
                )
    return resolved


def _topic_breakdowns(claim_rows: list[dict]) -> tuple[dict, dict]:
    by_topic = defaultdict(lambda: {"claim_count": 0, "statuses": defaultdict(int)})
    by_claim_type = defaultdict(lambda: {"claim_count": 0, "statuses": defaultdict(int)})
    for row in claim_rows:
        topic_key = row.get("topic") or "global"
        claim_type = row.get("conflict_type") or "fact"
        status = row.get("resolution_status") or "unresolved"
        by_topic[topic_key]["claim_count"] += 1
        by_topic[topic_key]["statuses"][status] += 1
        by_claim_type[claim_type]["claim_count"] += 1
        by_claim_type[claim_type]["statuses"][status] += 1
    return by_topic, by_claim_type


def _build_source_rows(source_stats: dict, snapshot_key: str, topic: str | None, generated_at: float) -> list[dict]:
    rows = []
    for stats in source_stats.values():
        claim_count = stats["claim_count"]
        corroborated = stats["corroborated_count"]
        contradicted = stats["contradicted_count"]
        unresolved = stats["unresolved_count"]
        mixed = stats["mixed_count"]
        vindicated = stats["vindicated_later_count"]

        empirical_score = round(
            (corroborated + (1.15 * vindicated) + (0.5 * mixed) + (0.3 * unresolved) + 1.0) / (claim_count + 2.0),
            3,
        )
        weight_multiplier = round(0.65 + (empirical_score * 0.9), 3)
        rows.append(
            {
                "source_name": stats["source_name"],
                "corroborated_count": corroborated,
                "contradicted_count": contradicted,
                "unresolved_count": unresolved,
                "mixed_count": mixed,
                "claim_count": claim_count,
                "empirical_score": empirical_score,
                "weight_multiplier": weight_multiplier,
                "payload": {
                    "vindicated_later_count": vindicated,
                    "topic_breakdown": stats["topic_breakdown"],
                    "claim_type_breakdown": stats["claim_type_breakdown"],
                    "examples": stats["examples"],
                    "snapshot_key": snapshot_key,
                },
                "generated_at": generated_at,
            }
        )
    rows.sort(key=lambda row: (row["empirical_score"], row["claim_count"]), reverse=True)
    save_source_reliability_snapshot(snapshot_key, rows, topic=topic)
    return rows


def build_claim_resolution_snapshot(topic: str | None = None, days: int = 180) -> dict:
    generated_at = datetime.now(timezone.utc).timestamp()
    snapshot_key = f"claim-resolution:{topic or 'global'}:{days}"

    contradiction_rows = _build_contradiction_claim_rows(topic=topic, days=days, generated_at=generated_at)
    direct_rows = _resolve_direct_claim_rows(_build_direct_claim_rows(topic=topic, days=days, generated_at=generated_at))
    claim_rows = contradiction_rows + direct_rows
    deduped_claim_rows = list({row["claim_record_key"]: row for row in claim_rows}.values())
    replace_claim_resolution_snapshot(snapshot_key, deduped_claim_rows)

    source_stats: dict[str, dict] = {}
    for row in deduped_claim_rows:
        source_key = _source_key(row["source_name"])
        bucket = source_stats.setdefault(
            source_key,
            {
                "source_name": row["source_name"],
                "corroborated_count": 0,
                "contradicted_count": 0,
                "unresolved_count": 0,
                "mixed_count": 0,
                "vindicated_later_count": 0,
                "claim_count": 0,
                "topic_breakdown": defaultdict(lambda: defaultdict(int)),
                "claim_type_breakdown": defaultdict(lambda: defaultdict(int)),
                "examples": [],
            },
        )
        status = row.get("resolution_status") or "unresolved"
        if status not in {"corroborated", "contradicted", "unresolved", "mixed", "vindicated_later"}:
            status = "unresolved"
        bucket["claim_count"] += 1
        counter_key = f"{status}_count"
        if counter_key not in bucket:
            bucket[counter_key] = 0
        bucket[counter_key] += 1
        topic_key = row.get("topic") or "global"
        claim_type = row.get("conflict_type") or "fact"
        bucket["topic_breakdown"][topic_key][status] += 1
        bucket["claim_type_breakdown"][claim_type][status] += 1
        if len(bucket["examples"]) < 8:
            bucket["examples"].append(
                {
                    "claim_text": row["claim_text"],
                    "resolution_status": status,
                    "claim_type": claim_type,
                    "event_label": row.get("event_label"),
                    "published_at": row.get("published_at"),
                    "evidence_url": row.get("evidence_url"),
                    "origin": (row.get("payload") or {}).get("origin"),
                }
            )

    for bucket in source_stats.values():
        bucket["topic_breakdown"] = {
            key: {"claim_count": sum(statuses.values()), "statuses": dict(statuses)}
            for key, statuses in bucket["topic_breakdown"].items()
        }
        bucket["claim_type_breakdown"] = {
            key: {"claim_count": sum(statuses.values()), "statuses": dict(statuses)}
            for key, statuses in bucket["claim_type_breakdown"].items()
        }

    source_rows = _build_source_rows(source_stats, snapshot_key, topic, generated_at)
    by_topic, by_claim_type = _topic_breakdowns(deduped_claim_rows)
    return {
        "snapshot_key": snapshot_key,
        "topic": topic,
        "days": days,
        "claim_records": len(deduped_claim_rows),
        "sources": source_rows,
        "breakdowns": {
            "topics": {
                key: {"claim_count": value["claim_count"], "statuses": dict(value["statuses"])}
                for key, value in by_topic.items()
            },
            "claim_types": {
                key: {"claim_count": value["claim_count"], "statuses": dict(value["statuses"])}
                for key, value in by_claim_type.items()
            },
        },
        "generated_at": generated_at,
    }


def get_source_reliability(topic: str | None = None, days: int = 180, refresh: bool = False) -> dict:
    if not refresh:
        current = load_latest_source_reliability(topic=topic, max_age_hours=12)
        if current:
            rows = sorted(current.values(), key=lambda row: (row["empirical_score"], row["claim_count"]), reverse=True)
            breakdowns = {
                "topics": {},
                "claim_types": {},
            }
            for row in rows:
                payload = row.get("payload") or {}
                for topic_key, value in (payload.get("topic_breakdown") or {}).items():
                    bucket = breakdowns["topics"].setdefault(topic_key, {"claim_count": 0, "statuses": defaultdict(int)})
                    bucket["claim_count"] += int(value.get("claim_count", 0) or 0)
                    for status, count in (value.get("statuses") or {}).items():
                        bucket["statuses"][status] += int(count or 0)
                for claim_type, value in (payload.get("claim_type_breakdown") or {}).items():
                    bucket = breakdowns["claim_types"].setdefault(claim_type, {"claim_count": 0, "statuses": defaultdict(int)})
                    bucket["claim_count"] += int(value.get("claim_count", 0) or 0)
                    for status, count in (value.get("statuses") or {}).items():
                        bucket["statuses"][status] += int(count or 0)
            normalized_breakdowns = {
                group: {
                    key: {"claim_count": value["claim_count"], "statuses": dict(value["statuses"])}
                    for key, value in bucket.items()
                }
                for group, bucket in breakdowns.items()
            }
            return {
                "topic": topic,
                "days": days,
                "sources": rows,
                "breakdowns": normalized_breakdowns,
                "generated_at": max(row["generated_at"] for row in rows),
                "cached": True,
            }

    refreshed = build_claim_resolution_snapshot(topic=topic, days=days)
    return {
        "topic": topic,
        "days": days,
        "sources": refreshed["sources"],
        "claim_records": refreshed["claim_records"],
        "breakdowns": refreshed["breakdowns"],
        "generated_at": refreshed["generated_at"],
        "cached": False,
    }
