"""Story clustering subsystem.

This module owns article-signature extraction, relatedness scoring,
and observation assembly (cluster -> event objects).
"""

from __future__ import annotations

import hashlib
import math
import re
from collections import Counter
from datetime import datetime

import spacy

from corpus import get_source_registry, load_latest_source_reliability
from news import normalize_article_description, normalize_article_title

STOPWORDS = {
    "the",
    "and",
    "that",
    "with",
    "from",
    "this",
    "into",
    "after",
    "over",
    "under",
    "amid",
    "against",
    "about",
    "their",
    "there",
    "which",
    "would",
    "could",
    "should",
    "while",
    "where",
    "when",
    "what",
    "than",
    "been",
    "have",
    "has",
    "were",
    "will",
}

FRAMING_LABELS = {
    "terrorist": {"terrorist", "terrorists"},
    "rebel": {"rebel", "rebels"},
    "militant": {"militant", "militants"},
    "insurgent": {"insurgent", "insurgents"},
    "resistance": {"resistance", "fighters", "resistance fighters"},
    "separatist": {"separatist", "separatists"},
}

EVENT_ANCHORS = {
    "strike": {
        "strike",
        "airstrike",
        "attack",
        "raid",
        "bombing",
        "drone",
        "missile",
        "shelling",
        "offensive",
        "assault",
    },
    "ceasefire": {"ceasefire", "truce", "pause", "de-escalation", "armistice"},
    "sanctions": {
        "sanction",
        "sanctions",
        "blacklist",
        "designate",
        "designation",
        "tariff",
        "embargo",
        "penalty",
    },
    "meeting": {
        "meeting",
        "talks",
        "summit",
        "negotiation",
        "negotiations",
        "dialogue",
        "consultation",
    },
    "filing": {
        "filing",
        "petition",
        "indictment",
        "warrant",
        "complaint",
        "case",
        "lawsuit",
        "submission",
    },
    "vote": {"vote", "ballot", "election", "referendum", "resolution"},
    "detention": {"arrest", "detained", "detention", "custody", "jailed", "sentence"},
    "aid": {"aid", "relief", "humanitarian", "shipment", "delivery", "assistance"},
    "market": {
        "stocks",
        "shares",
        "market",
        "inflation",
        "gdp",
        "rates",
        "yield",
        "currency",
        "trade",
        "exports",
        "imports",
    },
    "policy": {"policy", "bill", "law", "decree", "reform", "budget", "package"},
}

ANCHOR_CONFLICTS = {
    frozenset({"strike", "meeting"}),
    frozenset({"strike", "ceasefire"}),
    frozenset({"strike", "aid"}),
    frozenset({"sanctions", "meeting"}),
    frozenset({"sanctions", "aid"}),
    frozenset({"filing", "strike"}),
    frozenset({"filing", "market"}),
    frozenset({"vote", "strike"}),
    frozenset({"vote", "meeting"}),
    frozenset({"market", "strike"}),
}

SENSATIONAL_TITLE_TERMS = {
    "blasts",
    "blast",
    "slams",
    "slam",
    "explodes",
    "explode",
    "shocking",
    "shock",
    "dramatic",
    "chaos",
    "chaotic",
    "furious",
    "stuns",
    "stun",
    "panic",
    "warpath",
}

_nlp = None
_source_registry_cache = None
_source_reliability_cache = {}
_source_reliability_cache_time = {}

RELATEDNESS_THRESHOLD = 4.1


def get_nlp():
    global _nlp
    if _nlp is None:
        try:
            _nlp = spacy.load("en_core_web_sm")
        except OSError as exc:
            raise RuntimeError(
                "spaCy model 'en_core_web_sm' is required for event clustering. "
                "Install it with: python3 -m spacy download en_core_web_sm"
            ) from exc
    return _nlp


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _token_keywords(text: str) -> set[str]:
    words = re.findall(r"[A-Za-z][A-Za-z\-]{3,}", text.lower())
    return {word for word in words if word not in STOPWORDS}


def _parse_published_at(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        return datetime.fromisoformat(text)
    except ValueError:
        compact_match = re.fullmatch(
            r"(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z", text
        )
        if compact_match:
            year, month, day, hour, minute, second = compact_match.groups()
            return datetime.fromisoformat(
                f"{year}-{month}-{day}T{hour}:{minute}:{second}+00:00"
            )
    return None


def _extract_event_anchors(keywords: set[str], text: str) -> set[str]:
    lowered = text.lower()
    anchors = set()
    for anchor, variants in EVENT_ANCHORS.items():
        if keywords & variants or any(variant in lowered for variant in variants):
            anchors.add(anchor)
    return anchors


def _time_distance_hours(left: datetime | None, right: datetime | None) -> float | None:
    if left is None or right is None:
        return None
    return abs((left - right).total_seconds()) / 3600


def _anchor_conflict(left: set[str], right: set[str]) -> bool:
    if not left or not right:
        return False
    for left_anchor in left:
        for right_anchor in right:
            if frozenset({left_anchor, right_anchor}) in ANCHOR_CONFLICTS:
                return True
    return False


def _temporal_weight(hours_apart: float | None) -> float:
    if hours_apart is None:
        return 0.85
    if hours_apart <= 12:
        return 1.0
    if hours_apart <= 36:
        return 0.92
    if hours_apart <= 72:
        return 0.82
    if hours_apart <= 120:
        return 0.7
    return 0.52


def _soft_jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def _article_entities(text: str) -> set[str]:
    doc = get_nlp()(text[:700])
    return {
        ent.text.strip()
        for ent in doc.ents
        if ent.label_ in {"GPE", "PERSON", "ORG", "NORP", "EVENT"}
    }


def _article_framing_labels(text: str) -> set[str]:
    lowered = (text or "").lower()
    labels = set()
    for label, variants in FRAMING_LABELS.items():
        if any(variant in lowered for variant in variants):
            labels.add(label)
    return labels


def _article_signature(article: dict) -> dict:
    text = _normalize_text(
        f"{article.get('title', '')}. {article.get('description', '')}"
    )
    entities = _article_entities(text)
    keywords = _token_keywords(text)
    anchors = _extract_event_anchors(keywords, text)
    published_dt = _parse_published_at(article.get("published_at"))
    return {
        "text": text,
        "entities": entities,
        "keywords": keywords,
        "anchors": anchors,
        "published_at": article.get("published_at"),
        "published_dt": published_dt,
    }


def build_article_signatures(articles: list[dict]) -> list[dict]:
    return [_article_signature(article) for article in articles]


def _text_signature(text: str) -> dict:
    clean = _normalize_text(text)
    keywords = _token_keywords(clean)
    return {
        "text": clean,
        "entities": _article_entities(clean),
        "keywords": keywords,
        "anchors": _extract_event_anchors(keywords, clean),
        "framing_labels": _article_framing_labels(clean),
    }


def _source_key(article: dict) -> str:
    return (
        (article.get("source") or article.get("source_domain") or "unknown")
        .strip()
        .lower()
    )


def _cluster_consensus_counters(signatures: list[dict]) -> dict:
    entity_counter: Counter[str] = Counter()
    keyword_counter: Counter[str] = Counter()
    anchor_counter: Counter[str] = Counter()
    for signature in signatures:
        entity_counter.update(signature.get("entities", set()))
        keyword_counter.update(signature.get("keywords", set()))
        anchor_counter.update(signature.get("anchors", set()))
    return {
        "entities": entity_counter,
        "keywords": keyword_counter,
        "anchors": anchor_counter,
    }


def _candidate_bias_penalty(text: str, signature: dict) -> float:
    lowered = text.lower()
    penalty = float(len(signature.get("framing_labels", set())) * 2.8)
    penalty += sum(
        1.2
        for term in SENSATIONAL_TITLE_TERMS
        if re.search(rf"\b{re.escape(term)}\b", lowered)
    )
    if "?" in text or "!" in text:
        penalty += 1.5
    if text.count('"') >= 2 or text.count("'") >= 2:
        penalty += 0.8
    return penalty


def _score_consensus_candidate(
    text: str,
    signature: dict,
    counters: dict,
    source_weight: float,
    *,
    kind: str,
    headline: str | None = None,
    headline_signature: dict | None = None,
) -> float:
    if not text:
        return -10_000.0

    entity_counter = counters["entities"]
    keyword_counter = counters["keywords"]
    anchor_counter = counters["anchors"]
    score = source_weight * 4.0
    score += sum(
        min(entity_counter.get(entity, 0), 4)
        for entity in signature.get("entities", set())
    )
    score += 0.32 * sum(
        max(keyword_counter.get(keyword, 0) - 1, 0)
        for keyword in signature.get("keywords", set())
    )
    score += 1.25 * sum(
        max(anchor_counter.get(anchor, 0) - 1, 0)
        for anchor in signature.get("anchors", set())
    )

    if kind == "title":
        if 28 <= len(text) <= 110:
            score += 3.0
        elif len(text) < 18:
            score -= 3.0
        elif len(text) > 130:
            score -= min((len(text) - 130) / 10.0, 6.0)
    else:
        if 70 <= len(text) <= 220:
            score += 3.5
        elif len(text) < 48:
            score -= 3.5
        elif len(text) > 240:
            score -= min((len(text) - 240) / 8.0, 8.0)
        if headline and text.lower() == headline.lower():
            score -= 8.0
        if headline_signature is not None:
            score += 2.0 * len(
                signature.get("entities", set())
                & headline_signature.get("entities", set())
            )
            score += 1.4 * len(
                signature.get("anchors", set())
                & headline_signature.get("anchors", set())
            )
            score += 0.24 * len(
                signature.get("keywords", set())
                & headline_signature.get("keywords", set())
            )

    score -= _candidate_bias_penalty(text, signature)
    return round(score, 3)


def _select_consensus_title(
    cluster: list[dict], counters: dict, source_profiles: dict, topic: str | None = None
) -> str:
    best_title = ""
    best_score = -10_000.0
    for article in cluster:
        title = normalize_article_title(article.get("title"))
        if not title:
            continue
        signature = _text_signature(title)
        profile = source_profiles.get(_source_key(article)) or _source_profile(
            article, topic=topic
        )
        score = _score_consensus_candidate(
            title, signature, counters, profile["quality_weight"], kind="title"
        )
        if score > best_score:
            best_title = title
            best_score = score
    return best_title or "Emerging event"


def _select_consensus_summary(
    cluster: list[dict],
    counters: dict,
    source_profiles: dict,
    headline: str,
    topic: str | None = None,
) -> str:
    best_summary = ""
    best_score = -10_000.0
    headline_signature = _text_signature(headline)
    for article in cluster:
        summary = normalize_article_description(
            article.get("description"), article.get("title"), limit=220
        )
        if not summary:
            continue
        signature = _text_signature(summary)
        profile = source_profiles.get(_source_key(article)) or _source_profile(
            article, topic=topic
        )
        score = _score_consensus_candidate(
            summary,
            signature,
            counters,
            profile["quality_weight"],
            kind="summary",
            headline=headline,
            headline_signature=headline_signature,
        )
        if score > best_score:
            best_summary = summary
            best_score = score
    if best_summary:
        return best_summary
    return (
        normalize_article_description(headline, headline, limit=220)
        or "No summary available."
    )


def _source_registry_maps() -> dict:
    global _source_registry_cache
    if _source_registry_cache is not None:
        return _source_registry_cache

    rows = get_source_registry(active_only=True)
    by_name = {}
    by_domain = {}
    for row in rows:
        if row.get("source_name"):
            by_name[row["source_name"].strip().lower()] = row
        if row.get("source_domain"):
            by_domain[row["source_domain"].strip().lower()] = row
    _source_registry_cache = {"by_name": by_name, "by_domain": by_domain}
    return _source_registry_cache


def _source_reliability_map(topic: str | None = None) -> dict:
    cache_key = topic or "__global__"
    now = datetime.now().timestamp()
    cache_age = now - _source_reliability_cache_time.get(cache_key, 0)
    if cache_key not in _source_reliability_cache or cache_age > 900:
        _source_reliability_cache[cache_key] = load_latest_source_reliability(
            topic=topic, max_age_hours=24 * 14
        )
        _source_reliability_cache_time[cache_key] = now
    return _source_reliability_cache[cache_key]


def _source_profile(article: dict, topic: str | None = None) -> dict:
    registry = _source_registry_maps()
    reliability = _source_reliability_map(topic=topic)
    source_name = (article.get("source") or "").strip().lower()
    domain = (article.get("source_domain") or "").strip().lower()
    matched = registry["by_name"].get(source_name) or registry["by_domain"].get(domain)
    reliability_row = reliability.get(source_name)
    if not matched:
        return {
            "source_type": "article",
            "trust_tier": "tier_2",
            "region": "global",
            "reliability_score": float(
                (reliability_row or {}).get("empirical_score", 0.5) or 0.5
            ),
            "quality_weight": round(
                0.95
                * float((reliability_row or {}).get("weight_multiplier", 1.0) or 1.0),
                2,
            ),
        }

    tier_weight = {
        "tier_1": 1.45,
        "tier_2": 1.1,
        "tier_3": 0.45,
    }.get(matched.get("trust_tier"), 1.0)
    type_weight = {
        "official_update": 1.4,
        "structured_event": 1.25,
        "article": 1.0,
        "monitored_channel": 0.35,
    }.get(matched.get("source_type"), 1.0)
    reliability_weight = float(
        (reliability_row or {}).get("weight_multiplier", 1.0) or 1.0
    )
    return {
        "source_type": matched.get("source_type"),
        "trust_tier": matched.get("trust_tier"),
        "region": matched.get("region") or "global",
        "reliability_score": float(
            (reliability_row or {}).get("empirical_score", 0.5) or 0.5
        ),
        "quality_weight": round(tier_weight * type_weight * reliability_weight, 2),
    }


def _dominant_region(region_counts: dict[str, int]) -> str:
    if not region_counts:
        return "global"

    non_global = {
        region: count
        for region, count in region_counts.items()
        if region and region not in {"global"}
    }
    candidate_pool = non_global or region_counts
    return sorted(candidate_pool.items(), key=lambda item: (-item[1], item[0]))[0][0]


def relatedness_score(left: dict, right: dict) -> float:
    entity_overlap = len(left["entities"] & right["entities"])
    keyword_overlap = len(left["keywords"] & right["keywords"])
    anchor_overlap = len(left["anchors"] & right["anchors"])
    entity_similarity = _soft_jaccard(left["entities"], right["entities"])
    keyword_similarity = _soft_jaccard(left["keywords"], right["keywords"])
    anchor_similarity = _soft_jaccard(left["anchors"], right["anchors"])
    hours_apart = _time_distance_hours(
        left.get("published_dt"), right.get("published_dt")
    )
    time_weight = _temporal_weight(hours_apart)

    score = (
        (entity_overlap * 1.9)
        + (keyword_overlap * 0.42)
        + (anchor_overlap * 1.65)
        + (entity_similarity * 2.0)
        + (keyword_similarity * 1.4)
        + (anchor_similarity * 1.5)
    ) * time_weight

    if _anchor_conflict(left["anchors"], right["anchors"]) and (
        hours_apart is None or hours_apart > 6
    ):
        score -= 2.6
    if hours_apart is not None and hours_apart > 168 and entity_overlap < 3:
        score -= 2.2
    if entity_overlap == 0 and anchor_overlap == 0:
        score -= 0.8
    return round(score, 3)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _cluster_cohesion_metrics(
    group: list[int], signatures: list[dict], articles: list[dict]
) -> dict:
    if len(group) <= 1:
        return {
            "pair_count": 0,
            "mean_relatedness": None,
            "median_relatedness": None,
            "min_relatedness": None,
            "max_relatedness": None,
            "outlier_article_count": 0,
            "outlier_ratio": 0.0,
            "outlier_article_urls": [],
        }

    pair_scores: list[float] = []
    per_article_scores: dict[int, list[float]] = {idx: [] for idx in group}
    for left_pos, left_idx in enumerate(group):
        for right_idx in group[left_pos + 1 :]:
            score = relatedness_score(signatures[left_idx], signatures[right_idx])
            pair_scores.append(score)
            per_article_scores[left_idx].append(score)
            per_article_scores[right_idx].append(score)

    if not pair_scores:
        return {
            "pair_count": 0,
            "mean_relatedness": None,
            "median_relatedness": None,
            "min_relatedness": None,
            "max_relatedness": None,
            "outlier_article_count": 0,
            "outlier_ratio": 0.0,
            "outlier_article_urls": [],
        }

    article_avg_scores: dict[int, float] = {}
    for idx, scores in per_article_scores.items():
        if not scores:
            continue
        article_avg_scores[idx] = sum(scores) / len(scores)

    outlier_indices = [
        idx
        for idx, avg_score in article_avg_scores.items()
        if avg_score < RELATEDNESS_THRESHOLD
    ]
    outlier_urls = [
        str((articles[idx] or {}).get("url") or "").strip()
        for idx in outlier_indices
        if str((articles[idx] or {}).get("url") or "").strip()
    ]

    return {
        "pair_count": len(pair_scores),
        "mean_relatedness": round(sum(pair_scores) / len(pair_scores), 4),
        "median_relatedness": round(_median(pair_scores) or 0.0, 4),
        "min_relatedness": round(min(pair_scores), 4),
        "max_relatedness": round(max(pair_scores), 4),
        "outlier_article_count": len(outlier_indices),
        "outlier_ratio": round(len(outlier_indices) / len(group), 4),
        "outlier_article_urls": outlier_urls,
    }


def is_related(left: dict, right: dict) -> bool:
    entity_overlap = len(left["entities"] & right["entities"])
    keyword_overlap = len(left["keywords"] & right["keywords"])
    anchor_overlap = len(left["anchors"] & right["anchors"])
    hours_apart = _time_distance_hours(
        left.get("published_dt"), right.get("published_dt")
    )
    score = relatedness_score(left, right)

    if (
        anchor_overlap >= 1
        and entity_overlap >= 1
        and (hours_apart is None or hours_apart <= 96)
    ):
        return True
    if (
        entity_overlap >= 2
        and keyword_overlap >= 2
        and (hours_apart is None or hours_apart <= 120)
    ):
        return True
    if (
        keyword_overlap >= 6
        and anchor_overlap >= 1
        and (hours_apart is None or hours_apart <= 48)
    ):
        return True
    if _anchor_conflict(left["anchors"], right["anchors"]) and entity_overlap < 3:
        return False
    return score >= RELATEDNESS_THRESHOLD


def build_observation_groups(signatures: list[dict]) -> list[list[int]]:
    groups: list[list[int]] = []
    for index, signature in enumerate(signatures):
        placed = False
        for group in groups:
            comparisons = [signatures[other] for other in group]
            related_count = sum(1 for other in comparisons if is_related(signature, other))
            best_score = max(
                (relatedness_score(signature, other) for other in comparisons),
                default=0.0,
            )
            if related_count >= 1 and (
                best_score >= RELATEDNESS_THRESHOLD
                or related_count >= math.ceil(len(group) / 2)
            ):
                group.append(index)
                placed = True
                break
        if not placed:
            groups.append([index])
    return groups


def cluster_articles(articles: list[dict], topic: str | None = None) -> list[dict]:
    if not articles:
        return []

    signatures = build_article_signatures(articles)
    groups = build_observation_groups(signatures)

    events = []
    for event_index, group in enumerate(groups, 1):
        cluster = [articles[i] for i in group]
        cohesion = _cluster_cohesion_metrics(group, signatures, articles)
        source_profiles = {}
        region_counts: dict[str, int] = {}
        for article in cluster:
            key = (
                article.get("source") or article.get("source_domain") or "unknown"
            ).strip().lower() or "unknown"
            if key not in source_profiles:
                source_profiles[key] = _source_profile(article, topic=topic)
            region = (source_profiles[key].get("region") or "global").strip().lower()
            region_counts[region] = region_counts.get(region, 0) + 1
        counters = _cluster_consensus_counters([signatures[i] for i in group])
        event_entities = {}
        for i in group:
            for entity in signatures[i]["entities"]:
                event_entities[entity] = event_entities.get(entity, 0) + 1

        ranked_entities = sorted(
            event_entities.items(),
            key=lambda item: (-item[1], item[0].lower()),
        )
        representative = sorted(
            cluster,
            key=lambda article: (
                -(
                    len(_normalize_text(article.get("description", "")))
                    + len(_normalize_text(article.get("title", "")))
                ),
                article.get("published_at", ""),
            ),
        )[0]
        consensus_title = _select_consensus_title(
            cluster, counters, source_profiles, topic=topic
        )
        consensus_summary = _select_consensus_summary(
            cluster, counters, source_profiles, consensus_title, topic=topic
        )

        events.append(
            {
                "event_id": f"{topic or 'global'}-{event_index}",
                "topic": topic,
                "label": consensus_title or representative.get("title", "Emerging event"),
                "summary": consensus_summary
                or representative.get("description", "")
                or "No summary available.",
                "entity_focus": [entity for entity, _ in ranked_entities[:6]],
                "source_count": len({article.get("source") for article in cluster}),
                "article_count": len(cluster),
                "latest_update": max(
                    (article.get("published_at", "") for article in cluster), default=""
                ),
                "earliest_update": min(
                    (article.get("published_at", "") for article in cluster), default=""
                ),
                "story_anchor_focus": sorted(
                    {
                        anchor
                        for i in group
                        for anchor in signatures[i].get("anchors", set())
                    }
                )[:4],
                "source_quality_score": round(
                    sum(profile["quality_weight"] for profile in source_profiles.values()),
                    2,
                ),
                "official_source_count": sum(
                    1
                    for profile in source_profiles.values()
                    if profile["source_type"] == "official_update"
                ),
                "structured_source_count": sum(
                    1
                    for profile in source_profiles.values()
                    if profile["source_type"] == "structured_event"
                ),
                "monitored_channel_count": sum(
                    1
                    for profile in source_profiles.values()
                    if profile["source_type"] == "monitored_channel"
                ),
                "tier_1_source_count": sum(
                    1
                    for profile in source_profiles.values()
                    if profile["trust_tier"] == "tier_1"
                ),
                "region_counts": region_counts,
                "dominant_region": _dominant_region(region_counts),
                "cluster_cohesion": cohesion,
                "articles": cluster,
            }
        )

    events.sort(
        key=lambda event: (
            -event["source_quality_score"],
            -event["tier_1_source_count"],
            -len(event.get("story_anchor_focus", [])),
            -event["source_count"],
            -event["article_count"],
            event["latest_update"],
        ),
        reverse=False,
    )
    return list(reversed(events))


def _event_key(event: dict) -> str:
    urls = sorted(
        article.get("url", "")
        for article in event.get("articles", [])
        if article.get("url")
    )
    material = " | ".join(
        [
            event.get("topic") or "global",
            event.get("label") or "",
            event.get("latest_update") or "",
            *urls,
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def event_cluster_key(event: dict) -> str:
    return _event_key(event)
