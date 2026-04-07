import os
import re
import math
import hashlib
from collections import Counter
from datetime import datetime
from pathlib import Path

import spacy
from anthropic import Anthropic
from dotenv import load_dotenv
from news import normalize_article_description, normalize_article_title
from corpus import (
    get_source_registry,
    load_contradiction_history,
    load_contradiction_record,
    load_latest_source_reliability,
    save_contradiction_record,
)

load_dotenv(Path(__file__).with_name(".env"))

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

CONTRADICTION_STATUS_DIMENSIONS = {
    "closure": {
        "positive": {"open", "reopened", "reopen", "operating", "active", "accessible"},
        "negative": {"closed", "closure", "shut", "blocked", "suspended", "halted"},
    },
    "ceasefire": {
        "positive": {
            "agreed",
            "agreement",
            "accept",
            "accepted",
            "holding",
            "observed",
        },
        "negative": {
            "reject",
            "rejected",
            "collapse",
            "collapsed",
            "violated",
            "resumed",
        },
    },
    "control": {
        "positive": {"captured", "secured", "retook", "controls", "seized"},
        "negative": {"lost", "withdrawal", "withdrew", "abandoned", "evacuated"},
    },
    "detention": {
        "positive": {"released", "freed", "acquitted"},
        "negative": {"arrested", "detained", "jailed", "charged", "indicted"},
    },
    "sanctions": {
        "positive": {"lifted", "waived", "eased"},
        "negative": {"imposed", "tightened", "expanded", "blacklisted", "designated"},
    },
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
_client = None
_client_unavailable_until = 0.0
_source_registry_cache = None
_source_reliability_cache = {}
_source_reliability_cache_time = {}


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


def get_client():
    global _client, _client_unavailable_until
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    if (
        _client_unavailable_until
        and _client_unavailable_until > datetime.now().timestamp()
    ):
        return None
    if _client is None:
        try:
            _client = Anthropic(api_key=api_key)
        except Exception:
            _client_unavailable_until = datetime.now().timestamp() + 300
            return None
    return _client


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


def _keyword_snippet(text: str, limit: int = 18) -> str:
    lowered = _normalize_text(text).lower()
    tokens = [
        token
        for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9\-']*", lowered)
        if token not in STOPWORDS
    ]
    return " ".join(tokens[:limit])


def _article_numbers(text: str) -> list[str]:
    return re.findall(r"\b\d[\d,]*(?:\.\d+)?\b", text or "")


def _article_status_markers(text: str) -> dict[str, str]:
    lowered = (text or "").lower()
    markers = {}
    for dimension, poles in CONTRADICTION_STATUS_DIMENSIONS.items():
        positive = any(token in lowered for token in poles["positive"])
        negative = any(token in lowered for token in poles["negative"])
        if positive and not negative:
            markers[dimension] = "positive"
        elif negative and not positive:
            markers[dimension] = "negative"
    return markers


def _article_framing_labels(text: str) -> set[str]:
    lowered = (text or "").lower()
    labels = set()
    for label, variants in FRAMING_LABELS.items():
        if any(variant in lowered for variant in variants):
            labels.add(label)
    return labels


def _article_claim_features(article: dict) -> dict:
    text = _normalize_text(
        f"{article.get('title', '')}. {article.get('description', '')}"
    )
    entities = _article_entities(text)
    keywords = _token_keywords(text)
    return {
        "text": text,
        "entities": entities,
        "keywords": keywords,
        "numbers": _article_numbers(text),
        "status_markers": _article_status_markers(text),
        "framing_labels": _article_framing_labels(text),
        "snippet": _keyword_snippet(text),
    }


def _claims_share_context(left: dict, right: dict) -> bool:
    entity_overlap = len(left["entities"] & right["entities"])
    keyword_overlap = len(left["keywords"] & right["keywords"])
    return entity_overlap >= 1 or keyword_overlap >= 4


def _normalize_numeric_token(token: str) -> str:
    return token.replace(",", "")


def _resolve_article_record(article: dict) -> dict:
    return {
        "source": article.get("source") or "Unknown source",
        "title": article.get("title"),
        "url": article.get("url"),
        "published_at": article.get("published_at"),
    }


def _dedupe_contradictions(items: list[dict]) -> list[dict]:
    seen = set()
    deduped = []
    for item in items:
        key = (
            item.get("conflict_type"),
            tuple(
                sorted(
                    label.lower()
                    for label in item.get("sources_in_conflict", [])
                    if label
                )
            ),
            item.get("claim_a", "").strip().lower(),
            item.get("claim_b", "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def heuristic_contradictions(event: dict, max_items: int = 6) -> list[dict]:
    articles = event.get("articles", [])
    if len(articles) < 2:
        return []

    contradictions = []
    feature_cache = [_article_claim_features(article) for article in articles]

    for left_index in range(len(articles)):
        for right_index in range(left_index + 1, len(articles)):
            left = articles[left_index]
            right = articles[right_index]
            left_features = feature_cache[left_index]
            right_features = feature_cache[right_index]
            if not _claims_share_context(left_features, right_features):
                continue

            shared_entities = sorted(
                left_features["entities"] & right_features["entities"]
            )
            focus = (
                ", ".join(shared_entities[:3]) if shared_entities else "the same event"
            )

            left_numbers = {
                _normalize_numeric_token(token) for token in left_features["numbers"]
            }
            right_numbers = {
                _normalize_numeric_token(token) for token in right_features["numbers"]
            }
            divergent_numbers = sorted((left_numbers ^ right_numbers) - {"0", "1"})
            if (
                left_numbers
                and right_numbers
                and left_numbers != right_numbers
                and divergent_numbers
            ):
                contradictions.append(
                    {
                        "conflict_type": "scale",
                        "sources_in_conflict": [
                            left.get("source"),
                            right.get("source"),
                        ],
                        "source_records": [
                            _resolve_article_record(left),
                            _resolve_article_record(right),
                        ],
                        "claim_a": f"{left.get('source')}: {left_features['snippet']}",
                        "claim_b": f"{right.get('source')}: {right_features['snippet']}",
                        "confidence": 0.78,
                        "most_credible_source": "unresolved",
                        "most_credible_record": None,
                        "reasoning": f"These reports describe {focus} with different key numerical details.",
                    }
                )

            for dimension in sorted(
                set(left_features["status_markers"])
                & set(right_features["status_markers"])
            ):
                if (
                    left_features["status_markers"][dimension]
                    == right_features["status_markers"][dimension]
                ):
                    continue
                contradictions.append(
                    {
                        "conflict_type": (
                            "timeline"
                            if dimension in {"closure", "ceasefire", "detention"}
                            else "fact"
                        ),
                        "sources_in_conflict": [
                            left.get("source"),
                            right.get("source"),
                        ],
                        "source_records": [
                            _resolve_article_record(left),
                            _resolve_article_record(right),
                        ],
                        "claim_a": f"{left.get('source')}: {left.get('title')}",
                        "claim_b": f"{right.get('source')}: {right.get('title')}",
                        "confidence": 0.77,
                        "most_credible_source": "unresolved",
                        "most_credible_record": None,
                        "reasoning": f"These reports frame {focus} with opposite {dimension} status signals.",
                    }
                )

            left_labels = left_features["framing_labels"]
            right_labels = right_features["framing_labels"]
            framing_delta = left_labels ^ right_labels
            if (
                left_labels
                and right_labels
                and framing_delta
                and left_labels != right_labels
            ):
                contradictions.append(
                    {
                        "conflict_type": "intent",
                        "sources_in_conflict": [
                            left.get("source"),
                            right.get("source"),
                        ],
                        "source_records": [
                            _resolve_article_record(left),
                            _resolve_article_record(right),
                        ],
                        "claim_a": f"{left.get('source')}: {left.get('title')}",
                        "claim_b": f"{right.get('source')}: {right.get('title')}",
                        "confidence": 0.65,
                        "most_credible_source": "unresolved",
                        "most_credible_record": None,
                        "reasoning": f"These sources use different framing labels for {focus}, suggesting a narrative fracture rather than consensus language.",
                    }
                )

    ranked = sorted(
        _dedupe_contradictions(contradictions),
        key=lambda item: (
            item.get("confidence", 0),
            item.get("conflict_type") != "scale",
        ),
        reverse=True,
    )
    return ranked[:max_items]


def detect_narrative_fractures(event: dict, max_items: int = 4) -> list[dict]:
    articles = event.get("articles", [])
    if len(articles) < 2:
        return []

    feature_cache = [_article_claim_features(article) for article in articles]
    label_sources: dict[str, list[dict]] = {}
    for article, features in zip(articles, feature_cache):
        for label in features["framing_labels"]:
            label_sources.setdefault(label, []).append(article)

    if len(label_sources) < 2:
        return []

    ranked_labels = sorted(
        label_sources.items(),
        key=lambda item: len({(row.get("source") or "").lower() for row in item[1]}),
        reverse=True,
    )

    fractures = []
    for left_index in range(len(ranked_labels)):
        for right_index in range(left_index + 1, len(ranked_labels)):
            left_label, left_articles = ranked_labels[left_index]
            right_label, right_articles = ranked_labels[right_index]
            left_sources = {
                (row.get("source") or "").lower()
                for row in left_articles
                if row.get("source")
            }
            right_sources = {
                (row.get("source") or "").lower()
                for row in right_articles
                if row.get("source")
            }
            if not left_sources or not right_sources or left_sources == right_sources:
                continue

            left_sample = left_articles[0]
            right_sample = right_articles[0]
            shared_entities = _article_claim_features(left_sample).get(
                "entities", set()
            ) & _article_claim_features(right_sample).get("entities", set())
            focus = (
                ", ".join(sorted(shared_entities)[:3])
                if shared_entities
                else (event.get("entity_focus") or ["this event"])[0]
            )
            fractures.append(
                {
                    "fracture_type": "framing",
                    "dimension": "actor_labeling",
                    "label_a": left_label,
                    "label_b": right_label,
                    "sources_a": [row.get("source") for row in left_articles[:3]],
                    "sources_b": [row.get("source") for row in right_articles[:3]],
                    "source_records_a": [
                        _resolve_article_record(row) for row in left_articles[:3]
                    ],
                    "source_records_b": [
                        _resolve_article_record(row) for row in right_articles[:3]
                    ],
                    "reasoning": f"Sources are labeling {focus} differently, using '{left_label}' versus '{right_label}' framing for the same event cluster.",
                    "confidence": 0.64,
                }
            )

    deduped = []
    seen = set()
    for item in fractures:
        key = tuple(sorted([item["label_a"], item["label_b"]]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped[:max_items]


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


def _relatedness_score(left: dict, right: dict) -> float:
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


def _is_related(left: dict, right: dict) -> bool:
    entity_overlap = len(left["entities"] & right["entities"])
    keyword_overlap = len(left["keywords"] & right["keywords"])
    anchor_overlap = len(left["anchors"] & right["anchors"])
    hours_apart = _time_distance_hours(
        left.get("published_dt"), right.get("published_dt")
    )
    score = _relatedness_score(left, right)

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
    return score >= 4.1


def cluster_articles(articles: list[dict], topic: str | None = None) -> list[dict]:
    if not articles:
        return []

    signatures = [_article_signature(article) for article in articles]
    groups: list[list[int]] = []

    for index, signature in enumerate(signatures):
        placed = False
        for group in groups:
            comparisons = [signatures[other] for other in group]
            related_count = sum(
                1 for other in comparisons if _is_related(signature, other)
            )
            best_score = max(
                (_relatedness_score(signature, other) for other in comparisons),
                default=0.0,
            )
            if related_count >= 1 and (
                best_score >= 4.1 or related_count >= math.ceil(len(group) / 2)
            ):
                group.append(index)
                placed = True
                break
        if not placed:
            groups.append([index])

    events = []
    for event_index, group in enumerate(groups, 1):
        cluster = [articles[i] for i in group]
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
                "label": consensus_title
                or representative.get("title", "Emerging event"),
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
                    sum(
                        profile["quality_weight"]
                        for profile in source_profiles.values()
                    ),
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


def _build_contradiction_prompt(event: dict) -> str:
    article_lines = []
    for index, article in enumerate(event["articles"], 1):
        article_lines.append(f"""Source Record {index}
Source: {article['source']}
Published: {article.get('published_at', 'Unknown')}
Title: {article['title']}
URL: {article.get('url', 'Unknown')}
Summary: {article.get('description', 'No description')}""")

    joined = "\n\n".join(article_lines)
    return f"""You are comparing reporting on the same geopolitical event.

Event label: {event['label']}
Key entities: {", ".join(event.get('entity_focus', []))}

{joined}

Return ONLY valid JSON:
{{
  "contradictions": [
    {{
      "conflict_type": "fact|timeline|causality|scale|intent",
      "sources_in_conflict": ["Exact source name", "Exact source name"],
      "claim_a": "Short quoted or paraphrased claim",
      "claim_b": "Short quoted or paraphrased claim",
      "confidence": 0.0,
      "most_credible_source": "Source name or unresolved",
      "reasoning": "One tight sentence"
    }}
  ]
}}

Rules:
- Use the exact source names from the Source field, not placeholders
- Never refer to outlets as "Article 1", "Article 2", "Record 3", or similar
- If you mention a source in reasoning, use its source name

If there are no meaningful contradictions, return {{"contradictions":[]}}."""


def _article_reference_catalog(event: dict) -> dict:
    by_index = {}
    by_source = {}
    for index, article in enumerate(event.get("articles", []), 1):
        by_index[index] = article
        source_key = (article.get("source") or "").strip().lower()
        if source_key and source_key not in by_source:
            by_source[source_key] = article
    return {"by_index": by_index, "by_source": by_source}


def _replace_article_refs(text: str, catalog: dict) -> str:
    if not text:
        return ""

    def repl(match):
        article = catalog["by_index"].get(int(match.group(1)))
        if not article:
            return match.group(0)
        return article.get("source") or match.group(0)

    return re.sub(
        r"\b(?:Article|Record|Source Record)\s+(\d+)\b", repl, text, flags=re.IGNORECASE
    )


def _resolve_source_record(label: str, catalog: dict) -> dict | None:
    if not label:
        return None
    match = re.search(
        r"\b(?:Article|Record|Source Record)\s+(\d+)\b", label, flags=re.IGNORECASE
    )
    if match:
        return catalog["by_index"].get(int(match.group(1)))
    return catalog["by_source"].get(label.strip().lower())


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
    """Stable id for a narrative cluster (matches contradiction / cache keys)."""
    return _event_key(event)


def contradiction_history_for_event(event: dict, limit: int = 10) -> list[dict]:
    return load_contradiction_history(_event_key(event), limit=limit)


def detect_contradictions(event: dict) -> list[dict]:
    if len(event.get("articles", [])) < 2:
        return []

    heuristic = heuristic_contradictions(event)
    client = get_client()
    if client is None:
        return heuristic

    prompt = _build_contradiction_prompt(event)

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}],
        )
        text = (
            message.content[0]
            .text.strip()
            .replace("```json", "")
            .replace("```", "")
            .strip()
        )
        import json

        payload = json.loads(text)
        contradictions = payload.get("contradictions", [])
        catalog = _article_reference_catalog(event)
        normalized = []
        for contradiction in contradictions:
            source_labels = [
                _replace_article_refs(label, catalog)
                for label in contradiction.get("sources_in_conflict", [])
            ]
            source_records = []
            for label in source_labels[:2]:
                article = _resolve_source_record(label, catalog)
                source_records.append(
                    {
                        "source": label or "Unknown source",
                        "title": article.get("title") if article else None,
                        "url": article.get("url") if article else None,
                        "published_at": (
                            article.get("published_at") if article else None
                        ),
                    }
                )

            most_credible_source = _replace_article_refs(
                contradiction.get("most_credible_source", "unresolved"), catalog
            )
            most_credible_article = _resolve_source_record(
                most_credible_source, catalog
            )
            normalized.append(
                {
                    "conflict_type": contradiction.get("conflict_type", "fact"),
                    "sources_in_conflict": source_labels,
                    "source_records": source_records,
                    "claim_a": _replace_article_refs(
                        contradiction.get("claim_a", ""), catalog
                    ),
                    "claim_b": _replace_article_refs(
                        contradiction.get("claim_b", ""), catalog
                    ),
                    "confidence": float(contradiction.get("confidence", 0) or 0),
                    "most_credible_source": most_credible_source or "unresolved",
                    "most_credible_record": (
                        {
                            "source": most_credible_source,
                            "title": (
                                most_credible_article.get("title")
                                if most_credible_article
                                else None
                            ),
                            "url": (
                                most_credible_article.get("url")
                                if most_credible_article
                                else None
                            ),
                            "published_at": (
                                most_credible_article.get("published_at")
                                if most_credible_article
                                else None
                            ),
                        }
                        if most_credible_article
                        else None
                    ),
                    "reasoning": _replace_article_refs(
                        contradiction.get("reasoning", ""), catalog
                    ),
                }
            )
        merged = _dedupe_contradictions(normalized + heuristic)
        return merged[:8]
    except Exception as exc:
        global _client, _client_unavailable_until
        _client = None
        _client_unavailable_until = datetime.now().timestamp() + 300
        print(f"[contradictions] Error analyzing {event.get('event_id')}: {exc}")
        return heuristic


def enrich_events(events: list[dict]) -> list[dict]:
    enriched = []
    for event in events:
        event_key = _event_key(event)
        cached = load_contradiction_record(event_key)
        if cached is not None:
            contradictions = cached.get("contradictions", [])
        else:
            contradictions = detect_contradictions(event)
            save_contradiction_record(event_key, event, contradictions)
        narrative_fractures = detect_narrative_fractures(event)
        enriched.append(
            {
                **event,
                "event_key": event_key,
                "contradictions": contradictions,
                "contradiction_count": len(contradictions),
                "narrative_fractures": narrative_fractures,
                "narrative_fracture_count": len(narrative_fractures),
                "analysis_priority": round(
                    (event["source_quality_score"] * 2.0)
                    + (event["tier_1_source_count"] * 1.8)
                    + (event["official_source_count"] * 1.6)
                    + (event["structured_source_count"] * 1.2)
                    - (event["monitored_channel_count"] * 1.4)
                    + (event["source_count"] * 1.2)
                    + (event["article_count"] * 1.2)
                    + (len(contradictions) * 2.4)
                    + (len(narrative_fractures) * 1.7),
                    2,
                ),
            }
        )
    enriched.sort(key=lambda event: event["analysis_priority"], reverse=True)
    return enriched


def format_event_brief(events: list[dict]) -> str:
    if not events:
        return ""

    lines = ["EVENT RADAR (clustered coverage patterns):"]
    for event in events[:5]:
        latest = event.get("latest_update", "")
        date_label = latest[:10] if latest else "Unknown date"
        lines.append(
            f"- {event['label']} [{date_label}] — {event['source_count']} sources, "
            f"{event['article_count']} articles, focus: {', '.join(event.get('entity_focus', [])[:4]) or 'broad coverage'}"
        )
        if event.get("contradictions"):
            top = event["contradictions"][0]
            lines.append(
                f"  Contradiction: {top['conflict_type']} dispute between "
                f"{', '.join(top.get('sources_in_conflict', [])[:2]) or 'multiple sources'}; "
                f"{top.get('reasoning', 'competing accounts still unresolved')}"
            )
    return "\n".join(lines)


def format_contradictions_for_briefing(events: list[dict]) -> str:
    contradiction_events = [event for event in events if event.get("contradictions")]
    if not contradiction_events:
        return ""

    lines = ["SOURCE CONTRADICTIONS DATA:"]
    for event in contradiction_events[:4]:
        lines.append(f"- Event: {event['label']}")
        for contradiction in event["contradictions"][:2]:
            lines.append(
                f"  - {contradiction['conflict_type']}: "
                f"{' vs '.join(contradiction.get('sources_in_conflict', [])[:2]) or 'multiple sources'}"
            )
            lines.append(f"    Claim A: {contradiction.get('claim_a', '')}")
            lines.append(f"    Claim B: {contradiction.get('claim_b', '')}")
            lines.append(f"    Assessment: {contradiction.get('reasoning', '')}")
    return "\n".join(lines)


# Backward-compatibility wrappers: clustering now lives in clustering.py.
from clustering import cluster_articles as _cluster_articles_impl
from clustering import event_cluster_key as _event_cluster_key_impl


def cluster_articles(articles: list[dict], topic: str | None = None) -> list[dict]:
    return _cluster_articles_impl(articles, topic=topic)


def event_cluster_key(event: dict) -> str:
    return _event_cluster_key_impl(event)
