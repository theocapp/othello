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
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

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
_semantic_model = None
_source_registry_cache = None
_source_reliability_cache = {}
_source_reliability_cache_time = {}

RELATEDNESS_THRESHOLD = 0.380  # Cosine similarity threshold for semantic relatedness

# Auto-tunable clustering parameters (used by continuous improvement loop).
GEO_MISMATCH_PENALTY = 0.500
TOPICAL_BASE_PENALTY = 0.780
TOPICAL_LOW_OVERLAP_MULTIPLIER = 0.92
CONTEXT_PENALTY_FLOOR = 0.95
ANALYSIS_ABSTRACTION_PENALTY = 0.450
MULTI_THEATER_STRIKE_PENALTY = 0.350

HORMUZ_BRIDGE_BOOST = 0.24
LONG_CONFLICT_CONTINUITY_BOOST = 0.06
LONG_ACTOR_ANCHOR_STRONG_KW_BOOST = 0.24
LONG_WAR_CONTEXT_BOOST = 0.080

_CONTEXT_TERMS = {
    "iran",
    "war",
    "hormuz",
    "ceasefire",
    "trump",
    "oil",
    "talk",
    "talks",
    "diplomatic",
    "diplomacy",
}

_CONSEQUENCE_TERMS = {
    "market",
    "markets",
    "stocks",
    "shares",
    "energy",
    "supplies",
    "supply",
    "economy",
    "economic",
    "prices",
    "inflation",
    "barrel",
    "oil",
}

_LABOR_TERMS = {
    "teachers",
    "teacher",
    "workers",
    "worker",
    "union",
    "wage",
    "pension",
    "schools",
    "school",
    "walkout",
    "protest",
}

_MILITARY_TERMS = {
    "military",
    "forces",
    "aircraft",
    "missile",
    "drone",
    "militant",
    "targets",
    "defense",
    "defence",
    "fighters",
    "artillery",
}

_ANALYSIS_TERMS = {
    "analysis",
    "takeaway",
    "takeaways",
    "overview",
    "explainer",
    "interview",
    "opinion",
}

_BROAD_CONFLICT_GPE_TERMS = {
    "Iran",
    "Israel",
    "US",
    "United States",
    "Tehran",
    "Washington",
}


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


def get_semantic_model():
    """Load the sentence transformer model for semantic similarity."""
    global _semantic_model
    if _semantic_model is None:
        _semantic_model = SentenceTransformer("all-mpnet-base-v2")
    return _semantic_model


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


def _article_entity_breakdown(text: str) -> tuple[set[str], set[str]]:
    doc = get_nlp()(text[:700])
    gpe_entities = {
        ent.text.strip() for ent in doc.ents if ent.label_ == "GPE" and ent.text.strip()
    }
    actor_entities = {
        ent.text.strip()
        for ent in doc.ents
        if ent.label_ in {"PERSON", "ORG", "NORP", "EVENT"} and ent.text.strip()
    }
    return gpe_entities, actor_entities


def _article_framing_labels(text: str) -> set[str]:
    lowered = (text or "").lower()
    labels = set()
    for label, variants in FRAMING_LABELS.items():
        if any(variant in lowered for variant in variants):
            labels.add(label)
    return labels


def _article_signature(article: dict) -> dict:
    title = _normalize_text(article.get('title', ''))
    description = _normalize_text(article.get('description', ''))
    lead = (description or "").split(".")[0].strip()
    text = f"{title}. {lead}" if lead else title
    entities = _article_entities(text)
    gpe_entities, actor_entities = _article_entity_breakdown(text)
    keywords = _token_keywords(text)
    anchors = _extract_event_anchors(keywords, text)
    published_dt = _parse_published_at(article.get("published_at"))
    return {
        "text": text,
        "entities": entities,
        "gpe_entities": gpe_entities,
        "actor_entities": actor_entities,
        "keywords": keywords,
        "anchors": anchors,
        "published_at": article.get("published_at"),
        "published_dt": published_dt,
    }


def _context_term_overlap(left_text: str, right_text: str) -> set[str]:
    left_lower = left_text.lower()
    right_lower = right_text.lower()
    return {
        term
        for term in _CONTEXT_TERMS
        if re.search(rf"\b{re.escape(term)}\b", left_lower)
        and re.search(rf"\b{re.escape(term)}\b", right_lower)
    }


def _term_overlap(left_text: str, right_text: str, terms: set[str]) -> set[str]:
    left_lower = left_text.lower()
    right_lower = right_text.lower()
    return {
        term
        for term in terms
        if re.search(rf"\b{re.escape(term)}\b", left_lower)
        and re.search(rf"\b{re.escape(term)}\b", right_lower)
    }


def _has_any_term(text: str, terms: set[str]) -> bool:
    lowered = text.lower()
    return any(re.search(rf"\b{re.escape(term)}\b", lowered) for term in terms)


def build_article_signatures(articles: list[dict]) -> list[dict]:
    signatures = [_article_signature(article) for article in articles]

    # Batch-encode all article texts in one model call to avoid O(n²) encoding.
    model = get_semantic_model()
    texts = [sig.get("text") or "" for sig in signatures]
    if texts:
        all_embeddings = model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
        for sig, embedding in zip(signatures, all_embeddings):
            sig["_embedding"] = embedding

    return signatures


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
    """Calculate semantic relatedness using fine-tuned sentence embeddings.
    
    Score range: [0, 1] from cosine similarity, then scaled down by temporal weight.
    Penalty: Articles mentioning different geographic entities (different cities)
    get a lower score, requiring stronger semantic similarity to cluster.
    Threshold: 0.40 (after temporal decay).
    """
    left_embedding = left.get("_embedding")
    right_embedding = right.get("_embedding")

    if left_embedding is None or right_embedding is None:
        model = get_semantic_model()
        left_text = left.get("text", "")
        right_text = right.get("text", "")
        if not left_text or not right_text:
            return 0.0
        embeddings = model.encode([left_text, right_text], convert_to_numpy=True)
        left_embedding = embeddings[0]
        right_embedding = embeddings[1]
    
    # Compute cosine similarity (returns value between -1 and 1, typically 0-1 for text)
    semantic_score = float(cosine_similarity(
        left_embedding.reshape(1, -1),
        right_embedding.reshape(1, -1)
    )[0][0])
    
    # Get temporal weight
    hours_apart = _time_distance_hours(
        left.get("published_dt"), right.get("published_dt")
    )
    time_weight = _temporal_weight(hours_apart)
    
    # Check for geographic separation penalty
    # If both articles mention GPE (geographic/political entity) but they don't overlap,
    # apply a penalty to require stronger semantic similarity
    left_entities = left.get("entities", set())
    right_entities = right.get("entities", set())
    
    # Use explicit GPE entities for location checks to avoid NORP/actor bleed.
    left_gpes = left.get("gpe_entities", set())
    right_gpes = right.get("gpe_entities", set())
    
    # If both articles mention locations but they don't overlap, apply penalty
    geo_penalty = 1.0
    if left_gpes and right_gpes and not (left_gpes & right_gpes):
        # They mention locations but different ones (e.g., Cairo vs Alexandria)
        # Reduce the semantic score by 30% to require stronger semantic match
        geo_penalty = GEO_MISMATCH_PENALTY

    entity_overlap = left_entities & right_entities
    actor_overlap = left.get("actor_entities", set()) & right.get("actor_entities", set())
    anchor_overlap = left.get("anchors", set()) & right.get("anchors", set())
    keyword_overlap = left.get("keywords", set()) & right.get("keywords", set())

    # Penalize broad country-level topical overlap without shared actors.
    # This reduces false merges among "Iran-adjacent" but event-distinct stories.
    topical_bleed_penalty = 1.0
    if entity_overlap and not actor_overlap and len(entity_overlap) <= 2:
        topical_bleed_penalty = TOPICAL_BASE_PENALTY
    if len(entity_overlap) <= 1 and not anchor_overlap and len(keyword_overlap) < 3:
        topical_bleed_penalty *= TOPICAL_LOW_OVERLAP_MULTIPLIER

    context_overlap = _context_term_overlap(left_text, right_text)
    consequence_overlap = _term_overlap(left_text, right_text, _CONSEQUENCE_TERMS)
    ongoing_context_boost = 0.0
    # Recover low-overlap same-situation updates by boosting shared conflict context.
    if (
        hours_apart is not None
        and hours_apart <= 72
        and len(context_overlap) >= 2
        and ({"iran", "war"} <= context_overlap or "hormuz" in context_overlap)
        and not anchor_overlap
        and len(keyword_overlap) <= 8
    ):
        pass  # BASE_CONTEXT_BOOST removed

    # When strong shared context exists, reduce topical-bleed penalty severity.
    if hours_apart is not None and hours_apart <= 96 and len(context_overlap) >= 2:
        topical_bleed_penalty = max(topical_bleed_penalty, CONTEXT_PENALTY_FLOOR)

    # Penalize broad macro-consequence coverage that shares war context but not incident actors.
    if (
        len(consequence_overlap) >= 2
        and not actor_overlap
        and not anchor_overlap
        and ({"iran", "war"} <= context_overlap or {"iran", "oil"} <= context_overlap)
    ):
        pass  # CONSEQUENCE_CONTEXT_PENALTY removed

    # Separate broad downstream consequence stories across different countries.
    # Rule is conflict-agnostic: any single shared GPE with different spillover locations
    # (e.g., Iran spillover to India vs UAE, Sudan spillover to Ethiopia vs Kenya, etc.)
    gpe_intersection = left_gpes & right_gpes
    if len(gpe_intersection) == 1:
        # Exactly one shared conflict entity
        common_gpe = list(gpe_intersection)[0]
        left_extra_gpes = left_gpes - {common_gpe}
        right_extra_gpes = right_gpes - {common_gpe}
        
        # Consequence-type anchors (market, policy) shouldn't block spillover penalty
        consequence_anchors = {"market", "policy"}
        strong_anchor_overlap = anchor_overlap - consequence_anchors
        
        if (
            not actor_overlap
            and not strong_anchor_overlap
            and left_extra_gpes
            and right_extra_gpes
            and len(keyword_overlap) <= 3
        ):
            pass  # CONSEQUENCE_IRAN_ONLY_PENALTY removed

    left_labor = _term_overlap(left_text, left_text, _LABOR_TERMS)
    right_labor = _term_overlap(right_text, right_text, _LABOR_TERMS)
    left_military = _term_overlap(left_text, left_text, _MILITARY_TERMS)
    right_military = _term_overlap(right_text, right_text, _MILITARY_TERMS)

    # Separate labor strike coverage from military strike coverage.
    if (left_labor and right_military) or (right_labor and left_military):
        pass  # LABOR_MILITARY_PENALTY removed

    gpe_overlap = left_gpes & right_gpes
    # Same actor + strike language in different locations is often separate incidents.
    if anchor_overlap and actor_overlap and not gpe_overlap:
        pass  # ACTOR_STRIKE_DIFF_GPE_PENALTY removed

    # Distinct strike incidents across different theaters should not merge
    # just because they share broad conflict entities.
    left_specific_gpes = left_gpes - _BROAD_CONFLICT_GPE_TERMS
    right_specific_gpes = right_gpes - _BROAD_CONFLICT_GPE_TERMS
    if (
        "strike" in left.get("anchors", set())
        and "strike" in right.get("anchors", set())
        and left_specific_gpes
        and right_specific_gpes
        and not (left_specific_gpes & right_specific_gpes)
    ):
        topical_bleed_penalty *= MULTI_THEATER_STRIKE_PENALTY

    abstraction_mismatch = False
    left_analysis = _has_any_term(left_text, _ANALYSIS_TERMS)
    right_analysis = _has_any_term(right_text, _ANALYSIS_TERMS)
    left_incident_anchors = bool(left.get("anchors", set()) & {"strike", "detention", "filing", "aid", "vote"})
    right_incident_anchors = bool(right.get("anchors", set()) & {"strike", "detention", "filing", "aid", "vote"})
    # Avoid merging broad analysis/takeaway framing with incident-driven updates.
    if (
        left_analysis ^ right_analysis
        and not actor_overlap
        and (left_incident_anchors ^ right_incident_anchors)
        and len(gpe_overlap) <= 1
    ):
        topical_bleed_penalty *= ANALYSIS_ABSTRACTION_PENALTY
        abstraction_mismatch = True

    # Bridge terse rolling updates that mention Hormuz in one report and
    # Iran-war framing in another report from the same short time window.
    left_lower = left_text.lower()
    right_lower = right_text.lower()
    if hours_apart is not None and hours_apart <= 72:
        left_hormuz = bool(re.search(r"\bhormuz\b", left_lower))
        right_hormuz = bool(re.search(r"\bhormuz\b", right_lower))
        left_conflict = bool(re.search(r"\biran\b", left_lower)) and bool(
            re.search(r"\b(war|oil|trump|ceasefire|talks?)\b", left_lower)
        )
        right_conflict = bool(re.search(r"\biran\b", right_lower)) and bool(
            re.search(r"\b(war|oil|trump|ceasefire|talks?)\b", right_lower)
        )
        if (left_hormuz and right_conflict) or (right_hormuz and left_conflict):
            ongoing_context_boost = max(ongoing_context_boost, HORMUZ_BRIDGE_BOOST)

    # Keep long-running conflicts grouped when lexical continuity is strong.
    if (
        hours_apart is not None
        and 72 <= hours_apart <= 144
        and len(keyword_overlap) >= 3
        and len(entity_overlap) >= 1
    ):
        ongoing_context_boost = max(ongoing_context_boost, LONG_CONFLICT_CONTINUITY_BOOST)

    # Recover same-situation updates when shared actor/entity continuity is present.
    if hours_apart is not None and hours_apart <= 72 and actor_overlap:
        pass  # SHORT_ACTOR_CONTINUITY_BOOST removed
    if (
        hours_apart is not None
        and hours_apart <= 192
        and actor_overlap
        and anchor_overlap
    ):
        pass  # LONG_ACTOR_ANCHOR_BOOST removed
    if (
        hours_apart is not None
        and hours_apart <= 192
        and actor_overlap
        and anchor_overlap
        and len(keyword_overlap) >= 4
    ):
        ongoing_context_boost = max(ongoing_context_boost, LONG_ACTOR_ANCHOR_STRONG_KW_BOOST)

    # Recover long-horizon same-war updates that share high-level conflict context.
    if (
        hours_apart is not None
        and 72 < hours_apart <= 192
        and len(entity_overlap) >= 1
        and ({"iran", "war"} <= context_overlap or {"iran", "oil"} <= context_overlap)
    ):
        ongoing_context_boost = max(ongoing_context_boost, LONG_WAR_CONTEXT_BOOST)

    # Conflict-level continuity boosts should not override clear abstraction mismatch.
    if abstraction_mismatch:
        ongoing_context_boost = 0.0

    # Combine semantic similarity with temporal weight and geographic penalty
    final_score = (semantic_score * time_weight * geo_penalty * topical_bleed_penalty) + ongoing_context_boost
    final_score = min(1.0, max(0.0, final_score))
    
    return round(final_score, 3)


def _is_likely_location(entity: str) -> bool:
    """Check if entity text looks like a geographic location.
    
    Simple heuristic: capitalized multi-word phrases, or known city/country patterns.
    """
    if not entity:
        return False
    
    # Capitalized words (heuristic for proper nouns, especially places)
    if len(entity) > 1 and entity[0].isupper():
        return True
    
    return False


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
    score = relatedness_score(left, right)
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
