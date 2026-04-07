"""Canonical event identity resolution.

The system currently produces volatile observation keys from clustering (see
`contradictions.event_cluster_key`). Those keys change as coverage evolves.

This module provides a conservative resolver that maps each observation key onto
an existing canonical event_id when there is strong evidence that it's the same
real-world event; otherwise it treats the observation as a new canonical event.

v1 intentionally prefers false-splits over false-merges.
"""

from __future__ import annotations

import re
import uuid
from typing import Iterable


_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "to",
    "of",
    "in",
    "on",
    "for",
    "with",
    "as",
    "by",
    "from",
    "at",
    "after",
    "before",
    "over",
    "under",
    "amid",
    "into",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "it",
    "its",
    "their",
    "his",
    "her",
    "they",
    "them",
    "this",
    "that",
    "these",
    "those",
}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def _clean_token(token: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (token or "").lower()).strip()


def _tokenize(text: str) -> set[str]:
    tokens = {_clean_token(t) for t in re.split(r"\s+", (text or "").strip())}
    return {t for t in tokens if len(t) >= 3 and t not in _STOPWORDS}


def _normalize_str_set(values: Iterable[str] | None) -> set[str]:
    out: set[str] = set()
    for value in values or []:
        text = str(value or "").strip()
        if not text:
            continue
        out.add(text)
    return out


def _normalize_entity_set(values: Iterable[str] | None) -> set[str]:
    out: set[str] = set()
    for value in values or []:
        text = " ".join(str(value or "").strip().lower().split())
        if not text:
            continue
        out.add(text)
    return out


def _candidate_entity_focus(candidate: dict) -> list[str]:
    payload = candidate.get("payload") or {}
    if isinstance(payload, str):
        return []
    focus = payload.get("entity_focus")
    if isinstance(focus, list):
        return [str(item) for item in focus]
    return []


def score_observation_against_candidate(
    observation: dict, candidate: dict
) -> tuple[float, dict]:
    """Return (score 0-1, reasons dict) for mapping observation → candidate."""

    obs_urls = _normalize_str_set(observation.get("article_urls") or [])
    cand_urls = _normalize_str_set(candidate.get("article_urls") or [])

    obs_structured = _normalize_str_set(
        observation.get("linked_structured_event_ids") or []
    )
    cand_structured = _normalize_str_set(
        candidate.get("linked_structured_event_ids") or []
    )

    obs_entities = _normalize_entity_set(observation.get("entity_focus") or [])
    cand_entities = _normalize_entity_set(_candidate_entity_focus(candidate))

    obs_label = str(observation.get("label") or "")
    cand_label = str(candidate.get("label") or "")

    url_overlap = obs_urls & cand_urls
    structured_overlap = obs_structured & cand_structured

    # Hard-match rules: if we share an article URL (or an upstream structured ID),
    # it's overwhelmingly likely to be the same underlying event.
    if url_overlap:
        return (
            0.93,
            {
                "hard_match": "url_overlap",
                "url_overlap": len(url_overlap),
                "structured_overlap": len(structured_overlap),
            },
        )
    if structured_overlap:
        return (
            0.9,
            {
                "hard_match": "structured_id_overlap",
                "structured_overlap": len(structured_overlap),
            },
        )

    url_j = _jaccard(obs_urls, cand_urls)
    structured_j = _jaccard(obs_structured, cand_structured)
    entity_j = _jaccard(obs_entities, cand_entities)
    label_j = _jaccard(_tokenize(obs_label), _tokenize(cand_label))

    # Conservative weighting: prefer hard evidence overlaps (URLs, structured IDs)
    # over fuzzy text matching.
    score = (
        (structured_j * 0.2)
        + (url_j * 0.2)
        + (entity_j * 0.4)
        + (label_j * 0.2)
    )

    entity_overlap_count = len(obs_entities & cand_entities)

    # Heuristic boosts: if we have several shared entities and the label is at
    # least somewhat similar, treat it as likely the same evolving event.
    if entity_overlap_count >= 4 and label_j >= 0.2:
        score = max(score, 0.74)
    elif entity_overlap_count >= 3 and label_j >= 0.28:
        score = max(score, 0.68)
    elif entity_overlap_count >= 2 and label_j >= 0.4:
        score = max(score, 0.64)

    reasons = {
        "url_jaccard": round(url_j, 4),
        "structured_jaccard": round(structured_j, 4),
        "entity_jaccard": round(entity_j, 4),
        "label_jaccard": round(label_j, 4),
        "url_overlap": len(url_overlap),
        "structured_overlap": len(structured_overlap),
        "entity_overlap": entity_overlap_count,
        "observation_url_count": len(obs_urls),
        "candidate_url_count": len(cand_urls),
        "observation_structured_count": len(obs_structured),
        "candidate_structured_count": len(cand_structured),
    }

    return round(score, 4), reasons


def resolve_canonical_event_id(
    *,
    observation_key: str,
    observation: dict,
    candidates: list[dict],
    threshold: float = 0.62,
) -> tuple[str, dict]:
    """Resolve a stable canonical event_id for a volatile observation key.

    Returns: (event_id, decision)

    Decision fields:
    - action: created_new | mapped_existing
    - confidence: float | None
    - reasons: dict
    - matched_event_id: str | None
    """

    obs_key = (observation_key or "").strip()
    if not obs_key:
        raise ValueError("observation_key is required")

    best = None
    best_score = -1.0
    best_reasons: dict = {}
    scored_candidates: list[dict] = []

    runner_up = None
    runner_up_score = -1.0

    for candidate in candidates or []:
        score, reasons = score_observation_against_candidate(observation, candidate)
        scored_candidates.append(
            {
                "event_id": str(candidate.get("event_id") or ""),
                "score": float(score),
                "reasons": reasons,
            }
        )
        if score > best_score:
            runner_up = best
            runner_up_score = best_score
            best = candidate
            best_score = score
            best_reasons = reasons
        elif score > runner_up_score:
            runner_up = candidate
            runner_up_score = score

    scored_candidates.sort(key=lambda row: row["score"], reverse=True)

    if best and best_score >= threshold:
        confidence = best_score
        ambiguous = (
            runner_up is not None
            and runner_up_score >= threshold
            and (best_score - runner_up_score) < 0.07
        )
        merge_candidates = [
            {
                "event_id": row["event_id"],
                "score": round(float(row["score"]), 4),
            }
            for row in scored_candidates
            if row["event_id"]
            and row["event_id"] != str(best.get("event_id"))
            and float(row["score"]) >= threshold
        ]
        if ambiguous:
            best_reasons = {
                **best_reasons,
                "ambiguous_match": True,
                "runner_up_event_id": runner_up.get("event_id"),
                "runner_up_score": round(runner_up_score, 4),
            }
            confidence = max(0.0, confidence - 0.08)

        weak_match = (
            best_score < max(0.7, threshold + 0.08)
            and int(best_reasons.get("entity_overlap") or 0) <= 2
            and int(best_reasons.get("url_overlap") or 0) == 0
            and int(best_reasons.get("structured_overlap") or 0) == 0
        )
        split_candidate = (
            {
                "existing_event_id": str(best.get("event_id")),
                "score": round(best_score, 4),
                "reason": "weak_existing_match",
            }
            if weak_match
            else None
        )

        return (
            str(best.get("event_id")),
            {
                "action": "mapped_existing",
                "confidence": round(confidence, 4),
                "matched_event_id": str(best.get("event_id")),
                "merge_candidates": merge_candidates,
                "split_candidate": split_candidate,
                "reasons": {
                    **best_reasons,
                    "threshold": threshold,
                    "best_score": round(best_score, 4),
                },
            },
        )

    split_candidate = None
    if best is not None and best_score >= max(0.0, threshold - 0.08):
        split_candidate = {
            "existing_event_id": str(best.get("event_id")),
            "score": round(best_score, 4),
            "reason": "new_event_near_existing_threshold",
        }

    return (
        f"evt_{uuid.uuid4().hex}",
        {
            "action": "created_new",
            "confidence": None,
            "matched_event_id": None,
            "merge_candidates": [],
            "split_candidate": split_candidate,
            "reasons": {
                "observation_key": obs_key,
                "threshold": threshold,
                "best_score": round(best_score, 4) if best is not None else None,
                "best_candidate_event_id": best.get("event_id") if best is not None else None,
            },
        },
    )
