"""Claim reconciliation scaffold.

Provides lightweight, local reconciliation heuristics for clustered claims.
This scaffold extracts numeric and date candidates, computes weighted
aggregates using source reliability when available, and returns a
per-group reconciliation summary with provenance and confidence.
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from corpus import load_latest_source_reliability


def _normalize_source_name(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())


def _extract_number_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    # simple numeric with optional commas/decimals
    m = re.search(r"-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|-?\d+(?:\.\d+)?", text)
    if m:
        s = m.group(0).replace(",", "")
        try:
            return float(s)
        except Exception:
            return None
    lower = text.lower()
    if "dozen" in lower or "dozens" in lower:
        return 12.0
    if "hundred" in lower or "hundreds" in lower:
        return 100.0
    if "thousand" in lower or "thousands" in lower:
        return 1000.0
    if "million" in lower or "millions" in lower:
        return 1_000_000.0
    return None


def _extract_date_from_text(text: str) -> Optional[datetime]:
    if not text:
        return None
    # ISO date
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        try:
            return datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc)
        except Exception:
            return None
    # compact ISO-ish: YYYYMMDD
    m2 = re.search(r"(\d{4})(\d{2})(\d{2})", text)
    if m2:
        try:
            y, mo, d = m2.groups()
            return datetime(int(y), int(mo), int(d), tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _weighted_median(pairs: List[Tuple[float, float]]) -> float:
    if not pairs:
        return 0.0
    pairs = sorted(pairs, key=lambda p: p[0])
    total = sum(w for _, w in pairs)
    if total <= 0:
        return pairs[len(pairs) // 2][0]
    acc = 0.0
    for v, w in pairs:
        acc += w
        if acc >= total / 2.0:
            return v
    return pairs[-1][0]


def _source_weight_map(topic: str | None = None) -> Dict[str, float]:
    try:
        rows = load_latest_source_reliability(topic=topic, max_age_hours=24)
        weights: Dict[str, float] = {}
        for key, rec in rows.items():
            name = _normalize_source_name(rec.get("source_name"))
            weights[name] = float(rec.get("weight_multiplier") or 1.0)
        return weights
    except Exception:
        return {}


def reconcile_snapshot(
    claim_rows: List[Dict[str, Any]], topic: str | None = None
) -> Dict[str, Dict[str, Any]]:
    """Reconcile clustered claim rows.

    Returns a mapping from `group_key` to a reconciliation summary containing:
    - reconciled_type: 'numeric'|'date'|'text'|'none'
    - reconciled_value: chosen value (number, ISO date string, or text)
    - confidence: 0.0-1.0
    - method: heuristic used
    - components: list of provenance items
    """
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in claim_rows:
        groups[row.get("group_key")].append(row)

    weights = _source_weight_map(topic)
    results: Dict[str, Dict[str, Any]] = {}

    for group_key, rows in groups.items():
        total_weight = 0.0
        components: List[Dict[str, Any]] = []
        numeric_pairs: List[Tuple[float, float]] = []
        date_pairs: List[Tuple[datetime, float]] = []
        text_weights: Dict[str, float] = defaultdict(float)

        for r in rows:
            src = _normalize_source_name(r.get("source_name"))
            weight = float(weights.get(src, 1.0))
            total_weight += weight
            num = _extract_number_from_text(r.get("claim_text", "") or "")
            date = _extract_date_from_text(r.get("claim_text", "") or "")
            text = (r.get("claim_text") or "").strip()
            components.append(
                {
                    "source_name": r.get("source_name"),
                    "claim_text": text,
                    "numeric_value": num,
                    "date_value": date.isoformat() if date is not None else None,
                    "weight": weight,
                    "published_at": r.get("published_at"),
                }
            )
            if num is not None:
                numeric_pairs.append((num, weight))
            if date is not None:
                date_pairs.append((date, weight))
            if text:
                text_weights[text] += weight

        summary: Dict[str, Any] = {
            "group_key": group_key,
            "reconciled_type": "none",
            "reconciled_value": None,
            "confidence": 0.0,
            "method": "none",
            "components": components,
        }

        if numeric_pairs:
            median = _weighted_median(numeric_pairs)
            tol = max(1.0, abs(median) * 0.15)
            in_tol = sum(w for v, w in numeric_pairs if abs(v - median) <= tol)
            confidence = min(1.0, in_tol / (total_weight or 1.0))
            summary.update(
                {
                    "reconciled_type": "numeric",
                    "reconciled_value": median,
                    "confidence": round(float(confidence), 3),
                    "method": "weighted_median",
                }
            )
            results[group_key] = summary
            continue

        if date_pairs:
            # pick date with highest aggregated weight
            accum: Dict[str, float] = defaultdict(float)
            for d, w in date_pairs:
                accum[d.isoformat()] += w
            chosen, chosen_w = max(accum.items(), key=lambda kv: kv[1])
            confidence = min(1.0, chosen_w / (total_weight or 1.0))
            summary.update(
                {
                    "reconciled_type": "date",
                    "reconciled_value": chosen,
                    "confidence": round(float(confidence), 3),
                    "method": "weighted_vote_date",
                }
            )
            results[group_key] = summary
            continue

        if text_weights:
            chosen_text, weight_sum = max(text_weights.items(), key=lambda kv: kv[1])
            confidence = min(1.0, weight_sum / (total_weight or 1.0))
            summary.update(
                {
                    "reconciled_type": "text",
                    "reconciled_value": chosen_text,
                    "confidence": round(float(confidence), 3),
                    "method": "weighted_vote",
                }
            )
            results[group_key] = summary
            continue

        results[group_key] = summary

    return results
