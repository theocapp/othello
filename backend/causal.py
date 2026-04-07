"""Lightweight causal reasoning scaffolding for event-level causal links.

This module provides a minimal in-memory DAG-style graph useful for
experimentation and for wiring a causal-reasoning layer into event
story materialization. It purposely avoids heavy dependencies and uses
heuristics (shared entities, temporal ordering, causal keyphrases,
token overlap) to propose candidate causal edges.

The implementation is intended as a scaffold: replace or augment with
ML-based or knowledge-graph approaches as needed.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List

from core.runtime import parse_timestamp

CAUSAL_KEYPHRASES = [
    "in response to",
    "in retaliation for",
    "in retaliation",
    "after",
    "following",
    "because of",
    "due to",
    "amid",
    "over",
    "sparking",
]


def _normalize_text(s: str) -> str:
    return re.sub(r"\W+", " ", (s or "").lower()).strip()


class EventNode:
    def __init__(
        self,
        node_id: str,
        title: str,
        published_at: str | datetime | None,
        summary: str = "",
        entities: List[str] | None = None,
        country: str | None = None,
    ):
        self.id = node_id
        self.title = title or ""
        self.published_at = None
        if isinstance(published_at, str):
            self.published_at = parse_timestamp(published_at)
        elif isinstance(published_at, datetime):
            self.published_at = published_at
        self.summary = summary or ""
        self.entities = [e.lower() for e in (entities or [])]
        self.country = (country or "").lower() if country else None
        self._norm = _normalize_text(self.title + " " + self.summary)

    def tokens(self) -> set[str]:
        if not self._norm:
            return set()
        return set(self._norm.split())


class CausalGraph:
    """A simple in-memory event graph that proposes causal edges.

    Methods are intentionally lightweight so this can be used in unit
    tests and iteratively integrated into `story_materialization.py` or
    `correlation_engine.py`.
    """

    def __init__(self) -> None:
        self.nodes: Dict[str, EventNode] = {}
        self.edges: List[Dict] = []

    def add_node_from_event(self, ev: Dict) -> EventNode:
        """Add an event dict to the graph and return its node.

        The event dict is expected to have at least `id` or `title` and
        `published_at` fields. Entities should be a list of strings when available.
        """
        nid = (
            ev.get("id")
            or ev.get("event_id")
            or ev.get("url")
            or (ev.get("title") or "") + "@" + str(ev.get("published_at") or "")
        )
        node = EventNode(
            nid,
            ev.get("title"),
            ev.get("published_at"),
            summary=ev.get("summary", ""),
            entities=ev.get("entities", []),
            country=ev.get("country"),
        )
        self.nodes[nid] = node
        return node

    def infer_edges(self, max_lag_days: int = 14, min_score: float = 0.35) -> None:
        """Infer candidate causal edges between events using simple heuristics.

        - Causes must be earlier than effects (strictly before).
        - Only consider pairs within `max_lag_days`.
        - Score is accumulated from shared entities, same country,
          presence of causal keyphrases in the later event, and token overlap.
        """
        ids = list(self.nodes.keys())
        for a_id in ids:
            a = self.nodes[a_id]
            if not a.published_at:
                continue
            for b_id in ids:
                if a_id == b_id:
                    continue
                b = self.nodes[b_id]
                if not b.published_at:
                    continue
                # temporal ordering: cause must be strictly before effect
                if a.published_at >= b.published_at:
                    continue
                # max lag
                delta_days = (b.published_at - a.published_at).days
                if delta_days > max_lag_days:
                    continue

                score = 0.0
                reasons: List[str] = []

                # shared entities
                if set(a.entities) & set(b.entities):
                    score += 0.40
                    reasons.append("shared_entity")

                # same country
                if a.country and b.country and a.country == b.country:
                    score += 0.20
                    reasons.append("same_country")

                # causal keyphrases in the effect text
                effect_text = (b.title + " " + b.summary).lower()
                for phrase in CAUSAL_KEYPHRASES:
                    if phrase in effect_text:
                        score += 0.35
                        reasons.append(f"phrase:{phrase}")
                        break

                # token overlap (relative fraction)
                a_tokens = a.tokens()
                b_tokens = b.tokens()
                if a_tokens and b_tokens:
                    overlap = len(a_tokens & b_tokens)
                    denom = max(1, min(len(a_tokens), len(b_tokens)))
                    frac = overlap / denom
                    if frac > 0.1:
                        score += min(0.3, frac * 0.6)
                        reasons.append(f"token_overlap:{frac:.2f}")

                score = min(1.0, score)
                if score >= min_score:
                    self.edges.append(
                        {
                            "cause": a_id,
                            "effect": b_id,
                            "score": round(score, 3),
                            "reasons": reasons,
                        }
                    )

    def build_from_events(
        self, events: List[Dict], max_lag_days: int = 14, min_score: float = 0.35
    ) -> "CausalGraph":
        for ev in events:
            self.add_node_from_event(ev)
        self.infer_edges(max_lag_days=max_lag_days, min_score=min_score)
        return self


__all__ = ["CausalGraph", "EventNode"]
