"""Unit tests for canonical event identity resolution."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import event_identity as identity  # noqa: E402


class TestEventIdentity(unittest.TestCase):
    def test_created_new_generates_evt_id(self):
        event_id, decision = identity.resolve_canonical_event_id(
            observation_key="obs-123",
            observation={
                "label": "Test event",
                "article_urls": ["https://example.com/a"],
                "linked_structured_event_ids": [],
                "entity_focus": ["Iran"],
            },
            candidates=[],
            threshold=0.62,
        )
        self.assertTrue(event_id.startswith("evt_"))
        self.assertNotEqual(event_id, "obs-123")
        self.assertEqual(decision.get("action"), "created_new")

    def test_maps_existing_on_url_overlap(self):
        candidates = [
            {
                "event_id": "evt_existing",
                "label": "Something happened",
                "article_urls": ["https://example.com/a"],
                "linked_structured_event_ids": [],
                "payload": {"entity_focus": []},
            }
        ]
        event_id, decision = identity.resolve_canonical_event_id(
            observation_key="obs-456",
            observation={
                "label": "Another label",
                "article_urls": ["https://example.com/a"],
                "linked_structured_event_ids": [],
                "entity_focus": [],
            },
            candidates=candidates,
            threshold=0.62,
        )
        self.assertEqual(event_id, "evt_existing")
        self.assertEqual(decision.get("action"), "mapped_existing")

    def test_maps_existing_on_entity_and_label_overlap(self):
        candidates = [
            {
                "event_id": "evt_existing",
                "label": "Iran missile strike",
                "article_urls": [],
                "linked_structured_event_ids": [],
                "payload": {
                    "entity_focus": ["Iran", "Israel", "Tehran", "Jerusalem"]
                },
            }
        ]
        event_id, decision = identity.resolve_canonical_event_id(
            observation_key="obs-789",
            observation={
                "label": "Missile strike between Iran and Israel",
                "article_urls": [],
                "linked_structured_event_ids": [],
                "entity_focus": ["Iran", "Israel", "Tehran", "Jerusalem"],
            },
            candidates=candidates,
            threshold=0.62,
        )
        self.assertEqual(event_id, "evt_existing")
        self.assertEqual(decision.get("action"), "mapped_existing")

    def test_flags_merge_candidates_when_multiple_existing_events_match(self):
        candidates = [
            {
                "event_id": "evt_primary",
                "label": "Missile exchange",
                "article_urls": [],
                "linked_structured_event_ids": ["acled_1"],
                "payload": {"entity_focus": ["Iran", "Israel"]},
            },
            {
                "event_id": "evt_secondary",
                "label": "Missile exchange",
                "article_urls": [],
                "linked_structured_event_ids": ["acled_1"],
                "payload": {"entity_focus": ["Iran", "Israel"]},
            },
        ]
        event_id, decision = identity.resolve_canonical_event_id(
            observation_key="obs-merge-1",
            observation={
                "label": "Missile exchange",
                "article_urls": [],
                "linked_structured_event_ids": ["acled_1"],
                "entity_focus": ["Iran", "Israel"],
            },
            candidates=candidates,
            threshold=0.62,
        )
        self.assertEqual(event_id, "evt_primary")
        self.assertEqual(decision.get("action"), "mapped_existing")
        merge_candidates = decision.get("merge_candidates") or []
        self.assertTrue(any(item.get("event_id") == "evt_secondary" for item in merge_candidates))

    def test_flags_split_candidate_when_new_event_is_near_existing_threshold(self):
        candidates = [
            {
                "event_id": "evt_existing",
                "label": "Iran crisis talks",
                "article_urls": [],
                "linked_structured_event_ids": [],
                "payload": {"entity_focus": ["Iran"]},
            }
        ]
        event_id, decision = identity.resolve_canonical_event_id(
            observation_key="obs-split-1",
            observation={
                "label": "Iran crisis talks",
                "article_urls": [],
                "linked_structured_event_ids": [],
                "entity_focus": ["Iran"],
            },
            candidates=candidates,
            threshold=0.62,
        )
        self.assertTrue(event_id.startswith("evt_"))
        self.assertEqual(decision.get("action"), "created_new")
        split_candidate = decision.get("split_candidate") or {}
        self.assertEqual(split_candidate.get("existing_event_id"), "evt_existing")
        self.assertEqual(split_candidate.get("reason"), "new_event_near_existing_threshold")


if __name__ == "__main__":
    unittest.main(verbosity=2)
