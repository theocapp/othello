"""Unit tests for canonical event debug payload service."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import services.events_service as events_service_module  # noqa: E402


class TestEventsServiceDebug(unittest.TestCase):
    def test_debug_payload_404_when_event_missing(self):
        with patch.object(
            events_service_module,
            "get_canonical_event",
            return_value=None,
        ):
            with self.assertRaises(HTTPException) as raised:
                events_service_module.get_canonical_event_debug_payload("evt_missing")
            self.assertEqual(raised.exception.status_code, 404)

    def test_debug_payload_assembles_identity_claims_and_contradictions(self):
        event = {
            "event_id": "evt_1",
            "topic": "geopolitics",
            "label": "Test event",
            "article_urls": ["https://example.com/a"],
            "importance_score": 71.5,
            "importance_reasons": ["3 distinct sources (2 tier-1)"],
            "payload": {
                "importance": {
                    "breakdown": {
                        "source_credibility": 24.0,
                        "growth_novelty": 11.0,
                    }
                }
            },
        }
        perspectives = [
            {
                "perspective_id": "p1",
                "event_id": "evt_1",
                "article_url": "https://example.com/a",
                "source_name": "Example Source",
            }
        ]

        with patch.object(
            events_service_module,
            "get_canonical_event",
            return_value=event,
        ), patch.object(
            events_service_module,
            "get_event_perspectives",
            return_value=perspectives,
        ), patch.object(
            events_service_module,
            "get_articles_by_urls",
            return_value={
                "https://example.com/a": {
                    "url": "https://example.com/a",
                    "title": "A title",
                    "description": "A description",
                    "source": "Example Source",
                    "source_domain": "example.com",
                    "published_at": "2026-04-07T12:00:00Z",
                    "language": "en",
                }
            },
        ), patch.object(
            events_service_module,
            "load_framing_signals_for_article_urls",
            return_value={"https://example.com/a": {"dominant_frame": "security"}},
        ), patch.object(
            events_service_module,
            "list_observation_keys_for_event",
            return_value=["obs_1"],
        ), patch.object(
            events_service_module,
            "load_event_identity_history",
            return_value=[{"event_id": "evt_1", "action": "created_new"}],
        ), patch.object(
            events_service_module,
            "load_cluster_assignment_evidence",
            return_value={
                "obs_1": [
                    {
                        "observation_key": "obs_1",
                        "article_url": "https://example.com/a",
                        "rule": "anchor_entity_temporal",
                        "entity_overlap": 2,
                        "anchor_overlap": 1,
                        "keyword_overlap": 3,
                        "final_score": 8.1,
                    }
                ]
            },
        ), patch.object(
            events_service_module,
            "load_contradiction_record",
            return_value={
                "event_key": "obs_1",
                "contradictions": [{"conflict_type": "timeline"}],
                "contradiction_count": 1,
            },
        ), patch.object(
            events_service_module,
            "load_claim_resolution_for_event_key",
            return_value=[
                {
                    "claim_record_key": "claim_1",
                    "claim_text": "A claim",
                    "resolution_status": "unresolved",
                }
            ],
        ):
            payload = events_service_module.get_canonical_event_debug_payload("evt_1")

        self.assertEqual(payload["event"]["event_id"], "evt_1")
        self.assertEqual(payload["counts"]["articles"], 1)
        self.assertEqual(payload["counts"]["claims"], 1)
        self.assertEqual(payload["counts"]["contradictions"], 1)
        self.assertEqual(payload["counts"]["cluster_assignment_evidence"], 1)
        self.assertEqual(payload["observation_keys"], ["obs_1"])
        self.assertEqual(payload["cluster_assignment_evidence"][0]["rule"], "anchor_entity_temporal")
        self.assertEqual(payload["event"]["importance"]["score"], 71.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
