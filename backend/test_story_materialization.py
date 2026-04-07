"""Unit tests for story materialization helper behavior."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import story_materialization as story_materialization_module  # noqa: E402


class TestStoryMaterialization(unittest.TestCase):
    def test_link_structured_ids_prefers_target_country(self):
        event = {
            "earliest_update": "2026-04-07T12:00:00Z",
            "latest_update": "2026-04-07T12:00:00Z",
            "entity_focus": ["United States", "Iran"],
        }

        with patch.object(
            story_materialization_module,
            "list_structured_event_ids_in_date_range",
            return_value=["us-1", "iran-1"],
        ), patch.object(
            story_materialization_module,
            "get_structured_event_coordinates_by_ids",
            return_value={
                "us-1": {"country": "United States"},
                "iran-1": {"country": "Iran"},
            },
        ):
            self.assertEqual(
                story_materialization_module._link_structured_ids(event),
                ["iran-1"],
            )

    def test_link_structured_ids_uses_label_country_when_focus_is_missing(self):
        event = {
            "earliest_update": "2026-04-07T12:00:00Z",
            "latest_update": "2026-04-07T12:00:00Z",
            "label": "U.S. strike in Iran",
        }

        with patch.object(
            story_materialization_module,
            "list_structured_event_ids_in_date_range",
            return_value=["us-1", "iran-1"],
        ), patch.object(
            story_materialization_module,
            "get_structured_event_coordinates_by_ids",
            return_value={
                "us-1": {"country": "United States"},
                "iran-1": {"country": "Iran"},
            },
        ):
            self.assertEqual(
                story_materialization_module._link_structured_ids(event),
                ["iran-1"],
            )

    def test_link_structured_ids_falls_back_when_no_country_preference(self):
        event = {
            "earliest_update": "2026-04-07T12:00:00Z",
            "latest_update": "2026-04-07T12:00:00Z",
            "entity_focus": ["Pentagon"],
        }

        with patch.object(
            story_materialization_module,
            "list_structured_event_ids_in_date_range",
            return_value=["us-1", "iran-1"],
        ):
            self.assertEqual(
                story_materialization_module._link_structured_ids(event),
                ["us-1", "iran-1"],
            )

    def test_importance_score_monotonic_with_more_tier1_sources(self):
        low_signal_event = {
            "label": "Regional tensions rise",
            "articles": [
                {
                    "url": "https://example.com/1",
                    "source": "Wire One",
                    "source_domain": "wire1.com",
                    "published_at": "2026-04-07T12:00:00Z",
                    "language": "en",
                }
            ],
            "entity_focus": ["Iran", "Israel"],
            "contradictions": [],
            "latest_update": "2026-04-07T12:00:00Z",
            "story_anchor_focus": ["meeting"],
        }
        high_signal_event = {
            **low_signal_event,
            "articles": [
                {
                    "url": "https://example.com/1",
                    "source": "Wire One",
                    "source_domain": "wire1.com",
                    "published_at": "2026-04-07T12:00:00Z",
                    "language": "en",
                },
                {
                    "url": "https://example.com/2",
                    "source": "Wire Two",
                    "source_domain": "wire2.com",
                    "published_at": "2026-04-07T12:05:00Z",
                    "language": "en",
                },
                {
                    "url": "https://example.com/3",
                    "source": "Wire Three",
                    "source_domain": "wire3.com",
                    "published_at": "2026-04-07T12:10:00Z",
                    "language": "en",
                },
            ],
        }
        reliability = {
            "wire one": {"empirical_score": 0.9},
            "wire two": {"empirical_score": 0.88},
            "wire three": {"empirical_score": 0.86},
        }
        registry = {
            "wire1.com": {"trust_tier": "tier_1", "region": "global"},
            "wire2.com": {"trust_tier": "tier_1", "region": "europe"},
            "wire3.com": {"trust_tier": "tier_1", "region": "middle-east"},
        }

        low_score, _, _ = story_materialization_module._build_importance_scoring_artifacts(
            low_signal_event,
            linked_structured_ids=[],
            reliability_by_source=reliability,
            registry_by_domain=registry,
            latest_observation=None,
            structured_meta_by_id={},
        )
        high_score, _, _ = story_materialization_module._build_importance_scoring_artifacts(
            high_signal_event,
            linked_structured_ids=[],
            reliability_by_source=reliability,
            registry_by_domain=registry,
            latest_observation=None,
            structured_meta_by_id={},
        )

        self.assertGreater(high_score, low_score)

    def test_cluster_assignment_evidence_rows_include_overlap_and_rule(self):
        event = {
            "label": "Iran and Israel exchange missile fire",
            "summary": "Officials report overnight missile strike activity.",
            "latest_update": "2026-04-07T12:30:00Z",
            "entity_focus": ["Iran", "Israel"],
            "story_anchor_focus": ["missile", "strike"],
            "articles": [
                {
                    "url": "https://example.com/a",
                    "title": "Iran missile strike draws response from Israel",
                    "description": "Leaders discuss strike and regional fallout.",
                    "published_at": "2026-04-07T11:45:00Z",
                    "source": "Example Source",
                }
            ],
        }

        rows = story_materialization_module._build_cluster_assignment_evidence_rows(
            event_id="evt_test",
            topic="geopolitics",
            observation_key="obs_1",
            event=event,
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["observation_key"], "obs_1")
        self.assertEqual(row["event_id"], "evt_test")
        self.assertEqual(row["article_url"], "https://example.com/a")
        self.assertGreaterEqual(row["entity_overlap"], 1)
        self.assertGreaterEqual(row["anchor_overlap"], 1)
        self.assertGreater(row["final_score"], 0)
        self.assertIn(
            row["rule"],
            {
                "anchor_entity_temporal",
                "entity_keyword_temporal",
                "anchor_keyword_compact",
                "score_threshold",
                "fallback_in_cluster",
            },
        )

    def test_materialization_uses_mapped_stable_event_id(self):
        event = {
            "label": "Test event",
            "summary": "Summary",
            "articles": [
                {
                    "url": "https://example.com/a",
                    "source": "Example Source",
                    "source_domain": "example.com",
                    "published_at": "2026-04-07T12:00:00Z",
                }
            ],
            "earliest_update": "2026-04-07T12:00:00Z",
            "latest_update": "2026-04-07T12:30:00Z",
            "entity_focus": ["Iran"],
            "contradictions": [],
        }

        with patch.object(
            story_materialization_module,
            "get_source_registry",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "load_latest_source_reliability",
            return_value={},
        ), patch.object(
            story_materialization_module,
            "get_recent_articles",
            return_value=[
                {
                    "url": "https://example.com/a",
                    "source": "Example Source",
                    "source_domain": "example.com",
                    "published_at": "2026-04-07T12:00:00Z",
                }
            ],
        ), patch.object(
            story_materialization_module,
            "cluster_articles",
            return_value=[event],
        ), patch.object(
            story_materialization_module,
            "enrich_events",
            return_value=[event],
        ), patch.object(
            story_materialization_module,
            "event_cluster_key",
            return_value="obs_1",
        ), patch.object(
            story_materialization_module,
            "_link_structured_ids",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "get_event_id_for_observation_key",
            return_value="evt_existing",
        ), patch.object(
            story_materialization_module,
            "list_canonical_identity_candidates",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "get_latest_canonical_event_observation",
            return_value=None,
        ), patch.object(
            story_materialization_module,
            "resolve_canonical_event_id",
        ) as resolve_mock, patch.object(
            story_materialization_module,
            "load_framing_signals_for_article_urls",
            return_value={},
        ), patch.object(
            story_materialization_module,
            "load_claim_resolution_for_event_key",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "replace_materialized_story_clusters",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_event_perspectives",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_event_identity_mappings",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_canonical_event_observations",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_cluster_assignment_evidence",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "append_event_identity_events",
            return_value=0,
        ), patch.object(
            story_materialization_module,
            "upsert_canonical_events",
            return_value=1,
        ) as upsert_canonical:
            story_materialization_module.rebuild_materialized_story_clusters(
                topics=["geopolitics"],
                window_hours=96,
                articles_limit=20,
            )

            resolve_mock.assert_not_called()
            canonical_rows = upsert_canonical.call_args[0][0]
            self.assertEqual(canonical_rows[0]["event_id"], "evt_existing")

    def test_materialization_records_new_identity_decision(self):
        event = {
            "label": "Test event",
            "summary": "Summary",
            "articles": [
                {
                    "url": "https://example.com/a",
                    "source": "Example Source",
                    "source_domain": "example.com",
                    "published_at": "2026-04-07T12:00:00Z",
                }
            ],
            "earliest_update": "2026-04-07T12:00:00Z",
            "latest_update": "2026-04-07T12:30:00Z",
            "entity_focus": ["Iran"],
            "contradictions": [],
        }

        decision = {
            "action": "created_new",
            "confidence": None,
            "reasons": {"threshold": 0.62},
        }

        with patch.object(
            story_materialization_module,
            "get_source_registry",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "load_latest_source_reliability",
            return_value={},
        ), patch.object(
            story_materialization_module,
            "get_recent_articles",
            return_value=[
                {
                    "url": "https://example.com/a",
                    "source": "Example Source",
                    "source_domain": "example.com",
                    "published_at": "2026-04-07T12:00:00Z",
                }
            ],
        ), patch.object(
            story_materialization_module,
            "cluster_articles",
            return_value=[event],
        ), patch.object(
            story_materialization_module,
            "enrich_events",
            return_value=[event],
        ), patch.object(
            story_materialization_module,
            "event_cluster_key",
            return_value="obs_1",
        ), patch.object(
            story_materialization_module,
            "_link_structured_ids",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "get_event_id_for_observation_key",
            return_value=None,
        ), patch.object(
            story_materialization_module,
            "list_canonical_identity_candidates",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "get_latest_canonical_event_observation",
            return_value=None,
        ), patch.object(
            story_materialization_module,
            "resolve_canonical_event_id",
            return_value=("evt_new", decision),
        ), patch.object(
            story_materialization_module,
            "load_framing_signals_for_article_urls",
            return_value={},
        ), patch.object(
            story_materialization_module,
            "load_claim_resolution_for_event_key",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "replace_materialized_story_clusters",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_event_perspectives",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_canonical_events",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_event_identity_mappings",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_canonical_event_observations",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_cluster_assignment_evidence",
            return_value=1,
        ) as upsert_map, patch.object(
            story_materialization_module,
            "append_event_identity_events",
            return_value=1,
        ) as append_events:
            story_materialization_module.rebuild_materialized_story_clusters(
                topics=["geopolitics"],
                window_hours=96,
                articles_limit=20,
            )

            mapping_rows = upsert_map.call_args[0][0]
            self.assertEqual(mapping_rows[0]["observation_key"], "obs_1")
            self.assertEqual(mapping_rows[0]["event_id"], "evt_new")

            event_rows = append_events.call_args[0][0]
            self.assertEqual(event_rows[0]["action"], "created_new")

    def test_materialization_persists_merge_and_split_candidate_events(self):
        event = {
            "label": "Test event",
            "summary": "Summary",
            "articles": [
                {
                    "url": "https://example.com/a",
                    "source": "Example Source",
                    "source_domain": "example.com",
                    "published_at": "2026-04-07T12:00:00Z",
                }
            ],
            "earliest_update": "2026-04-07T12:00:00Z",
            "latest_update": "2026-04-07T12:30:00Z",
            "entity_focus": ["Iran"],
            "contradictions": [],
        }

        decision = {
            "action": "mapped_existing",
            "confidence": 0.66,
            "reasons": {"best_score": 0.66},
            "merge_candidates": [
                {"event_id": "evt_other", "score": 0.64},
            ],
            "split_candidate": {
                "existing_event_id": "evt_existing",
                "score": 0.66,
                "reason": "weak_existing_match",
            },
        }

        with patch.object(
            story_materialization_module,
            "get_source_registry",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "load_latest_source_reliability",
            return_value={},
        ), patch.object(
            story_materialization_module,
            "get_recent_articles",
            return_value=[
                {
                    "url": "https://example.com/a",
                    "source": "Example Source",
                    "source_domain": "example.com",
                    "published_at": "2026-04-07T12:00:00Z",
                }
            ],
        ), patch.object(
            story_materialization_module,
            "cluster_articles",
            return_value=[event],
        ), patch.object(
            story_materialization_module,
            "enrich_events",
            return_value=[event],
        ), patch.object(
            story_materialization_module,
            "event_cluster_key",
            return_value="obs_1",
        ), patch.object(
            story_materialization_module,
            "_link_structured_ids",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "get_event_id_for_observation_key",
            return_value=None,
        ), patch.object(
            story_materialization_module,
            "list_canonical_identity_candidates",
            return_value=[{"event_id": "evt_existing", "payload": {"entity_focus": []}}],
        ), patch.object(
            story_materialization_module,
            "get_latest_canonical_event_observation",
            return_value=None,
        ), patch.object(
            story_materialization_module,
            "resolve_canonical_event_id",
            return_value=("evt_existing", decision),
        ), patch.object(
            story_materialization_module,
            "load_framing_signals_for_article_urls",
            return_value={},
        ), patch.object(
            story_materialization_module,
            "load_claim_resolution_for_event_key",
            return_value=[],
        ), patch.object(
            story_materialization_module,
            "replace_materialized_story_clusters",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_event_perspectives",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_canonical_events",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_event_identity_mappings",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_canonical_event_observations",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "upsert_cluster_assignment_evidence",
            return_value=1,
        ), patch.object(
            story_materialization_module,
            "append_event_identity_events",
            return_value=3,
        ) as append_events:
            story_materialization_module.rebuild_materialized_story_clusters(
                topics=["geopolitics"],
                window_hours=96,
                articles_limit=20,
            )

            event_rows = append_events.call_args[0][0]
            actions = {row.get("action") for row in event_rows}
            self.assertIn("mapped_existing", actions)
            self.assertIn("merge_candidate", actions)
            self.assertIn("split_candidate", actions)


if __name__ == "__main__":
    unittest.main(verbosity=2)
