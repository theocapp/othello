import unittest

from services.event_importance import annotate_event_importance, compute_event_importance


class EventImportanceTests(unittest.TestCase):
    def test_score_increases_with_source_density(self):
        sparse = compute_event_importance({
            "source_count": 1,
            "article_count": 1,
            "tier_1_source_count": 0,
            "contradiction_count": 0,
            "entity_focus": [],
        })
        dense = compute_event_importance({
            "source_count": 6,
            "article_count": 12,
            "tier_1_source_count": 2,
            "contradiction_count": 1,
            "entity_focus": ["NATO", "Russia", "Ukraine"],
        })
        assert dense["importance_score"] > sparse["importance_score"]

    def test_annotation_preserves_existing_fields(self):
        event = {
            "event_id": "evt-1",
            "label": "Test event",
            "source_count": 4,
            "article_count": 7,
            "contradiction_count": 2,
        }
        annotated = annotate_event_importance(event)
        assert annotated["event_id"] == "evt-1"
        assert annotated["label"] == "Test event"
        assert "importance_score" in annotated
        assert "importance_bucket" in annotated
        assert "importance_breakdown" in annotated
