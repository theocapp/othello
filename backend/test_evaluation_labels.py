"""Unit tests for annotation label validation helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from evaluation.labels import validate_annotation_record  # noqa: E402


class TestEvaluationLabels(unittest.TestCase):
    def test_valid_clustering_record(self):
        record = {
            "schema_version": "v1-draft",
            "kind": "clustering",
            "event_id": "evt_1",
            "observation_key": "obs_1",
            "label_decision": "correct_cluster",
            "annotator_id": "analyst_a",
            "annotated_at": "2026-04-07T12:00:00Z",
            "notes": "good cluster",
        }
        self.assertEqual(validate_annotation_record(record), [])

    def test_importance_requires_rank_bucket(self):
        record = {
            "schema_version": "v1-draft",
            "kind": "importance",
            "event_id": "evt_2",
            "label_decision": "should_be_top_n",
            "annotator_id": "analyst_b",
            "annotated_at": "2026-04-07T12:00:00Z",
        }
        errors = validate_annotation_record(record)
        self.assertTrue(any("expected_rank_bucket" in err for err in errors))

    def test_summary_requires_quality_range(self):
        record = {
            "schema_version": "v1-draft",
            "kind": "summary",
            "event_id": "evt_3",
            "label_decision": "faithful",
            "annotator_id": "analyst_c",
            "annotated_at": "2026-04-07T12:00:00Z",
            "overall_quality": 7,
        }
        errors = validate_annotation_record(record)
        self.assertTrue(any("overall_quality" in err for err in errors))


if __name__ == "__main__":
    unittest.main(verbosity=2)
