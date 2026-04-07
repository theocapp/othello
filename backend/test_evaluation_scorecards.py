"""Unit tests for scorecard snapshot aggregation."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from evaluation.scorecards import build_scorecard_snapshot  # noqa: E402


class TestEvaluationScorecards(unittest.TestCase):
    def test_snapshot_aggregates_valid_records_and_errors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            labels_dir = Path(tmp_dir)
            file_path = labels_dir / "labels_a.jsonl"
            lines = [
                {
                    "schema_version": "v1-draft",
                    "kind": "clustering",
                    "event_id": "evt_1",
                    "observation_key": "obs_1",
                    "label_decision": "correct_cluster",
                    "annotator_id": "ann_a",
                    "annotated_at": "2026-04-07T12:00:00Z",
                    "topic": "geopolitics",
                },
                {
                    "schema_version": "v1-draft",
                    "kind": "clustering",
                    "event_id": "evt_1",
                    "observation_key": "obs_1",
                    "label_decision": "correct_cluster",
                    "annotator_id": "ann_b",
                    "annotated_at": "2026-04-07T12:01:00Z",
                    "topic": "geopolitics",
                },
                {
                    "schema_version": "v1-draft",
                    "kind": "clustering",
                    "event_id": "evt_2",
                    "observation_key": "obs_2",
                    "label_decision": "false_split",
                    "annotator_id": "ann_a",
                    "annotated_at": "2026-04-07T12:02:00Z",
                    "topic": "geopolitics",
                },
                {
                    "schema_version": "v1-draft",
                    "kind": "clustering",
                    "event_id": "evt_2",
                    "observation_key": "obs_2",
                    "label_decision": "false_merge",
                    "annotator_id": "ann_b",
                    "annotated_at": "2026-04-07T12:03:00Z",
                    "topic": "geopolitics",
                },
                {
                    "schema_version": "v1-draft",
                    "kind": "importance",
                    "event_id": "evt_3",
                    "expected_rank_bucket": "top_10",
                    "label_decision": "should_be_top_n",
                    "annotator_id": "ann_c",
                    "annotated_at": "2026-04-07T12:05:00Z",
                    "topic": "geopolitics",
                },
                "{not-json}",
                {
                    "schema_version": "v1-draft",
                    "kind": "clustering",
                    "event_id": "evt_bad",
                },
            ]

            with file_path.open("w", encoding="utf-8") as handle:
                for line in lines:
                    if isinstance(line, str):
                        handle.write(line + "\n")
                    else:
                        handle.write(json.dumps(line) + "\n")

            snapshot = build_scorecard_snapshot(
                labels_dir=str(labels_dir),
                kind="clustering",
                include_error_samples=True,
            )

            self.assertEqual(snapshot["files_scanned"], 1)
            self.assertEqual(snapshot["records_scanned"], 7)
            self.assertEqual(snapshot["valid_records"], 5)
            self.assertEqual(snapshot["invalid_records"], 2)
            self.assertEqual(snapshot["records_considered"], 4)
            self.assertGreaterEqual(len(snapshot["error_samples"]), 1)

            clustering = snapshot["kind_summaries"]["clustering"]
            self.assertEqual(clustering["record_count"], 4)
            self.assertEqual(clustering["comparable_event_count"], 2)
            self.assertEqual(clustering["agreement_rate"], 0.75)
            self.assertEqual(clustering["decision_counts"]["correct_cluster"], 2)

    def test_snapshot_handles_missing_directory(self):
        snapshot = build_scorecard_snapshot(labels_dir="/tmp/non-existent-othello-labels")
        self.assertEqual(snapshot["files_scanned"], 0)
        self.assertEqual(snapshot["records_scanned"], 0)
        self.assertEqual(snapshot["records_considered"], 0)

    def test_snapshot_includes_cluster_cohesion_operational_metrics(self):
        with tempfile.TemporaryDirectory() as tmp_dir, patch(
            "evaluation.scorecards._get_canonical_events",
            return_value=[
                {
                    "event_id": "evt_1",
                    "payload": {
                        "cluster_cohesion": {
                            "mean_relatedness": 6.2,
                            "outlier_ratio": 0.0,
                        }
                    },
                },
                {
                    "event_id": "evt_2",
                    "payload": {
                        "cluster_cohesion": {
                            "mean_relatedness": 4.0,
                            "outlier_ratio": 0.5,
                        }
                    },
                },
            ],
        ):
            snapshot = build_scorecard_snapshot(labels_dir=str(Path(tmp_dir)))

        cohesion = snapshot["operational_metrics"]["cluster_cohesion"]
        self.assertEqual(cohesion["event_count"], 2)
        self.assertEqual(cohesion["events_with_cohesion"], 2)
        self.assertEqual(cohesion["coverage_rate"], 1.0)
        self.assertEqual(cohesion["avg_mean_relatedness"], 5.1)
        self.assertEqual(cohesion["avg_outlier_ratio"], 0.25)
        self.assertEqual(cohesion["outlier_ratio_p75"], 0.375)
        self.assertEqual(cohesion["outlier_ratio_p90"], 0.45)
        self.assertEqual(cohesion["high_outlier_threshold"], 0.34)
        self.assertEqual(cohesion["high_outlier_event_rate"], 0.5)

    def test_snapshot_respects_configured_high_outlier_threshold(self):
        with tempfile.TemporaryDirectory() as tmp_dir, patch(
            "evaluation.scorecards._get_canonical_events",
            return_value=[
                {"event_id": "evt_1", "payload": {"cluster_cohesion": {"outlier_ratio": 0.5}}},
                {"event_id": "evt_2", "payload": {"cluster_cohesion": {"outlier_ratio": 0.4}}},
            ],
        ), patch(
            "evaluation.scorecards.EVALUATION_COHESION_HIGH_OUTLIER_THRESHOLD",
            0.6,
        ):
            snapshot = build_scorecard_snapshot(labels_dir=str(Path(tmp_dir)))

        cohesion = snapshot["operational_metrics"]["cluster_cohesion"]
        self.assertEqual(cohesion["high_outlier_threshold"], 0.6)
        self.assertEqual(cohesion["high_outlier_event_rate"], 0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
