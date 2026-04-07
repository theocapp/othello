"""Tests for analytics scorecard service payload."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import services.analytics_service as analytics_service_module  # noqa: E402


class TestAnalyticsServiceScorecard(unittest.TestCase):
    def test_invalid_kind_raises_400(self):
        with self.assertRaises(HTTPException) as raised:
            analytics_service_module.evaluation_scorecard_payload(kind="not_a_kind")
        self.assertEqual(raised.exception.status_code, 400)

    def test_payload_delegates_to_scorecard_builder(self):
        expected = {"records_considered": 4}
        with patch(
            "evaluation.scorecards.build_scorecard_snapshot",
            return_value=expected,
        ) as mocked:
            result = analytics_service_module.evaluation_scorecard_payload(
                kind="Clustering",
                topic="geopolitics",
                limit_files=99999,
                include_error_samples=True,
            )

        self.assertEqual(result, expected)
        mocked.assert_called_once_with(
            kind="clustering",
            topic="geopolitics",
            limit_files=5000,
            include_error_samples=True,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
