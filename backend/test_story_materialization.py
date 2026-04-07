"""Unit tests for story materialization helper behavior."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import story_materialization as story_materialization_module


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
