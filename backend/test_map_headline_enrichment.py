from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import structured_story_rollups as rollups  # noqa: E402
import services.map_service as map_service  # noqa: E402


class TestMapHeadlineEnrichment(unittest.TestCase):
    def test_structured_rollup_projects_source_urls(self):
        event = {
            "event_id": "evt-1",
            "dataset": "gdelt_gkg",
            "event_date": "2026-04-10",
            "country": "Lebanon",
            "region": None,
            "admin1": "Beyrouth",
            "admin2": None,
            "location": "Beirut, Lebanon",
            "latitude": 33.89,
            "longitude": 35.5,
            "event_type": "Battles",
            "sub_event_type": "Fight",
            "actor_primary": None,
            "actor_secondary": None,
            "fatalities": 0,
            "source_count": 1,
            "source_urls": ["https://example.test/a"],
            "summary": "Fight reported in Beirut.",
            "payload": {"source_url": "https://example.test/b", "source": "Example"},
        }

        with patch.object(rollups, "get_recent_structured_events", return_value=[event]):
            clusters = rollups.build_structured_story_clusters(
                days=1,
                limit=1,
                source_limit=10,
                dataset=None,
            )

        self.assertEqual(len(clusters), 1)
        projected = clusters[0]["events"][0]
        urls = projected.get("source_urls") or []
        self.assertIn("https://example.test/a", urls)
        self.assertIn("https://example.test/b", urls)

    def test_incident_hotspot_prefers_article_headline(self):
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=2)
        source_url = "https://example.test/headline-1"
        semantic_clusters = [
            {
                "label": "cluster",
                "events": [
                    {
                        "event_id": "evt-hotspot-1",
                        "event_date": now.isoformat(),
                        "country": "Lebanon",
                        "admin1": "Beyrouth",
                        "location": "Beirut, Lebanon",
                        "latitude": 33.89,
                        "longitude": 35.5,
                        "event_type": "Battles",
                        "sub_event_type": "Fight",
                        "fatalities": 0,
                        "source_count": 1,
                        "source_urls": [source_url],
                        "summary": "",
                        "payload": {},
                    }
                ],
            }
        ]
        article_lookup = {
            source_url: {
                "url": source_url,
                "title": "Ceasefire talks advance in Beirut",
                "description": "Officials report a new diplomatic channel opening overnight.",
            }
        }

        hotspots, _, _ = map_service._incident_hotspots_from_semantic_clusters(
            semantic_clusters=semantic_clusters,
            now=now,
            hours=48,
            cutoff=cutoff,
            article_lookup_by_url=article_lookup,
        )

        self.assertEqual(len(hotspots), 1)
        hotspot = hotspots[0]
        sample = hotspot["sample_events"][0]
        self.assertEqual(hotspot["label"], "Ceasefire talks advance in Beirut")
        self.assertEqual(sample["title"], "Ceasefire talks advance in Beirut")
        self.assertIn("diplomatic channel", sample["summary"])


if __name__ == "__main__":
    unittest.main()
