"""
HTTP smoke tests for the FastAPI app.

Uses an isolated Postgres test database. Run from backend/:

    python -m unittest test_api_smoke -v

Requires a Postgres database named `othello_test` (or set OTHELLO_TEST_PGDATABASE).
Create it once with: createdb othello_test
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

os.environ["OTHELLO_INTERNAL_SCHEDULER"] = "false"
# Point corpus at the test database
os.environ["OTHELLO_PGDATABASE"] = os.environ.get("OTHELLO_TEST_PGDATABASE", "othello_test")

# Isolate the briefing/headline cache (cache.py still uses SQLite)
_TEST_HOME = tempfile.mkdtemp(prefix="othello_api_smoke_", dir=str(_BACKEND))
import cache as _cache_module
_cache_module.DB_PATH = Path(_TEST_HOME) / "othello_cache.db"

import corpus
corpus.init_db()

import main as main_module
from corpus import upsert_structured_events
from starlette.testclient import TestClient


def tearDownModule():
    shutil.rmtree(_TEST_HOME, ignore_errors=True)


class TestAPISmoke(unittest.TestCase):
    def test_health_returns_runtime_shape(self):
        with TestClient(main_module.app) as client:
            response = client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("runtime", data)
        self.assertIn("scheduler_running", data)
        self.assertFalse(data["internal_scheduler_enabled"])

    def test_openapi_available(self):
        with TestClient(main_module.app) as client:
            response = client.get("/openapi.json")
        self.assertEqual(response.status_code, 200)
        self.assertIn("paths", response.json())

    def test_root_without_loading_chroma(self):
        with patch(
            "chroma.get_collection_stats",
            return_value={"total_articles": 0, "collection": "signal_articles"},
        ):
            with TestClient(main_module.app) as client:
                response = client.get("/")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "Othello V2 API is running")
        self.assertIn("runtime", body)
        self.assertEqual(body["collection"]["total_articles"], 0)

    def test_coverage_map_hotspot_shape(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now_ts = time.time()
        upsert_structured_events(
            [
                {
                    "event_id": "smoke-map-a",
                    "dataset": "acled",
                    "dataset_event_id": "smoke-map-a",
                    "event_date": today,
                    "country": "Ukraine",
                    "region": None,
                    "admin1": "Donetsk",
                    "admin2": None,
                    "location": "Bakhmut",
                    "latitude": 48.595,
                    "longitude": 38.002,
                    "event_type": "Battles",
                    "sub_event_type": "Armed clash",
                    "actor_primary": "Force A",
                    "actor_secondary": "Force B",
                    "fatalities": 1,
                    "source_count": 2,
                    "source_urls": [],
                    "summary": "Smoke test incident note for map clustering.",
                    "payload": {},
                    "first_ingested_at": now_ts,
                    "last_ingested_at": now_ts,
                },
                {
                    "event_id": "smoke-map-b",
                    "dataset": "acled",
                    "dataset_event_id": "smoke-map-b",
                    "event_date": today,
                    "country": "Ukraine",
                    "region": None,
                    "admin1": "Donetsk",
                    "admin2": None,
                    "location": "Bakhmut",
                    "latitude": 48.596,
                    "longitude": 38.003,
                    "event_type": "Battles",
                    "sub_event_type": "Armed clash",
                    "actor_primary": "Force A",
                    "actor_secondary": "Force C",
                    "fatalities": 0,
                    "source_count": 1,
                    "source_urls": [],
                    "summary": "",
                    "payload": {},
                    "first_ingested_at": now_ts,
                    "last_ingested_at": now_ts,
                },
            ]
        )
        main_module._MAP_ATTENTION_CACHE.clear()
        main_module._STORY_LOCATION_INDEX_CACHE.clear()

        allowed_aspects = {"conflict", "political", "economic"}
        with TestClient(main_module.app) as client:
            response = client.get("/coverage/map?window=7d")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("hotspots", data)
        self.assertIsInstance(data["hotspots"], list)
        self.assertGreater(len(data["hotspots"]), 0, "seeded structured events should yield at least one hotspot")
        for h in data["hotspots"]:
            self.assertTrue(str(h.get("label") or "").strip(), "hotspot.label must be non-empty")
            self.assertIsInstance(h.get("sample_events"), list)
            self.assertGreaterEqual(len(h["sample_events"]), 1)
            self.assertIn(h.get("aspect"), allowed_aspects)
            lat, lon = h.get("latitude"), h.get("longitude")
            self.assertIsInstance(lat, (int, float))
            self.assertIsInstance(lon, (int, float))
            self.assertTrue(-90 <= float(lat) <= 90)
            self.assertTrue(-180 <= float(lon) <= 180)
            for ev in h["sample_events"]:
                self.assertTrue(
                    str(ev.get("summary") or ev.get("title") or "").strip(),
                    "each sample_event needs summary or title",
                )


if __name__ == "__main__":
    unittest.main()
