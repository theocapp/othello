from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import bootstrap  # noqa: E402
import worker  # noqa: E402


class _FakeScheduler:
    def __init__(self) -> None:
        self.running = False
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.running = True
        self.started = True

    def shutdown(self) -> None:
        self.running = False
        self.stopped = True


class TestRuntimeWiring(unittest.TestCase):
    def test_initialize_runtime_bootstraps_when_corpus_empty(self):
        initial_state = {"corpus": {"total_articles": 0}}
        refreshed_state = {"corpus": {"total_articles": 5}}

        with patch.object(bootstrap, "init_cache_db") as init_cache_db, patch.object(
            bootstrap, "init_corpus_db"
        ) as init_corpus_db, patch.object(
            bootstrap, "seed_sources"
        ) as seed_sources, patch.object(
            bootstrap, "runtime_status", side_effect=[initial_state, refreshed_state]
        ) as runtime_status, patch.object(
            bootstrap, "bootstrap_from_legacy_cache"
        ) as bootstrap_from_legacy_cache:
            result = bootstrap.initialize_runtime()

        init_cache_db.assert_called_once_with()
        init_corpus_db.assert_called_once_with()
        seed_sources.assert_called_once_with()
        bootstrap_from_legacy_cache.assert_called_once_with()
        self.assertEqual(runtime_status.call_count, 2)
        self.assertEqual(result, refreshed_state)

    def test_initialize_runtime_skips_bootstrap_when_corpus_present(self):
        current_state = {"corpus": {"total_articles": 3}}

        with patch.object(bootstrap, "init_cache_db") as init_cache_db, patch.object(
            bootstrap, "init_corpus_db"
        ) as init_corpus_db, patch.object(
            bootstrap, "seed_sources"
        ) as seed_sources, patch.object(
            bootstrap, "runtime_status", return_value=current_state
        ) as runtime_status, patch.object(
            bootstrap, "bootstrap_from_legacy_cache"
        ) as bootstrap_from_legacy_cache:
            result = bootstrap.initialize_runtime()

        init_cache_db.assert_called_once_with()
        init_corpus_db.assert_called_once_with()
        seed_sources.assert_called_once_with()
        runtime_status.assert_called_once_with()
        bootstrap_from_legacy_cache.assert_not_called()
        self.assertEqual(result, current_state)

    def test_worker_initialize_worker_state_delegates_to_bootstrap(self):
        with patch.object(worker, "initialize_runtime") as initialize_runtime:
            worker.initialize_worker_state()
        initialize_runtime.assert_called_once_with()

    def test_worker_main_starts_and_shuts_down_scheduler_cleanly(self):
        scheduler = _FakeScheduler()

        with patch.object(
            worker, "initialize_worker_state"
        ) as initialize_worker_state, patch.object(
            worker, "build_worker_scheduler", return_value=scheduler
        ) as build_worker_scheduler, patch.object(
            worker, "WORKER_ENABLE_INGESTION", False
        ), patch.object(
            worker, "WORKER_ENABLE_TRANSLATIONS", False
        ), patch.object(
            worker, "WORKER_BOOTSTRAP_MODE", "none"
        ), patch(
            "worker.signal.signal"
        ), patch(
            "worker.time.sleep", side_effect=KeyboardInterrupt
        ), patch(
            "worker.sys.exit", side_effect=SystemExit(0)
        ):
            with self.assertRaises(SystemExit):
                worker.main()

        initialize_worker_state.assert_called_once_with()
        build_worker_scheduler.assert_called_once_with()
        self.assertTrue(scheduler.started)
        self.assertTrue(scheduler.stopped)


if __name__ == "__main__":
    unittest.main()
