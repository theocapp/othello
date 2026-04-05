"""
Tests for the memory-reduction changes to the ingestion pipeline:
1. spaCy model priority (sm before lg)
2. ChromaDB batching
3. Tier 2 article_summaries table + upsert_article_summaries
4. Tier 2 wiring in ingest_topic / ingest_global (via should_promote_article gate)
"""

import sqlite3
import tempfile
import os
import sys
import time
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

# ── Path setup ───────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))


# ─────────────────────────────────────────────────────────────────────────────
# 1. spaCy model priority
# ─────────────────────────────────────────────────────────────────────────────
class TestSpacyModelPriority(unittest.TestCase):

    def test_english_candidates_start_with_sm(self):
        """English model list must try sm before md/lg."""
        from entities import LANGUAGE_MODEL_CANDIDATES
        en = LANGUAGE_MODEL_CANDIDATES["en"]
        self.assertEqual(en[0], "en_core_web_sm",
            "en_core_web_sm should be first candidate to minimise RAM usage")

    def test_english_sm_loads(self):
        """en_core_web_sm must be importable and functional."""
        import spacy
        nlp = spacy.load("en_core_web_sm")
        doc = nlp("NATO and Russia clashed over Ukraine.")
        ents = [e.text for e in doc.ents]
        self.assertTrue(len(ents) > 0, "en_core_web_sm should extract at least one entity")

    def test_resolve_model_name_returns_sm_when_available(self):
        """_resolve_model_name should return en_core_web_sm on this machine."""
        # Clear cache so we get a fresh resolution
        import entities
        original_cache = dict(entities._MODEL_NAME_CACHE)
        entities._MODEL_NAME_CACHE.clear()
        entities._NLP_CACHE.clear()
        try:
            name = entities._resolve_model_name("en")
            self.assertEqual(name, "en_core_web_sm",
                "Should resolve to sm since it's installed and listed first")
        finally:
            entities._MODEL_NAME_CACHE.update(original_cache)

    def test_fallback_extension_uses_sm_first(self):
        """_candidate_models fallback for non-English should prepend sm before lg."""
        from entities import _candidate_models
        # Use a language with no native model to trigger english fallback
        candidates = _candidate_models("xx_fake_language", include_english_fallback=True)
        en_candidates = [c for c in candidates if c.startswith("en_core_web")]
        if en_candidates:
            self.assertEqual(en_candidates[0], "en_core_web_sm",
                "English fallback should try sm first")


# ─────────────────────────────────────────────────────────────────────────────
# 2. ChromaDB batching
# ─────────────────────────────────────────────────────────────────────────────
class TestChromaBatching(unittest.TestCase):

    def _make_articles(self, n):
        return [
            {
                "url": f"https://example.com/article-{i}",
                "title": f"Test Article {i}",
                "description": f"Description for article {i}",
                "source": "Test Source",
                "published_at": datetime.now(timezone.utc).isoformat(),
            }
            for i in range(n)
        ]

    def test_batch_size_constant_exists(self):
        import chroma
        self.assertTrue(hasattr(chroma, "_CHROMA_BATCH_SIZE"),
            "_CHROMA_BATCH_SIZE constant should exist")
        self.assertEqual(chroma._CHROMA_BATCH_SIZE, 20)

    def test_large_batch_splits_into_chunks(self):
        """50 articles should result in 3 upsert calls (20+20+10)."""
        import chroma
        upsert_calls = []

        mock_collection = MagicMock()
        def capture_upsert(**kwargs):
            upsert_calls.append(len(kwargs["documents"]))
        mock_collection.upsert.side_effect = capture_upsert

        with patch.object(chroma, "get_collection", return_value=mock_collection):
            chroma.store_articles(self._make_articles(50), topic="geopolitics")

        self.assertEqual(upsert_calls, [20, 20, 10],
            "50 articles should split into batches of 20, 20, 10")

    def test_small_batch_single_call(self):
        """5 articles should result in exactly 1 upsert call."""
        import chroma
        upsert_calls = []

        mock_collection = MagicMock()
        mock_collection.upsert.side_effect = lambda **kw: upsert_calls.append(len(kw["documents"]))

        with patch.object(chroma, "get_collection", return_value=mock_collection):
            chroma.store_articles(self._make_articles(5), topic="economics")

        self.assertEqual(len(upsert_calls), 1)
        self.assertEqual(upsert_calls[0], 5)

    def test_empty_articles_no_call(self):
        """Empty list should result in zero upsert calls."""
        import chroma
        mock_collection = MagicMock()

        with patch.object(chroma, "get_collection", return_value=mock_collection):
            chroma.store_articles([], topic="geopolitics")

        mock_collection.upsert.assert_not_called()

    def test_batch_error_stops_remaining_batches(self):
        """An error mid-batch should stop further batches (fail fast)."""
        import chroma
        call_count = [0]

        mock_collection = MagicMock()
        def raise_on_second(**kwargs):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise RuntimeError("simulated chroma failure")
        mock_collection.upsert.side_effect = raise_on_second

        with patch.object(chroma, "get_collection", return_value=mock_collection):
            # Should not raise — errors are caught internally
            chroma.store_articles(self._make_articles(50), topic="geopolitics")

        self.assertEqual(call_count[0], 2,
            "Should stop after the first failed batch")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Tier 2: article_summaries table + upsert_article_summaries
# ─────────────────────────────────────────────────────────────────────────────
class TestArticleSummaries(unittest.TestCase):

    def setUp(self):
        """Use a temp SQLite DB for each test."""
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.tmp.name
        self.tmp.close()

        import corpus
        self._orig_sqlite_path = corpus.SQLITE_DB_PATH

        # Patch both the module-level constant and the connect helper
        corpus.SQLITE_DB_PATH = self.db_path
        self._corpus = corpus

        # Ensure we use SQLite (not postgres) for tests
        self._orig_database_url = corpus._database_url
        corpus._database_url = lambda: None

        corpus.init_db()

    def tearDown(self):
        self._corpus.SQLITE_DB_PATH = self._orig_sqlite_path
        self._corpus._database_url = self._orig_database_url
        os.unlink(self.db_path)

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_article_summaries_table_created(self):
        """init_db() should create article_summaries table."""
        conn = self._conn()
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        self.assertIn("article_summaries", tables)

    def test_article_summaries_schema(self):
        """article_summaries should have the expected columns."""
        conn = self._conn()
        cols = {r[1] for r in conn.execute("PRAGMA table_info(article_summaries)").fetchall()}
        conn.close()
        expected = {"url", "title", "source", "source_domain", "published_at",
                    "topic", "quality_score", "first_seen_at"}
        self.assertEqual(expected, cols)

    def test_upsert_article_summaries_basic(self):
        """upsert_article_summaries should insert rows correctly."""
        articles = [
            {
                "url": "https://example.com/story-1",
                "title": "Low signal story",
                "source": "Example News",
                "source_domain": "example.com",
                "published_at": "2024-01-01T12:00:00Z",
            }
        ]
        inserted = self._corpus.upsert_article_summaries(
            articles, topic="geopolitics", quality_scores={"https://example.com/story-1": 3}
        )
        self.assertEqual(inserted, 1)

        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM article_summaries WHERE url = ?",
            ("https://example.com/story-1",)
        ).fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row["title"], "Low signal story")
        self.assertEqual(row["source"], "Example News")
        self.assertEqual(row["topic"], "geopolitics")
        self.assertEqual(row["quality_score"], 3)

    def test_upsert_article_summaries_no_duplicate(self):
        """Upserting the same URL twice should not create duplicates."""
        article = [{
            "url": "https://example.com/dup",
            "title": "Duplicate article",
            "source": "Source",
            "published_at": "2024-01-01T00:00:00Z",
        }]
        self._corpus.upsert_article_summaries(article, topic="economics")
        inserted2 = self._corpus.upsert_article_summaries(article, topic="economics")
        self.assertEqual(inserted2, 0, "Second upsert of same URL should insert 0 rows")

        conn = self._conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM article_summaries WHERE url = ?",
            ("https://example.com/dup",)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)

    def test_upsert_article_summaries_skips_missing_url(self):
        """Articles without a url should be skipped."""
        articles = [{"title": "No URL article", "source": "src", "published_at": "2024-01-01T00:00:00Z"}]
        inserted = self._corpus.upsert_article_summaries(articles, topic="economics")
        self.assertEqual(inserted, 0)

    def test_upsert_article_summaries_batch(self):
        """Multiple articles should all be inserted."""
        articles = [
            {
                "url": f"https://example.com/batch-{i}",
                "title": f"Batch article {i}",
                "source": "Source",
                "published_at": "2024-01-01T00:00:00Z",
            }
            for i in range(10)
        ]
        inserted = self._corpus.upsert_article_summaries(articles, topic="geopolitics")
        self.assertEqual(inserted, 10)

        conn = self._conn()
        count = conn.execute("SELECT COUNT(*) FROM article_summaries").fetchone()[0]
        conn.close()
        self.assertEqual(count, 10)

    def test_upsert_article_summaries_empty(self):
        """Empty list should return 0 and not error."""
        inserted = self._corpus.upsert_article_summaries([], topic="geopolitics")
        self.assertEqual(inserted, 0)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Scheduler: max_instances=1 on all jobs
# ─────────────────────────────────────────────────────────────────────────────
class TestSchedulerConcurrencyLock(unittest.TestCase):

    def test_all_jobs_have_max_instances_1(self):
        """Every scheduled job should have max_instances=1 to prevent overlapping runs."""
        import main
        scheduler = main.build_scheduler()
        scheduler.start()
        try:
            jobs = scheduler.get_jobs()
            self.assertGreater(len(jobs), 0, "Scheduler should have jobs")
            for job in jobs:
                self.assertEqual(
                    job.max_instances, 1,
                    f"Job '{job.id}' should have max_instances=1, got {job.max_instances}",
                )
        finally:
            scheduler.shutdown(wait=False)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Translation model LRU eviction
# ─────────────────────────────────────────────────────────────────────────────
class TestTranslationModelLRU(unittest.TestCase):

    def setUp(self):
        import analyst
        # Save originals
        self._orig_pipelines = dict(analyst._translation_pipelines)
        self._orig_order = list(analyst._translation_pipeline_order)
        self._orig_max = analyst._TRANSLATION_PIPELINE_MAX
        analyst._translation_pipelines.clear()
        analyst._translation_pipeline_order.clear()
        analyst._TRANSLATION_PIPELINE_MAX = 2

    def tearDown(self):
        import analyst
        analyst._translation_pipelines.clear()
        analyst._translation_pipelines.update(self._orig_pipelines)
        analyst._translation_pipeline_order.clear()
        analyst._translation_pipeline_order.extend(self._orig_order)
        analyst._TRANSLATION_PIPELINE_MAX = self._orig_max

    def _inject_fake_model(self, lang):
        """Insert a fake bundle into the cache without loading real models."""
        import analyst
        bundle = {"tokenizer": MagicMock(), "model": MagicMock()}
        analyst._translation_pipelines[lang] = bundle
        analyst._translation_pipeline_order.append(lang)
        return bundle

    def test_max_pipeline_constant_exists(self):
        import analyst
        self.assertTrue(hasattr(analyst, "_TRANSLATION_PIPELINE_MAX"))
        self.assertGreater(analyst._TRANSLATION_PIPELINE_MAX, 0)

    def test_lru_order_tracking(self):
        """Loading a cached model should move it to end of LRU order."""
        import analyst
        self._inject_fake_model("fr")
        self._inject_fake_model("de")
        self.assertEqual(analyst._translation_pipeline_order, ["fr", "de"])

        # Access fr again — should move to end
        analyst._load_local_translation_pipeline("fr")
        self.assertEqual(analyst._translation_pipeline_order[-1], "fr",
            "Accessing 'fr' again should move it to most-recently-used position")

    def test_eviction_on_capacity(self):
        """Loading a 3rd model when max=2 should evict the oldest (LRU)."""
        import analyst

        self._inject_fake_model("fr")
        self._inject_fake_model("de")
        self.assertEqual(len(analyst._translation_pipelines), 2)

        # Mock the model loading so no real download happens
        fake_tokenizer = MagicMock()
        fake_model = MagicMock()
        with patch("transformers.AutoTokenizer.from_pretrained", return_value=fake_tokenizer), \
             patch("transformers.AutoModelForSeq2SeqLM.from_pretrained", return_value=fake_model):
            analyst._load_local_translation_pipeline("uk")

        self.assertEqual(len(analyst._translation_pipelines), 2,
            "Cache should stay at max=2 after eviction")
        self.assertNotIn("fr", analyst._translation_pipelines,
            "Oldest model ('fr') should have been evicted")
        self.assertIn("de", analyst._translation_pipelines)
        self.assertIn("uk", analyst._translation_pipelines)

    def test_evict_removes_from_order(self):
        """Evicting a model should remove it from the LRU order list."""
        import analyst
        self._inject_fake_model("fr")
        analyst._evict_translation_pipeline("fr")
        self.assertNotIn("fr", analyst._translation_pipeline_order)
        self.assertNotIn("fr", analyst._translation_pipelines)

    def test_no_eviction_under_capacity(self):
        """Loading a model when under capacity should not evict anything."""
        import analyst
        self._inject_fake_model("fr")
        self.assertEqual(len(analyst._translation_pipelines), 1)

        fake_tokenizer = MagicMock()
        fake_model = MagicMock()
        with patch("transformers.AutoTokenizer.from_pretrained", return_value=fake_tokenizer), \
             patch("transformers.AutoModelForSeq2SeqLM.from_pretrained", return_value=fake_model):
            analyst._load_local_translation_pipeline("de")

        self.assertIn("fr", analyst._translation_pipelines,
            "'fr' should NOT be evicted when under capacity")
        self.assertIn("de", analyst._translation_pipelines)


if __name__ == "__main__":
    unittest.main(verbosity=2)
