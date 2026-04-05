from cache import init_db as init_cache_db
from corpus import init_db as init_corpus_db
from bootstrap_sources import seed_sources
from core.runtime import runtime_status
from services.ingest_service import bootstrap_from_legacy_cache


def initialize_runtime() -> dict:
    init_cache_db()
    init_corpus_db()
    seed_sources()
    state = runtime_status()
    if state["corpus"]["total_articles"] == 0:
        bootstrap_from_legacy_cache()
        state = runtime_status()
    return state
