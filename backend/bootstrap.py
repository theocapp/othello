from cache import init_db as init_cache_db
from corpus import init_db as init_corpus_db
from entities import init_db as init_entities_db
from bootstrap_sources import seed_sources
import core.runtime as runtime

# Expose legacy symbol for tests and callers that patch `bootstrap.runtime_status`
runtime_status = runtime.runtime_status
from services.ingest_service import bootstrap_from_legacy_cache  # noqa: E402


def initialize_runtime() -> dict:
    # Initialize structured logging early so subsequent imports/logs are structured
    try:
        runtime.init_logging()
    except Exception:
        # Safe fallback if logging initialization fails
        pass

    init_cache_db()
    init_corpus_db()
    init_entities_db()
    seed_sources()
    state = runtime_status()
    if state["corpus"]["total_articles"] == 0:
        bootstrap_from_legacy_cache()
        state = runtime_status()
    return state
