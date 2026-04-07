"""Shallow compatibility shim.

This module re-exports the split repositories under `db.*` so existing
callers that import ``corpus`` keep working while the implementation is
now organized under ``backend/db/``.

Do not add new logic here — new code should go into the `db` package.
"""

from pathlib import Path
import os
from dotenv import load_dotenv

# Preserve prior behavior: load backend/.env if present
import warnings

# Preserve prior behavior: load backend/.env if present
load_dotenv(Path(__file__).resolve().with_name(".env"), override=False)

# Emit a gentle deprecation notice for callers still importing corpus.
# Prefer importing from the new db package instead of this compatibility shim.
if os.getenv("OTHELLO_CORPUS_SHIM_WARN", "false").strip().lower() == "true":
    warnings.warn(
        "The 'corpus' module is a compatibility shim and is deprecated; import from the 'db' package instead. This shim will be removed in a future release.",
        FutureWarning,
        stacklevel=2,
    )

# Re-export repository modules for back-compat (public API)
from db.common import *  # noqa: F401,F403,E402
from db.schema import *  # noqa: F401,F403,E402
from db.articles_repo import *  # noqa: F401,F403,E402
from db.sources_repo import *  # noqa: F401,F403,E402
from db.events_repo import *  # noqa: F401,F403,E402
from db.analytics_repo import *  # noqa: F401,F403,E402
from db.predictions_repo import *  # noqa: F401,F403,E402

# Also explicitly expose certain legacy _prefixed helpers that
# `from corpus import ...` and direct `corpus._...` call sites rely on.
import db.common as _db_common  # noqa: E402
import db.articles_repo as _db_articles_repo  # noqa: E402

# Common helpers
_connect = _db_common._connect
_coerce_timestamptz = _db_common._coerce_timestamptz
_parse_article_timestamp = _db_common._parse_article_timestamp
_row_to_article = _db_common._row_to_article
_canonical_url = _db_common._canonical_url
_domain = _db_common._domain
_content_hash = _db_common._content_hash
_headline_corpus_sql_filter = _db_common._headline_corpus_sql_filter

# Articles helpers
_bulk_upsert_articles_pg = _db_articles_repo._bulk_upsert_articles_pg
_normalize_article = _db_articles_repo._normalize_article
_topic_time_bounds_python = _db_articles_repo._topic_time_bounds_python
