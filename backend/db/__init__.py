"""Database subpackage for repository separation.

This package contains split-out repository modules extracted from the
monolithic `backend/corpus.py` to improve separation of responsibilities.
"""

__all__ = [
    "common",
    "schema",
    "articles_repo",
    "sources_repo",
    "events_repo",
    "analytics_repo",
    "predictions_repo",
]
