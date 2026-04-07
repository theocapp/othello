"""Ingestion domain package.

This package exposes ingestion-related submodules. During the
incremental refactor we prefer importing submodules from this package
instead of the legacy top-level modules.
"""

from . import (
    acled_ingestion,
    gdelt_gkg_ingestion,
    # official_ingestion,
    # source_ingestion,
    # import_articles,
    # ingest_gdelt,
    # fetch_historical_queue,
    # bootstrap_sources,
)

__all__ = ["acled_ingestion", "gdelt_gkg_ingestion"]
