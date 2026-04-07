"""Compatibility shim: re-export source ingestion functions from package.

The real implementation now lives in `backend/sources/source_ingestion.py`.
This module re-exports to preserve top-level imports during migration.
"""

try:
    from sources.source_ingestion import *  # type: ignore
except Exception:
    from backend.sources.source_ingestion import *  # type: ignore

__all__ = [name for name in globals().keys() if not name.startswith("_")]
