"""Compatibility shim: re-export ingestion submodule.

The real implementation lives in `backend/ingestion/acled_ingestion.py`.
This module is a thin wrapper to preserve existing top-level imports
while the codebase migrates to a domain package layout.
"""

from ingestion.acled_ingestion import *  # noqa: F401,F403

__all__ = [name for name in globals().keys() if not name.startswith("_")]
