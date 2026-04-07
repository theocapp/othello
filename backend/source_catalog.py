"""Compatibility shim: re-export source catalog from backend.sources.

Implementation moved to `backend/sources/source_catalog.py` during
package migration. Keep this shim to preserve top-level imports.
"""

try:
    from sources.source_catalog import *  # type: ignore
except Exception:
    from backend.sources.source_catalog import *  # type: ignore

__all__ = [name for name in globals().keys() if not name.startswith("_")]
