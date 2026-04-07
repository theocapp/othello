"""Sources domain package.

Re-exports legacy source and catalog modules.
"""

from __future__ import annotations

__all__ = ["source_catalog", "source_ingestion"]


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(name)

    import importlib

    module = importlib.import_module(f"{__name__}.{name}")
    globals()[name] = module
    return module
