"""Backend domain package.

This file makes the `backend` directory a Python package so we can
introduce domain subpackages (ingestion, entities, foresight, sources,
runtime, storage, intel) while remaining backwards compatible with
existing top-level modules during an incremental refactor.
"""

__all__ = []
