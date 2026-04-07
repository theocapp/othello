"""Sources domain package.

Re-exports legacy source and catalog modules.
"""

try:
	from .. import source_catalog  # type: ignore
	from .. import source_ingestion  # type: ignore
	__all__ = ["source_catalog", "source_ingestion"]
except Exception:
	import source_catalog  # type: ignore
	import source_ingestion  # type: ignore
	__all__ = ["source_catalog", "source_ingestion"]
