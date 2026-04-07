"""Storage domain package.

Re-exports storage/cache and corpus initialization helpers.
"""

try:
	from .. import cache  # type: ignore
	from .. import corpus  # type: ignore
	__all__ = ["cache", "corpus"]
except Exception:
	import cache  # type: ignore
	import corpus  # type: ignore
	__all__ = ["cache", "corpus"]
