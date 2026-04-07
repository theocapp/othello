"""Runtime package.

Hosts runtime-related utilities and re-exports where appropriate.
"""

try:
	from .. import bootstrap  # type: ignore
	import core.runtime as runtime  # type: ignore
	__all__ = ["bootstrap", "runtime"]
except Exception:
	import bootstrap  # type: ignore
	import core.runtime as runtime  # type: ignore
	__all__ = ["bootstrap", "runtime"]
