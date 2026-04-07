"""Tiered in-memory cache with stampede protection.

Replaces scattered ad-hoc caches with a unified system:
  - Named cache entries with configurable TTL tiers
  - Stampede protection: only one caller computes a stale entry, others get stale data
  - Automatic stats tracking per key (hits, misses, avg compute time)
  - Stale-while-revalidate: return stale data while fresh data computes

TTL Tiers (inspired by WorldMonitor):
  fast:   5 min   — live event streams, headlines
  medium: 10 min  — map attention, instability, correlations
  slow:   30 min  — briefings-adjacent, source reliability
  static: 2 hr    — reference data, historical
  daily:  24 hr   — constants, registry snapshots
"""

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

# ── TTL tiers ────────────────────────────────────────────────────────────────

TTL_FAST = 300  # 5 min
TTL_MEDIUM = 600  # 10 min
TTL_SLOW = 1800  # 30 min
TTL_STATIC = 7200  # 2 hr
TTL_DAILY = 86400  # 24 hr


@dataclass
class CacheEntry:
    value: Any = None
    created_at: float = 0.0
    ttl: float = TTL_MEDIUM
    computing: bool = False
    hits: int = 0
    misses: int = 0
    last_compute_ms: float = 0.0


class TieredCache:
    """Thread-safe in-memory cache with stampede protection."""

    def __init__(self):
        self._entries: dict[str, CacheEntry] = {}
        self._lock = threading.Lock()

    def get(
        self, key: str, compute_fn: Callable[[], Any], ttl: float = TTL_MEDIUM
    ) -> Any:
        """Get a cached value, computing it if stale or missing.

        Stampede protection: if the entry is stale and another thread is already
        recomputing, return the stale value instead of blocking.
        """
        now = time.time()

        with self._lock:
            entry = self._entries.get(key)

            # Fresh hit
            if entry and (now - entry.created_at) < entry.ttl:
                entry.hits += 1
                return entry.value

            # Stale or missing
            if entry is None:
                entry = CacheEntry(ttl=ttl)
                self._entries[key] = entry

            # Another thread is computing — return stale data if available
            if entry.computing and entry.value is not None:
                entry.hits += 1
                return entry.value

            # We'll be the one to compute
            entry.computing = True
            entry.misses += 1

        # Compute outside the lock
        start = time.time()
        try:
            value = compute_fn()
        except Exception:
            with self._lock:
                entry.computing = False
            raise

        elapsed_ms = (time.time() - start) * 1000

        with self._lock:
            entry.value = value
            entry.created_at = time.time()
            entry.ttl = ttl
            entry.computing = False
            entry.last_compute_ms = elapsed_ms

        return value

    def invalidate(self, key: str):
        """Force invalidation of a cache entry."""
        with self._lock:
            if key in self._entries:
                self._entries[key].created_at = 0.0

    def invalidate_prefix(self, prefix: str):
        """Invalidate all entries matching a key prefix."""
        with self._lock:
            for key, entry in self._entries.items():
                if key.startswith(prefix):
                    entry.created_at = 0.0

    def clear(self):
        """Clear all entries."""
        with self._lock:
            self._entries.clear()

    def stats(self) -> list[dict]:
        """Return stats for all cache entries."""
        with self._lock:
            now = time.time()
            result = []
            for key, entry in self._entries.items():
                age = now - entry.created_at if entry.created_at > 0 else None
                result.append(
                    {
                        "key": key,
                        "ttl": entry.ttl,
                        "age_seconds": round(age, 1) if age is not None else None,
                        "fresh": age is not None and age < entry.ttl,
                        "computing": entry.computing,
                        "hits": entry.hits,
                        "misses": entry.misses,
                        "hit_rate": round(
                            entry.hits / max(entry.hits + entry.misses, 1), 3
                        ),
                        "last_compute_ms": round(entry.last_compute_ms, 1),
                    }
                )
            return sorted(result, key=lambda r: r["key"])


# ── Singleton ────────────────────────────────────────────────────────────────

cache = TieredCache()
