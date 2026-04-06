"""Shared mutable state for map / hotspot cache layers."""

MAP_ATTENTION_CACHE: dict[str, tuple[float, dict]] = {}
STORY_LOCATION_INDEX_CACHE: dict[int, tuple[float, dict[str, dict]]] = {}


def clear_map_caches() -> None:
    MAP_ATTENTION_CACHE.clear()
    STORY_LOCATION_INDEX_CACHE.clear()
