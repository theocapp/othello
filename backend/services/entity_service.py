from fastapi import HTTPException

from core.config import TOPICS


def entity_signals_payload(topic: str):
    from entities import get_entity_frequencies, get_top_entities
    if topic not in TOPICS:
        raise HTTPException(status_code=400, detail=f"Topic must be one of {TOPICS}")
    return {"topic": topic, "spikes": get_entity_frequencies(topic=topic), "top_entities": get_top_entities(topic=topic)}


def all_entity_signals_payload():
    from entities import get_entity_frequencies, get_top_entities
    return {"spikes": get_entity_frequencies(), "top_entities": get_top_entities()}


def entity_relationships_payload(entity: str, days: int = 7):
    from entities import get_entity_relationships
    return {"entity": entity, "related": get_entity_relationships(entity, days=days)}


def entity_reference_payload(entity: str, refresh: bool = False):
    from corpus import load_entity_reference, save_entity_reference
    from wikipedia_reference import fetch_wikipedia_reference
    fresh_cache = None if refresh else load_entity_reference(entity, provider="wikipedia", max_age_hours=24 * 14)
    if fresh_cache:
        return {**fresh_cache, "cached": True, "stale": False}
    stale_cache = load_entity_reference(entity, provider="wikipedia", max_age_hours=None)
    try:
        reference = fetch_wikipedia_reference(entity)
        save_entity_reference(entity=entity, provider="wikipedia", reference=reference, status=reference.get("status", "ok"), error=None)
        return {**reference, "cached": False, "stale": False}
    except Exception as exc:
        if stale_cache:
            return {**stale_cache, "cached": True, "stale": True, "warning": "Using cached Wikipedia reference because the live lookup failed."}
        raise HTTPException(status_code=502, detail=f"Wikipedia reference unavailable for '{entity}': {exc}")


def entity_graph_payload(days: int = 7, min_cooccurrences: int = 2, topic: str | None = None):
    from entities import get_relationship_graph
    return get_relationship_graph(days=days, min_cooccurrences=min_cooccurrences, topic=topic)
