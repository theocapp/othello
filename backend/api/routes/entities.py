from fastapi import APIRouter

from services.entity_service import (
    all_entity_signals_payload,
    entity_graph_payload,
    entity_reference_payload,
    entity_relationships_payload,
    entity_signals_payload,
)

router = APIRouter()


@router.get("/entities/signals/{topic}")
def entity_signals(topic: str):
    return entity_signals_payload(topic)


@router.get("/entities/signals")
def all_entity_signals():
    return all_entity_signals_payload()


@router.get("/entities/relationships/{entity}")
def entity_relationships(entity: str, days: int = 7):
    return entity_relationships_payload(entity, days)


@router.get("/entities/reference/{entity}")
def entity_reference(entity: str, refresh: bool = False):
    return entity_reference_payload(entity, refresh)


@router.get("/entities/graph")
def entity_graph(days: int = 7, min_cooccurrences: int = 2, topic: str | None = None):
    return entity_graph_payload(days, min_cooccurrences, topic)
