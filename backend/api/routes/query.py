from fastapi import APIRouter

from services.query_service import (
    QueryRequest,
    chroma_stats_payload,
    query_payload,
    timeline_payload,
)

router = APIRouter()


@router.post("/query")
def query(request: QueryRequest):
    return query_payload(request)


@router.get("/timeline/{query}")
def get_timeline(query: str):
    return timeline_payload(query)


@router.post("/timeline")
def get_custom_timeline(request: QueryRequest):
    return timeline_payload(request.question)


@router.get("/chroma/stats")
def chroma_stats():
    return chroma_stats_payload()
