from fastapi import APIRouter, Depends

from api.deps import require_write_access_dep
from services.briefing_service import cache_status_payload, force_refresh_payload
from services.headlines_service import get_headlines_payload

router = APIRouter()


@router.get("/headlines")
def get_headlines(sort_by: str = "relevance", region: str | None = None):
    return get_headlines_payload(sort_by=sort_by, region=region)


@router.get("/cache/status")
def cache_status():
    return cache_status_payload()


@router.post("/cache/refresh")
def force_refresh(
    topic: str | None = None, _: None = Depends(require_write_access_dep)
):
    return force_refresh_payload(topic)
