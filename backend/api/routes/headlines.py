from fastapi import APIRouter, Depends, Header, Request

from services.briefing_service import cache_status_payload, force_refresh_payload
from services.headlines_service import get_headlines_payload

router = APIRouter()


def require_write_access_dep(request: Request, x_api_key: str | None = Header(default=None, alias="X-API-Key")):
    from main import require_write_access
    return require_write_access(request, x_api_key)


@router.get("/headlines")
def get_headlines(sort_by: str = "relevance", region: str | None = None):
    return get_headlines_payload(sort_by=sort_by, region=region)


@router.get("/cache/status")
def cache_status():
    return cache_status_payload()


@router.post("/cache/refresh")
def force_refresh(topic: str | None = None, _: None = Depends(require_write_access_dep)):
    return force_refresh_payload(topic)
