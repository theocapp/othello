from fastapi import APIRouter

from services.analytics_service import get_health_payload, get_root_payload, get_system_overview_payload

router = APIRouter()


@router.get("/")
def root():
    return get_root_payload()


@router.get("/health")
def health():
    return get_health_payload()


@router.get("/system/overview")
def system_overview():
    return get_system_overview_payload()
