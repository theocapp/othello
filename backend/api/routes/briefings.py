from fastapi import APIRouter

from services.briefing_service import (
    get_before_news_archive_payload,
    get_briefing_payload,
    get_predictions_payload,
)

router = APIRouter()


@router.get("/briefing/{topic}")
def get_briefing(topic: str):
    return get_briefing_payload(topic)


@router.get("/foresight/predictions")
def foresight_predictions(
    topic: str | None = None, refresh: bool = False, limit: int = 100
):
    return get_predictions_payload(topic=topic, refresh=refresh, limit=limit)


@router.get("/foresight/before-news")
def before_news_archive(limit: int = 50, minimum_gap_hours: int = 0):
    return get_before_news_archive_payload(
        limit=limit, minimum_gap_hours=minimum_gap_hours
    )
