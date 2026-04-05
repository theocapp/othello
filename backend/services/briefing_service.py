from fastapi import HTTPException


def get_briefing_payload(topic: str):
    from main import build_topic_briefing
    result = build_topic_briefing(topic)
    if not result:
        raise HTTPException(status_code=503, detail="No article corpus available for this topic yet")
    return {**result, "cached": True}


def cache_status_payload():
    from cache import load_briefing
    from corpus import get_article_count
    from main import BRIEFING_TOPICS, BRIEFING_TTL, _corpus_topic_for_briefing
    import time
    result = {}
    for topic in BRIEFING_TOPICS:
        cached = load_briefing(topic, ttl=BRIEFING_TTL)
        corpus_topic = _corpus_topic_for_briefing(topic)
        result[topic] = {
            "cached": cached is not None,
            "age_minutes": int((time.time() - cached["generated_at"]) / 60) if cached else None,
            "article_count": cached["article_count"] if cached else 0,
            "event_count": len(cached.get("events", [])) if cached else 0,
            "corpus_articles_72h": get_article_count(topic=corpus_topic, hours=72),
        }
    return result


def force_refresh_payload(topic: str | None = None):
    from fastapi import HTTPException
    from main import BRIEFING_TOPICS, TOPICS, build_topic_briefing, rebuild_headlines_cache, refresh_snapshot_layer
    if topic:
        if topic not in BRIEFING_TOPICS:
            raise HTTPException(status_code=400, detail=f"Topic must be one of {BRIEFING_TOPICS}")
        result = build_topic_briefing(topic, force_refresh=True)
        rebuild_headlines_cache()
        return {"refreshed": [topic], "success": result is not None}
    refresh_snapshot_layer()
    return {"refreshed": TOPICS, "success": True}


def get_predictions_payload(topic: str | None = None, refresh: bool = False, limit: int = 100):
    from foresight import load_prediction_ledger
    return load_prediction_ledger(topic=topic, refresh=refresh, limit=max(1, min(limit, 300)))


def get_before_news_archive_payload(limit: int = 50, minimum_gap_hours: int = 0):
    from foresight import load_early_signal_archive
    return load_early_signal_archive(limit=max(1, min(limit, 200)), minimum_gap_hours=max(0, minimum_gap_hours))
