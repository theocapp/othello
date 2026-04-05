from chroma import get_collection_stats
from fastapi import HTTPException
from pydantic import BaseModel


class QueryRequest(BaseModel):
    question: str
    topic: str | None = None
    region_context: str | None = None
    hotspot_id: str | None = None
    story_event_id: str | None = None
    source_urls: list[str] | None = None
    attention_window: str | None = None


def query_payload(request: QueryRequest):
    from analyst import answer_query
    from main import REQUEST_ENABLE_LLM_RESPONSES, _gather_query_articles, _normalize_query_corpus_topic, _query_fallback
    import os
    corpus_topic = _normalize_query_corpus_topic(request.topic)
    combined, meta = _gather_query_articles(request.question, topic=corpus_topic, limit=12, region_context=request.region_context, hotspot_id=request.hotspot_id, story_event_id=request.story_event_id, source_urls=request.source_urls, attention_window=request.attention_window)
    if not combined:
        raise HTTPException(status_code=404, detail="No relevant reporting found in the corpus")
    if REQUEST_ENABLE_LLM_RESPONSES and os.getenv("GROQ_API_KEY"):
        try:
            answer = answer_query(request.question, combined, topic=meta["topic"])
        except Exception:
            answer = _query_fallback(request.question, combined, topic=meta["topic"])
    else:
        answer = _query_fallback(request.question, combined, topic=meta["topic"])
    return {"question": request.question, "answer": answer, "sources": combined, "source_count": len(combined), "historical_sources": meta["historical_sources"], "live_sources": meta["live_sources"], "topic": meta["topic"] or request.topic}


def timeline_payload(query: str):
    from analyst import build_timeline
    from main import REQUEST_ENABLE_LLM_RESPONSES, _gather_query_articles, _timeline_fallback
    import os
    articles, meta = _gather_query_articles(query, limit=18)
    if not articles:
        raise HTTPException(status_code=404, detail="No articles found for this topic")
    if not REQUEST_ENABLE_LLM_RESPONSES or not os.getenv("GROQ_API_KEY"):
        return _timeline_fallback(query, articles)
    try:
        return build_timeline(query, articles)
    except Exception:
        return _timeline_fallback(query, articles)


def chroma_stats_payload():
    return get_collection_stats()
