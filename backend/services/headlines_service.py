from fastapi import HTTPException


def get_headlines_payload(sort_by: str = "relevance", region: str | None = None):
    from cache import load_headlines
    from main import HEADLINES_TTL, _available_story_regions, _sort_headline_stories, _standardize_headline_story, rebuild_headlines_cache

    cached = load_headlines(ttl=HEADLINES_TTL)
    if cached:
        normalized = [_standardize_headline_story(story) for story in cached]
        return {
            "stories": _sort_headline_stories(normalized, sort_by=sort_by, region=region),
            "available_regions": _available_story_regions(normalized),
            "sort_by": sort_by,
            "region": region or "all",
        }

    stories = rebuild_headlines_cache(use_llm=False)
    if not stories:
        raise HTTPException(status_code=503, detail="No article corpus available yet")
    return {
        "stories": _sort_headline_stories(stories, sort_by=sort_by, region=region),
        "available_regions": _available_story_regions(stories),
        "sort_by": sort_by,
        "region": region or "all",
    }
