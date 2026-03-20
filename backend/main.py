from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
import os
import time

load_dotenv()
from chroma import store_articles, search_articles, get_collection_stats
from news import fetch_articles, fetch_articles_for_query
from analyst import generate_briefing, answer_query
from entities import store_entity_mentions, format_signals_for_briefing
from contradictions import detect_contradictions

app = FastAPI(title="Signal Intelligence API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Cache ────────────────────────────────────────────────────────────────────
# Simple in-memory cache: { topic: { briefing, sources, article_count, generated_at } }
cache = {}
CACHE_TTL = 3600  # 1 hour in seconds

def is_cache_valid(topic: str) -> bool:
    if topic not in cache:
        return False
    age = time.time() - cache[topic]["generated_at"]
    return age < CACHE_TTL

def get_cached(topic: str) -> dict | None:
    if is_cache_valid(topic):
        return cache[topic]
    return None

def set_cache(topic: str, briefing: str, sources: list, article_count: int):
    cache[topic] = {
        "topic": topic,
        "briefing": briefing,
        "sources": sources,
        "article_count": article_count,
        "generated_at": time.time(),
    }
    print(f"[cache] Stored briefing for '{topic}'")

# ─── Briefing generation ──────────────────────────────────────────────────────
def generate_and_cache(topic: str):
    print(f"[briefing] Generating '{topic}'...")
    try:
        articles = fetch_articles(topic)
        if not articles:
            print(f"[briefing] No articles found for '{topic}'")
            return None

        # Store in ChromaDB
        store_articles(articles, topic)

        # Extract and store entities
        store_entity_mentions(articles, topic)

        # Get entity signals
        signals = format_signals_for_briefing(topic)

        # Detect contradictions across sources
        print(f"[contradictions] Analyzing '{topic}'...")
        contradictions = detect_contradictions(articles, topic)
        if contradictions:
            print(f"[contradictions] Found contradictions in '{topic}'")
        else:
            print(f"[contradictions] No contradictions in '{topic}'")

        # Generate briefing with all context
        briefing = generate_briefing(topic, articles, signals, contradictions)

        set_cache(topic, briefing, articles, len(articles))
        print(f"[briefing] Done '{topic}' ({len(articles)} articles)")
        return cache[topic]
    except Exception as e:
        print(f"[briefing] Error generating '{topic}': {e}")
        return None

# ─── Scheduler ───────────────────────────────────────────────────────────────
TOPICS = ["geopolitics", "economics"]

def refresh_all_briefings():
    """Called by scheduler every hour to pre-generate all briefings."""
    print("[scheduler] Refreshing all briefings...")
    for topic in TOPICS:
        generate_and_cache(topic)
    print("[scheduler] All briefings refreshed.")

scheduler = BackgroundScheduler()
scheduler.add_job(refresh_all_briefings, "interval", hours=1, id="refresh_briefings")

@app.on_event("startup")
def startup():
    """On startup: generate all briefings immediately, then start scheduler."""
    print("[startup] Pre-generating all briefings...")
    refresh_all_briefings()
    scheduler.start()
    print("[startup] Scheduler started. Briefings will refresh every hour.")

@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown()

# ─── Routes ──────────────────────────────────────────────────────────────────
class QueryRequest(BaseModel):
    question: str
    topic: str = None

@app.get("/")
def root():
    return {
        "status": "Signal Intelligence API is running",
        "cached_topics": list(cache.keys()),
        "cache_ages": {
            topic: f"{int((time.time() - cache[topic]['generated_at']) / 60)}m ago"
            for topic in cache
        }
    }

@app.get("/briefing/{topic}")
def get_briefing(topic: str):
    if topic not in TOPICS:
        raise HTTPException(status_code=400, detail=f"Topic must be one of {TOPICS}")

    # Return cached version if valid
    cached = get_cached(topic)
    if cached:
        print(f"[cache] HIT for '{topic}'")
        return {**cached, "cached": True}

    # Cache miss — generate on demand
    print(f"[cache] MISS for '{topic}' — generating on demand")
    result = generate_and_cache(topic)
    if not result:
        raise HTTPException(status_code=503, detail="Could not generate briefing")

    return {**result, "cached": False}

@app.get("/cache/status")
def cache_status():
    """Shows current cache state — useful for debugging."""
    return {
        topic: {
            "cached": is_cache_valid(topic),
            "age_minutes": int((time.time() - cache[topic]["generated_at"]) / 60) if topic in cache else None,
            "article_count": cache[topic]["article_count"] if topic in cache else 0,
        }
        for topic in TOPICS
    }

@app.post("/cache/refresh")
def force_refresh(topic: str = None):
    """Manually trigger a refresh — useful for testing."""
    if topic:
        if topic not in TOPICS:
            raise HTTPException(status_code=400, detail=f"Topic must be one of {TOPICS}")
        result = generate_and_cache(topic)
        return {"refreshed": [topic], "success": result is not None}
    else:
        refresh_all_briefings()
        return {"refreshed": TOPICS, "success": True}

@app.post("/query")
def query(request: QueryRequest):
    # Search ChromaDB for historically relevant articles
    historical = search_articles(request.question, n_results=6)

    # Fetch live articles for the question
    live = fetch_articles_for_query(request.question)

    # Merge, deduplicating by URL
    seen_urls = {a['url'] for a in historical}
    combined = list(historical)
    for article in live:
        if article['url'] not in seen_urls:
            combined.append(article)
            seen_urls.add(article['url'])

    # If topic specified, add topic articles too
    if request.topic:
        topic_articles = fetch_articles(request.topic, page_size=5)
        for article in topic_articles:
            if article['url'] not in seen_urls:
                combined.append(article)
                seen_urls.add(article['url'])

    answer = answer_query(request.question, combined)

    return {
        "question": request.question,
        "answer": answer,
        "sources": combined,
        "source_count": len(combined),
        "historical_sources": len(historical),
        "live_sources": len(live),
    }

@app.get("/chroma/stats")
def chroma_stats():
    return get_collection_stats()

@app.get("/entities/signals/{topic}")
def entity_signals(topic: str):
    """See current entity signals for a topic."""
    from entities import get_entity_frequencies, get_top_entities
    return {
        "topic": topic,
        "spikes": get_entity_frequencies(topic=topic),
        "top_entities": get_top_entities(topic=topic),
    }

@app.get("/entities/signals")
def all_entity_signals():
    """See entity signals across all topics."""
    from entities import get_entity_frequencies, get_top_entities
    return {
        "spikes": get_entity_frequencies(),
        "top_entities": get_top_entities(),
    }

@app.get("/headlines")
def get_headlines():
    all_articles = []
    for topic in TOPICS:
        if topic in cache and cache[topic].get("sources"):
            for article in cache[topic]["sources"]:
                all_articles.append({**article, "topic": topic})

    if not all_articles:
        raise HTTPException(status_code=503, detail="No articles in cache yet")

    articles_text = ""
    for i, article in enumerate(all_articles, 1):
        articles_text += f"""
Article {i} — {article['source']} ({article['published_at'][:10]}) [{article['topic']}]
Title: {article['title']}
Summary: {article.get('description', '')}
"""

    from analyst import client
    import json

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=600,
        messages=[{
            "role": "user",
            "content": f"""You are a senior intelligence editor choosing the front page.

Here are today's articles across geopolitics, politics, and economics:

{articles_text}

Select the 3 most IMPORTANT stories — not the most recent, but the ones with the highest geopolitical, economic, or political significance. Prioritize:
- Active conflicts or major escalations
- Policy decisions with broad impact
- Economic shocks or market-moving events
- Significant political developments

For each story write:
1. A sharp, specific headline (max 12 words, NYT style — no clickbait)
2. One sentence explaining why this matters (max 25 words)
3. Which article numbers are relevant
4. Topic category

Respond ONLY as valid JSON, no other text:
{{
  "stories": [
    {{
      "headline": "...",
      "summary": "...",
      "article_indices": [1, 3],
      "topic": "geopolitics"
    }}
  ]
}}"""
        }]
    )

    try:
        text = message.content[0].text.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        stories = data.get("stories", [])
        for story in stories:
            indices = story.get("article_indices", [])
            story["sources"] = [
                all_articles[i - 1] for i in indices
                if 0 < i <= len(all_articles)
            ]
        return {"stories": stories}
    except Exception as e:
        print(f"[headlines] Parse error: {e}")
        raise HTTPException(status_code=500, detail="Could not parse headlines")

@app.get("/timeline/{query}")
def get_timeline(query: str):
    import json
    from analyst import client
    from chroma import search_articles

    # Search ChromaDB for all relevant articles
    articles = search_articles(query, n_results=20)

    if not articles:
        raise HTTPException(status_code=404, detail="No articles found for this topic")

    # Format articles for Claude
    articles_text = ""
    for i, article in enumerate(articles, 1):
        articles_text += f"""
Article {i} — {article['source']} ({article['published_at'][:10]})
Title: {article['title']}
Summary: {article.get('description', '')}
"""

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1500,
        messages=[{
            "role": "user",
            "content": f"""You are an intelligence analyst building a timeline of events for: "{query}"

Here are the relevant articles from the archive:

{articles_text}

Build a chronological timeline of the most significant events related to this topic.

Respond ONLY as valid JSON, no other text:
{{
  "title": "Timeline: [topic]",
  "summary": "One sentence overview of the situation",
  "events": [
    {{
      "date": "YYYY-MM-DD",
      "headline": "Short event headline (max 10 words)",
      "description": "2-3 sentences explaining what happened and why it matters",
      "significance": "HIGH" | "MEDIUM" | "LOW",
      "source": "Source name"
    }}
  ]
}}

Order events chronologically oldest to newest.
Mark turning points or major escalations as HIGH significance.
Include 6-12 events maximum — only the most important ones."""
        }]
    )

    try:
        text = message.content[0].text.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        return data
    except Exception as e:
        print(f"[timeline] Parse error: {e}")
        raise HTTPException(status_code=500, detail="Could not generate timeline")

@app.post("/timeline")
def get_custom_timeline(request: QueryRequest):
    return get_timeline(request.question)

@app.get("/entities/relationships/{entity}")
def entity_relationships(entity: str, days: int = 7):
    from entities import get_entity_relationships
    return {"entity": entity, "related": get_entity_relationships(entity, days=days)}

@app.get("/entities/graph")
def entity_graph(days: int = 7, min_cooccurrences: int = 2, topic: str = None):
    from entities import get_relationship_graph
    return get_relationship_graph(days=days, min_cooccurrences=min_cooccurrences, topic=topic)