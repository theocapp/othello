from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
import os
import time
import json

load_dotenv()
from chroma import store_articles, search_articles, get_collection_stats
from news import fetch_articles, fetch_articles_for_query
from analyst import generate_briefing, answer_query, client
from entities import store_entity_mentions, format_signals_for_briefing
from contradictions import detect_contradictions
from cache import init_db, load_briefing, save_briefing, load_headlines, save_headlines  # CHANGED

app = FastAPI(title="Signal Intelligence API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TOPICS = ["geopolitics", "economics"]
CACHE_TTL = 3600

# CHANGED — no more in-memory cache dict, SQLite handles it now

def generate_and_cache(topic: str):
    print(f"[briefing] Generating '{topic}'...")
    try:
        articles = fetch_articles(topic)
        if not articles:
            print(f"[briefing] No articles found for '{topic}'")
            return None

        store_articles(articles, topic)
        store_entity_mentions(articles, topic)
        signals = format_signals_for_briefing(topic)

        print(f"[contradictions] Analyzing '{topic}'...")
        contradictions = detect_contradictions(articles, topic)

        briefing = generate_briefing(topic, articles, signals, contradictions)
        save_briefing(topic, briefing, articles, len(articles))  # CHANGED
        print(f"[briefing] Done '{topic}' ({len(articles)} articles)")
        return load_briefing(topic)  # CHANGED
    except Exception as e:
        print(f"[briefing] Error generating '{topic}': {e}")
        return None

def refresh_all_briefings():
    print("[scheduler] Refreshing all briefings...")
    for topic in TOPICS:
        generate_and_cache(topic)
    print("[scheduler] All briefings refreshed.")

scheduler = BackgroundScheduler()
scheduler.add_job(refresh_all_briefings, "interval", hours=1, id="refresh_briefings")

@app.on_event("startup")
def startup():
    init_db()  # CHANGED — initialize SQLite tables
    print("[startup] Checking cache...")
    needs_refresh = []
    for topic in TOPICS:
        cached = load_briefing(topic, ttl=CACHE_TTL)
        if not cached:
            needs_refresh.append(topic)
        else:
            age = int((time.time() - cached["generated_at"]) / 60)
            print(f"[startup] '{topic}' cache is {age}m old — skipping regeneration")  # CHANGED

    if needs_refresh:
        print(f"[startup] Generating fresh briefings for: {needs_refresh}")
        for topic in needs_refresh:
            generate_and_cache(topic)
    else:
        print("[startup] All briefings fresh — skipping warmup")  # CHANGED

    scheduler.start()

@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown()

class QueryRequest(BaseModel):
    question: str
    topic: str = None

@app.get("/")
def root():
    status = {}
    for topic in TOPICS:
        cached = load_briefing(topic, ttl=CACHE_TTL)
        status[topic] = f"{int((time.time() - cached['generated_at']) / 60)}m ago" if cached else "not cached"
    return {"status": "Signal Intelligence API is running", "cache": status}

@app.get("/briefing/{topic}")
def get_briefing(topic: str):
    if topic not in TOPICS:
        raise HTTPException(status_code=400, detail=f"Topic must be one of {TOPICS}")

    cached = load_briefing(topic, ttl=CACHE_TTL)
    if cached:
        print(f"[cache] HIT for '{topic}'")
        return {**cached, "cached": True}

    print(f"[cache] MISS for '{topic}' — generating on demand")
    result = generate_and_cache(topic)
    if not result:
        raise HTTPException(status_code=503, detail="Could not generate briefing")
    return {**result, "cached": False}

@app.get("/cache/status")
def cache_status():
    result = {}
    for topic in TOPICS:
        cached = load_briefing(topic, ttl=CACHE_TTL)
        result[topic] = {
            "cached": cached is not None,
            "age_minutes": int((time.time() - cached["generated_at"]) / 60) if cached else None,
            "article_count": cached["article_count"] if cached else 0,
        }
    return result

@app.post("/cache/refresh")
def force_refresh(topic: str = None):
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
    historical = search_articles(request.question, n_results=5)  # CHANGED — was 6
    live = fetch_articles_for_query(request.question)

    seen_urls = {a['url'] for a in historical}
    combined = list(historical)
    for article in live:
        if article['url'] not in seen_urls:
            combined.append(article)
            seen_urls.add(article['url'])

    if request.topic:
        topic_articles = fetch_articles(request.topic, page_size=3)  # CHANGED — was 5
        for article in topic_articles:
            if article['url'] not in seen_urls:
                combined.append(article)
                seen_urls.add(article['url'])

    combined = combined[:10]  # CHANGED — hard cap, prevents bloated calls

    answer = answer_query(request.question, combined)
    return {
        "question": request.question,
        "answer": answer,
        "sources": combined,
        "source_count": len(combined),
        "historical_sources": len(historical),
        "live_sources": len(live),
    }

@app.get("/headlines")
def get_headlines():
    # CHANGED — check cache first
    cached_stories = load_headlines(ttl=CACHE_TTL)
    if cached_stories:
        print("[headlines] Cache HIT")
        return {"stories": cached_stories}

    print("[headlines] Cache MISS — generating")
    all_articles = []
    for topic in TOPICS:
        cached = load_briefing(topic, ttl=CACHE_TTL)
        if cached and cached.get("sources"):
            for article in cached["sources"]:
                all_articles.append({**article, "topic": topic})

    if not all_articles:
        raise HTTPException(status_code=503, detail="No articles in cache yet")

    # CHANGED — truncate descriptions to save tokens
    articles_text = ""
    for i, article in enumerate(all_articles, 1):
        desc = (article.get('description') or '')[:120]
        articles_text += f"Article {i} — {article['source']} [{article['topic']}]\nTitle: {article['title']}\nSummary: {desc}\n\n"

    message = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        max_tokens=600,
        messages=[{
            "role": "user",
            "content": f"""You are a senior intelligence editor choosing the front page.

Here are today's articles:

{articles_text}

Select the 3 most important stories by geopolitical, economic, or political significance.

Respond ONLY as valid JSON:
{{
  "stories": [
    {{
      "headline": "Sharp specific headline max 12 words",
      "summary": "One sentence why this matters max 25 words",
      "article_indices": [1, 3],
      "topic": "geopolitics"
    }}
  ]
}}"""
        }]
    )

    try:
        text = message.choices[0].message.content.strip().replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        stories = data.get("stories", [])
        for story in stories:
            indices = story.get("article_indices", [])
            story["sources"] = [all_articles[i - 1] for i in indices if 0 < i <= len(all_articles)]
        save_headlines(stories)  # CHANGED — persist to SQLite
        return {"stories": stories}
    except Exception as e:
        print(f"[headlines] Parse error: {e}")
        raise HTTPException(status_code=500, detail="Could not parse headlines")

# Timeline and entity endpoints unchanged
@app.get("/timeline/{query}")
def get_timeline(query: str):
    import json
    from chroma import search_articles

    articles = search_articles(query, n_results=15)  # CHANGED — was 20
    if not articles:
        raise HTTPException(status_code=404, detail="No articles found for this topic")

    articles_text = ""
    for i, article in enumerate(articles, 1):
        desc = (article.get('description') or '')[:100]  # CHANGED — truncate
        articles_text += f"Article {i} — {article['source']} ({article['published_at'][:10]})\nTitle: {article['title']}\nSummary: {desc}\n\n"

    message = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        max_tokens=1200,
        messages=[{
            "role": "user",
            "content": f"""You are an intelligence analyst building a timeline for: "{query}"

{articles_text}

Respond ONLY as valid JSON:
{{
  "title": "Timeline: [topic]",
  "summary": "One sentence overview",
  "events": [
    {{
      "date": "YYYY-MM-DD",
      "headline": "Short event headline max 10 words",
      "description": "2-3 sentences on what happened and why it matters",
      "significance": "HIGH" | "MEDIUM" | "LOW",
      "source": "Source name"
    }}
  ]
}}

Chronological, oldest first. 6-10 events max."""
        }]
    )

    try:
        text = message.choices[0].message.content.strip().replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"[timeline] Parse error: {e}")
        raise HTTPException(status_code=500, detail="Could not generate timeline")

@app.post("/timeline")
def get_custom_timeline(request: QueryRequest):
    return get_timeline(request.question)

@app.get("/chroma/stats")
def chroma_stats():
    return get_collection_stats()

@app.get("/entities/signals/{topic}")
def entity_signals(topic: str):
    from entities import get_entity_frequencies, get_top_entities
    return {"topic": topic, "spikes": get_entity_frequencies(topic=topic), "top_entities": get_top_entities(topic=topic)}

@app.get("/entities/signals")
def all_entity_signals():
    from entities import get_entity_frequencies, get_top_entities
    return {"spikes": get_entity_frequencies(), "top_entities": get_top_entities()}

@app.get("/entities/relationships/{entity}")
def entity_relationships(entity: str, days: int = 7):
    from entities import get_entity_relationships
    return {"entity": entity, "related": get_entity_relationships(entity, days=days)}

@app.get("/entities/graph")
def entity_graph(days: int = 7, min_cooccurrences: int = 2, topic: str = None):
    from entities import get_relationship_graph
    return get_relationship_graph(days=days, min_cooccurrences=min_cooccurrences, topic=topic)