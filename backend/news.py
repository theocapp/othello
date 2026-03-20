from newsapi import NewsApiClient
from dotenv import load_dotenv
import os

load_dotenv()

newsapi = NewsApiClient(api_key=os.getenv("NEWS_API_KEY"))

TOPIC_QUERIES = {
    "geopolitics": "geopolitics OR war OR NATO OR sanctions OR diplomacy OR military OR conflict OR nuclear",
    "economics": "Federal Reserve OR inflation OR recession OR trade war OR markets OR GDP OR tariffs OR interest rates",
}

# Tier-1 trusted sources only — cuts out noise, regional outlets, and low-quality sources
TRUSTED_SOURCES = ",".join([
    "reuters",
    "associated-press",
    "bbc-news",
    "the-guardian-uk",
    "financial-times",
    "the-economist",
    "bloomberg",
    "the-wall-street-journal",
    "the-new-york-times",
    "the-washington-post",
    "foreign-policy",
    "al-jazeera-english",
    "politico",
    "axios",
    "the-hill",
    "time",
    "newsweek",
])

def fetch_articles(topic: str, page_size: int = 10) -> list[dict]:
    query = TOPIC_QUERIES.get(topic, topic)

    response = newsapi.get_everything(
        q=query,
        sources=TRUSTED_SOURCES,
        language="en",
        sort_by="publishedAt",
        page_size=page_size,
    )

    articles = []
    for article in response.get("articles", []):
        if not article.get("title") or not article.get("description"):
            continue
        # Skip removed articles
        if article["title"] == "[Removed]":
            continue
        articles.append({
            "title": article["title"],
            "description": article["description"],
            "source": article["source"]["name"],
            "url": article["url"],
            "published_at": article["publishedAt"],
        })

    return articles


def fetch_articles_for_query(question: str, page_size: int = 8) -> list[dict]:
    stop_words = {
        "what", "why", "how", "when", "where", "who", "is", "are",
        "the", "a", "an", "do", "does", "will", "can", "should",
        "tell", "me", "about", "explain", "describe", "think"
    }

    words = question.lower().replace("?", "").replace(",", "").split()
    key_terms = [w for w in words if w not in stop_words and len(w) > 3]
    search_query = " OR ".join(key_terms[:5]) if key_terms else question

    try:
        response = newsapi.get_everything(
            q=search_query,
            sources=TRUSTED_SOURCES,
            language="en",
            sort_by="publishedAt",
            page_size=page_size,
        )

        articles = []
        for article in response.get("articles", []):
            if not article.get("title") or not article.get("description"):
                continue
            if article["title"] == "[Removed]":
                continue
            articles.append({
                "title": article["title"],
                "description": article["description"],
                "source": article["source"]["name"],
                "url": article["url"],
                "published_at": article["publishedAt"],
            })

        return articles
    except Exception as e:
        print(f"[news] Fetch error: {e}")
        return []