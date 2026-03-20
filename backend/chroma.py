import chromadb
from chromadb.utils import embedding_functions
import os
from datetime import datetime

# Initialize ChromaDB — stores data locally in a folder called 'chroma_db'
client = chromadb.PersistentClient(path="./chroma_db")

# Use a lightweight but powerful embedding model
embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2"
)

# One collection that stores all articles across all topics
collection = client.get_or_create_collection(
    name="signal_articles",
    embedding_function=embedding_fn,
    metadata={"hnsw:space": "cosine"}
)

def store_articles(articles: list[dict], topic: str):
    """Store a list of articles in ChromaDB."""
    if not articles:
        return

    documents = []
    metadatas = []
    ids = []

    for article in articles:
        # Create a unique ID from the URL
        article_id = article["url"].replace("https://", "").replace("http://", "")
        article_id = article_id[:512]  # ChromaDB has an ID length limit

        # Combine title and description for richer embeddings
        document = f"{article['title']}. {article['description']}"

        metadata = {
            "title": article["title"],
            "source": article["source"],
            "url": article["url"],
            "topic": topic,
            "published_at": article["published_at"],
            "stored_at": datetime.now().isoformat(),
        }

        documents.append(document)
        metadatas.append(metadata)
        ids.append(article_id)

    # Upsert — adds new articles, updates existing ones
    try:
        collection.upsert(
            documents=documents,
            metadatas=metadatas,
            ids=ids,
        )
        print(f"[chroma] Stored {len(articles)} articles for topic '{topic}'")
    except Exception as e:
        print(f"[chroma] Error storing articles: {e}")


def search_articles(query: str, n_results: int = 8, topic: str = None) -> list[dict]:
    """Search ChromaDB for articles semantically relevant to a query."""
    try:
        where = {"topic": topic} if topic else None

        results = collection.query(
            query_texts=[query],
            n_results=n_results,
            where=where,
        )

        if not results["documents"] or not results["documents"][0]:
            return []

        articles = []
        for i, doc in enumerate(results["documents"][0]):
            metadata = results["metadatas"][0][i]
            articles.append({
                "title": metadata["title"],
                "description": doc,
                "source": metadata["source"],
                "url": metadata["url"],
                "published_at": metadata["published_at"],
                "topic": metadata["topic"],
            })

        return articles

    except Exception as e:
        print(f"[chroma] Search error: {e}")
        return []


def get_collection_stats() -> dict:
    """Returns info about what's stored in ChromaDB."""
    count = collection.count()
    return {
        "total_articles": count,
        "collection": "signal_articles",
    }
