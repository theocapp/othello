from datetime import datetime

import chromadb
from chromadb.utils import embedding_functions

_client = None
_collection = None


def get_collection():
    global _client, _collection
    try:
        if _collection is not None:
            _collection.count()
            return _collection
    except Exception:
        _client = None
        _collection = None

    _client = chromadb.PersistentClient(path="./chroma_db")
    embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )
    _collection = _client.get_or_create_collection(
        name="signal_articles",
        embedding_function=embedding_fn,
        metadata={"hnsw:space": "cosine"},
    )
    return _collection


_CHROMA_BATCH_SIZE = 20


def store_articles(articles: list[dict], topic: str):
    if not articles:
        return

    stored = 0
    for batch_start in range(0, len(articles), _CHROMA_BATCH_SIZE):
        batch = articles[batch_start : batch_start + _CHROMA_BATCH_SIZE]
        documents = []
        metadatas = []
        ids = []
        stored_at = datetime.now().isoformat()

        for article in batch:
            article_id = article["url"].replace("https://", "").replace("http://", "")[:512]
            documents.append(f"{article['title']}. {article['description']}")
            metadatas.append(
                {
                    "title": article["title"],
                    "source": article["source"],
                    "url": article["url"],
                    "topic": topic,
                    "published_at": article["published_at"],
                    "stored_at": stored_at,
                }
            )
            ids.append(article_id)

        try:
            get_collection().upsert(documents=documents, metadatas=metadatas, ids=ids)
            stored += len(batch)
        except Exception as exc:
            print(f"[chroma] Error storing batch for '{topic}': {exc}")
            break

    print(f"[chroma] Stored {stored}/{len(articles)} articles for topic '{topic}'")


def search_articles(query: str, n_results: int = 8, topic: str = None) -> list[dict]:
    try:
        where = {"topic": topic} if topic else None
        results = get_collection().query(query_texts=[query], n_results=n_results, where=where)
        if not results["documents"] or not results["documents"][0]:
            return []

        articles = []
        for index, doc in enumerate(results["documents"][0]):
            metadata = results["metadatas"][0][index]
            articles.append(
                {
                    "title": metadata["title"],
                    "description": doc,
                    "source": metadata["source"],
                    "url": metadata["url"],
                    "published_at": metadata["published_at"],
                    "topic": metadata["topic"],
                }
            )
        return articles
    except Exception as exc:
        print(f"[chroma] Search error: {exc}")
        return []


def get_collection_stats() -> dict:
    try:
        count = get_collection().count()
    except Exception as exc:
        print(f"[chroma] Stats error: {exc}")
        global _client, _collection
        _client = None
        _collection = None
        try:
            count = get_collection().count()
        except Exception:
            count = 0
    return {"total_articles": count, "collection": "signal_articles"}
