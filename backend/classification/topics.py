TOPIC_KEYWORDS = {
    "geopolitics": {
        "war",
        "conflict",
        "military",
        "ceasefire",
        "airstrike",
        "air strike",
        "missile",
        "strike",
        "bombing",
        "explosion",
        "attack",
        "assault",
        "invasion",
        "occupation",
        "offensive",
        "counteroffensive",
        "shelling",
        "artillery",
        "drone strike",
        "naval",
        "warship",
        "fighter jet",
        "special forces",
        "troops",
        "soldiers",
        "casualties",
        "killed",
        "wounded",
        "fatalities",
        "civilian",
        "displacement",
        "refugee",
        "evacuation",
        "sanctions",
        "diplomacy",
        "treaty",
        "agreement",
        "summit",
        "talks",
        "peace deal",
        "nato",
        "un security council",
        "united nations",
        "elections",
        "election",
        "coup",
        "overthrow",
        "junta",
        "referendum",
        "parliament",
        "president",
        "prime minister",
        "government",
        "regime",
        "opposition",
        "protest",
        "uprising",
        "crackdown",
        "arrest",
        "detained",
        "extradition",
        "asylum",
        "expelled",
        "ambassador",
        "diplomat",
        "foreign minister",
        "secretary of state",
        "state department",
        "midterms",
        "election interference",
        "ballot",
        "iran",
        "israel",
        "ukraine",
        "russia",
        "china",
        "taiwan",
        "north korea",
        "gaza",
        "west bank",
        "hamas",
        "hezbollah",
        "isis",
        "al-qaeda",
        "putin",
        "zelensky",
        "netanyahu",
        "xi jinping",
        "sudan",
        "myanmar",
        "ethiopia",
        "sahel",
        "mali",
        "niger",
        "burkina faso",
        "somalia",
        "yemen",
        "syria",
        "iraq",
        "afghanistan",
        "pakistan",
        "venezuela",
        "haiti",
        "colombia",
        "mexico cartel",
        "intelligence",
        "espionage",
        "spy",
        "cyber attack",
        "cyberattack",
        "hack",
        "disinformation",
        "propaganda",
        "nuclear",
        "weapons",
        "ballistic",
        "hypersonic",
        "chemical weapons",
    },
    "economics": {
        "inflation",
        "recession",
        "gdp",
        "growth",
        "economy",
        "economic",
        "market",
        "markets",
        "unemployment",
        "jobs",
        "labor market",
        "wage",
        "wages",
        "interest rate",
        "rates",
        "federal reserve",
        "fed",
        "central bank",
        "monetary policy",
        "quantitative easing",
        "tightening",
        "rate hike",
        "rate cut",
        "yield",
        "yields",
        "bond",
        "treasury",
        "tariffs",
        "tariff",
        "trade war",
        "trade deal",
        "trade deficit",
        "sanctions",
        "embargo",
        "export",
        "import",
        "supply chain",
        "supply crunch",
        "shortage",
        "futures",
        "options",
        "hedge fund",
        "stocks",
        "equities",
        "index",
        "rally",
        "selloff",
        "volatility",
        "oil",
        "crude",
        "opec",
        "gas",
        "energy",
        "commodity",
        "commodities",
        "gold",
        "copper",
        "wheat",
        "food prices",
        "gasoline",
        "fuel",
        "buyout",
        "merger",
        "acquisition",
        "ipo",
        "bankruptcy",
        "default",
        "debt",
        "credit",
        "loan",
        "bailout",
        "austerity",
        "imf",
        "world bank",
        "rupee",
        "yuan",
        "ruble",
        "currency",
        "exchange rate",
        "devaluation",
        "semiconductor",
        "chip",
        "reshoring",
        "nearshoring",
    },
}

# Representative sentences that define each topic for embedding-based classification.
# These are the centroids used when keyword matching returns no result.
TOPIC_CENTROID_TEXTS = {
    "geopolitics": (
        "military conflict armed forces war ceasefire diplomacy sanctions "
        "coup election government troops missile airstrike invasion occupation "
        "rebel forces peace talks nuclear weapons intelligence espionage"
    ),
    "economics": (
        "inflation interest rates central bank federal reserve GDP growth "
        "recession unemployment trade tariffs market stocks bonds currency "
        "commodity oil energy supply chain IMF World Bank fiscal policy "
        "monetary policy exchange rate debt default bankruptcy merger acquisition"
    ),
}

_topic_centroid_embeddings: dict[str, object] | None = None


def infer_article_topics(article: dict) -> list[str]:
    haystack = " ".join(
        [
            article.get("title", ""),
            article.get("description", ""),
            article.get("source", ""),
        ]
    ).lower()
    scored = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in haystack)
        if score > 0:
            scored.append((topic, score))

    if not scored:
        # No keyword match — try embedding-based classification as fallback.
        return _classify_topic_by_embedding(article)

    scored.sort(key=lambda item: item[1], reverse=True)
    best_topic, best_score = scored[0]

    if best_score < 1:
        return []

    matches = [best_topic]
    for topic, score in scored[1:]:
        if score >= best_score - 1 and score >= 3:
            matches.append(topic)
    return matches


def _classify_topic_by_embedding(article: dict) -> list[str]:
    """Classify an article by cosine similarity to topic centroid embeddings.

    Only called when keyword matching returns no result. Uses the same
    SentenceTransformer model as the clustering pipeline.
    Returns a list of matching topics, or empty list if below threshold.
    """
    global _topic_centroid_embeddings

    try:
        from clustering import get_semantic_model
        from sklearn.metrics.pairwise import cosine_similarity as _cosine_similarity
        import numpy as np
    except ImportError:
        return []

    text = " ".join(
        filter(
            None,
            [
                article.get("title", ""),
                article.get("description", ""),
            ],
        )
    ).strip()
    if not text:
        return []

    try:
        model = get_semantic_model()

        if _topic_centroid_embeddings is None:
            centroid_texts = list(TOPIC_CENTROID_TEXTS.values())
            centroid_keys = list(TOPIC_CENTROID_TEXTS.keys())
            embeddings = model.encode(centroid_texts, convert_to_numpy=True)
            _topic_centroid_embeddings = dict(zip(centroid_keys, embeddings))

        article_embedding = model.encode([text], convert_to_numpy=True)[0]

        results = []
        for topic, centroid_embedding in _topic_centroid_embeddings.items():
            score = float(
                _cosine_similarity(
                    article_embedding.reshape(1, -1), centroid_embedding.reshape(1, -1)
                )[0][0]
            )
            if score >= 0.30:
                results.append((topic, score))

        results.sort(key=lambda x: x[1], reverse=True)
        return [topic for topic, _ in results]

    except Exception:
        return []
