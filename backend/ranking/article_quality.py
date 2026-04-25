import re
from sources.source_catalog import SOURCE_SEEDS, SOURCE_PACKS
from normalization.articles import is_english_article

LOW_SIGNAL_PATTERNS = [
    r"\bhow to\b",
    r"\bwhat is\b",
    r"\bwhat are\b",
    r"\bwhen asked\b",
    r"\btalking about\b",
    r"\btop \d+\b",
    r"\bquiz\b",
    r"\bbest \w+ to\b",
    r"\bopinion:\b",
    r"\breview:\b",
]

REGISTERED_ARTICLE_DOMAINS = {
    (seed.get("source_domain") or "").lower()
    for seed in SOURCE_SEEDS
    if seed.get("source_type") == "article" and seed.get("source_domain")
}

SOURCE_METADATA_BY_DOMAIN = {
    (seed.get("source_domain") or "").lower(): seed
    for seed in SOURCE_SEEDS
    if seed.get("source_domain")
}

DOMINANT_GLOBAL_DOMAINS = {
    "www.aljazeera.com",
    "aljazeera.com",
    "bbc.co.uk",
    "www.bbc.co.uk",
    "apnews.com",
    "reuters.com",
}


def _score_article(article: dict, topic: str) -> int:
    # Import here to avoid circular dependency
    from news import TOPIC_QUERIES
    
    haystack = " ".join(
        [
            article.get("title", ""),
            article.get("description", ""),
            article.get("source", ""),
        ]
    ).lower()
    score = 0
    
    configured = TOPIC_QUERIES.get(topic, topic)
    if isinstance(configured, list):
        queries = configured
    else:
        queries = [configured]
    
    raw_terms = (
        " ".join(queries).replace('"', " ").replace("OR", " ")
    )
    for token in raw_terms.lower().split():
        clean = token.strip()
        if not clean or clean in {"or"} or len(clean) < 4:
            continue
        if clean in haystack:
            score += 1
    return score


def article_quality_score(article: dict, topics: list[str] | None = None) -> int:
    text = " ".join(
        [
            article.get("title", ""),
            article.get("description", ""),
            article.get("source", ""),
        ]
    ).strip()
    haystack = text.lower()
    title = (article.get("title") or "").strip()
    description = (article.get("description") or "").strip()
    source_domain = (article.get("source_domain") or "").strip().lower()
    source_meta = SOURCE_METADATA_BY_DOMAIN.get(source_domain, {})

    score = 0

    if source_domain in REGISTERED_ARTICLE_DOMAINS:
        score += 3
    if source_meta.get("region") and source_meta.get("region") not in {
        "global",
        "united-states",
        "europe",
    }:
        score += 2
    if source_meta.get("trust_tier") == "tier_2":
        score += 1
    if title and 24 <= len(title) <= 180:
        score += 2
    if description and description != title and len(description) >= 48:
        score += 2
    if article.get("published_at"):
        score += 1
    if is_english_article(article):
        score += 1
    elif article.get("translated_title") or article.get("translated_description"):
        score += 2
    elif article.get("language"):
        score += 1

    title_lower = title.lower()
    for pattern in LOW_SIGNAL_PATTERNS:
        if re.search(pattern, title_lower):
            score -= 4

    if len(title.split()) < 5:
        score -= 2
    if description == title and len(description) < 80:
        score -= 2

    topic_list = topics
    if topic_list is None:
        # Import here to avoid circular dependency
        from classification.topics import infer_article_topics
        topic_list = infer_article_topics(article)
    
    topic_bonus = 0
    for topic in topic_list:
        topic_bonus = max(topic_bonus, _score_article(article, topic))
    score += topic_bonus
    if source_domain in DOMINANT_GLOBAL_DOMAINS:
        score -= 1
    return score


def should_promote_article(article: dict, topics: list[str] | None = None) -> bool:
    if topics is None:
        from classification.topics import infer_article_topics
        topic_list = infer_article_topics(article)
    else:
        topic_list = topics
        
    quality = article_quality_score(article, topic_list)
    source_domain = (article.get("source_domain") or "").strip().lower()
    source_meta = SOURCE_METADATA_BY_DOMAIN.get(source_domain, {})

    if not topic_list:
        return False
    if quality >= 7:
        return True
    if source_domain in REGISTERED_ARTICLE_DOMAINS and quality >= 5:
        return True
    if (
        source_meta.get("region")
        and source_meta.get("region") not in {"global", "united-states", "europe"}
        and quality >= 4
    ):
        return True
    if not is_english_article(article) and quality >= 5:
        return True
    return False


def diversify_articles(
    articles: list[dict],
    page_size: int,
    topics: list[str] | None = None,
    max_per_domain: int = 2,
) -> list[dict]:
    if not articles:
        return []

    ranked = sorted(
        articles,
        key=lambda article: (
            -article_quality_score(article, topics),
            -int(
                (
                    SOURCE_METADATA_BY_DOMAIN.get(
                        (article.get("source_domain") or "").strip().lower(), {}
                    )
                    or {}
                ).get("region")
                not in {"", "global", "united-states", "europe"}
            ),
            -int(not is_english_article(article)),
            article.get("published_at", ""),
        ),
        reverse=False,
    )
    ranked = list(reversed(ranked))

    selected = []
    overflow = []
    per_domain_counts: dict[str, int] = {}

    for article in ranked:
        domain = (
            (article.get("source_domain") or article.get("source") or "unknown")
            .strip()
            .lower()
        )
        limit = max_per_domain
        if domain in DOMINANT_GLOBAL_DOMAINS:
            limit = min(limit, 1)
        if per_domain_counts.get(domain, 0) < limit:
            selected.append(article)
            per_domain_counts[domain] = per_domain_counts.get(domain, 0) + 1
        else:
            overflow.append(article)
        if len(selected) >= page_size:
            return selected[:page_size]

    if len(selected) >= max(3, page_size // 2):
        return selected[:page_size]

    for article in overflow:
        selected.append(article)
        if len(selected) >= page_size:
            break

    return selected[:page_size]
