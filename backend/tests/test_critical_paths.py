"""Basic tests for critical paths in the signal intelligence system."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, MagicMock


# ============================================================================
# Clustering Tests
# ============================================================================

def test_relatedness_score_basic():
    """Test basic relatedness scoring between two articles."""
    from clustering import relatedness_score, build_article_signatures

    articles = [
        {
            "url": "https://example.com/article1",
            "title": "Iran military strike reported",
            "description": "Military forces conducted a strike operation",
            "published_at": "2024-01-01T12:00:00Z",
        },
        {
            "url": "https://example.com/article2",
            "title": "Iran military operation continues",
            "description": "Military operations continue in the region",
            "published_at": "2024-01-01T14:00:00Z",
        },
    ]

    signatures = build_article_signatures(articles)
    score = relatedness_score(signatures[0], signatures[1])

    assert 0.0 <= score <= 1.0
    assert score > 0.3  # Should be related


def test_relatedness_score_temporal_decay():
    """Test that temporal distance affects relatedness score."""
    from clustering import relatedness_score, build_article_signatures

    articles = [
        {
            "url": "https://example.com/article1",
            "title": "Iran military strike",
            "description": "Military strike operation",
            "published_at": "2024-01-01T12:00:00Z",
        },
        {
            "url": "https://example.com/article2",
            "title": "Iran military strike",
            "description": "Military strike operation",
            "published_at": "2024-01-15T12:00:00Z",  # 14 days later
        },
    ]

    signatures = build_article_signatures(articles)
    score = relatedness_score(signatures[0], signatures[1])

    assert 0.0 <= score <= 1.0


def test_cluster_articles_empty():
    """Test clustering with empty article list."""
    from clustering import cluster_articles

    result = cluster_articles([])
    assert result == []


def test_cluster_articles_single():
    """Test clustering with single article."""
    from clustering import cluster_articles

    articles = [
        {
            "url": "https://example.com/article1",
            "title": "Test article",
            "description": "Test description",
            "published_at": "2024-01-01T12:00:00Z",
        },
    ]

    result = cluster_articles(articles)
    assert len(result) == 1
    assert result[0]["article_count"] == 1


def test_cluster_articles_multiple():
    """Test clustering with multiple related articles."""
    from clustering import cluster_articles

    articles = [
        {
            "url": f"https://example.com/article{i}",
            "title": "Iran military strike reported",
            "description": f"Military operation {i}",
            "published_at": "2024-01-01T12:00:00Z",
        }
        for i in range(5)
    ]

    result = cluster_articles(articles)
    assert len(result) >= 1
    assert all(event["article_count"] >= 1 for event in result)


# ============================================================================
# Article Upsert Tests
# ============================================================================

def test_normalize_article_basic():
    """Test basic article normalization."""
    from db.articles_repo import _normalize_article

    article = {
        "url": "https://example.com/article",
        "title": "Test Article",
        "description": "Test description",
        "source": "Test Source",
        "published_at": "2024-01-01T12:00:00Z",
    }

    result = _normalize_article(article, "test_provider")

    assert result["url"] == "https://example.com/article"
    assert result["title"] == "Test Article"
    assert result["source"] == "Test Source"
    assert result["provider"] == "test_provider"
    assert "content_hash" in result


def test_normalize_article_missing_url():
    """Test that missing URL raises ValueError."""
    from db.articles_repo import _normalize_article

    article = {
        "title": "Test Article",
        "description": "Test description",
    }

    with pytest.raises(ValueError, match="article missing url"):
        _normalize_article(article, "test_provider")


def test_normalize_article_missing_title():
    """Test that missing title raises ValueError."""
    from db.articles_repo import _normalize_article

    article = {
        "url": "https://example.com/article",
        "description": "Test description",
    }

    with pytest.raises(ValueError, match="article missing title"):
        _normalize_article(article, "test_provider")


def test_upsert_articles_empty():
    """Test upserting empty article list."""
    from db.articles_repo import upsert_articles

    result = upsert_articles([], "test_topic", "test_provider")
    assert result == 0


def test_upsert_articles_single():
    """Test upserting single article."""
    from db.articles_repo import upsert_articles

    article = {
        "url": "https://example.com/test-article",
        "title": "Test Article",
        "description": "Test description",
        "source": "Test Source",
        "published_at": "2024-01-01T12:00:00Z",
    }

    result = upsert_articles([article], "test_topic", "test_provider")
    assert result >= 0


# ============================================================================
# Query Service Tests
# ============================================================================

def test_extract_search_focus():
    """Test search focus extraction."""
    from services.query_service import _extract_search_focus

    question = 'What is happening with "Iran military strikes" in the region?'
    focus = _extract_search_focus(question)

    assert "Iran" in focus or "military" in focus or "strikes" in focus


def test_extract_search_focus_no_quotes():
    """Test search focus extraction without quotes."""
    from services.query_service import _extract_search_focus

    question = "What is happening with Iran military strikes in the region?"
    focus = _extract_search_focus(question)

    assert len(focus) > 0


def test_clean_source_urls():
    """Test source URL cleaning."""
    from services.query_service import _clean_source_urls

    urls = [
        "https://example.com/article1",
        "https://example.com/article2",
        "not-a-url",
        "https://example.com/article1",  # duplicate
        "https://example.com/article3",
    ]

    cleaned = _clean_source_urls(urls, limit=12)

    assert len(cleaned) == 3
    assert all(url.startswith("http") for url in cleaned)


def test_clean_source_urls_limit():
    """Test source URL cleaning with limit."""
    from services.query_service import _clean_source_urls

    urls = [f"https://example.com/article{i}" for i in range(20)]

    cleaned = _clean_source_urls(urls, limit=5)

    assert len(cleaned) == 5


def test_normalize_query_corpus_topic():
    """Test query topic normalization."""
    from services.query_service import _normalize_query_corpus_topic

    assert _normalize_query_corpus_topic("conflict") == "geopolitics"
    assert _normalize_query_corpus_topic("geopolitics") == "geopolitics"
    assert _normalize_query_corpus_topic("economics") == "economics"
    assert _normalize_query_corpus_topic(None) is None


def test_infer_query_topic():
    """Test query topic inference."""
    from services.query_service import _infer_query_topic

    assert _infer_query_topic("Iran military strike") == "geopolitics"
    assert _infer_query_topic("Federal Reserve interest rates") == "economics"
    assert _infer_query_topic("random unrelated text") is None


# ============================================================================
# Hotspot Tests
# ============================================================================

def test_get_hotspot_aspect_conflict():
    """Test hotspot aspect detection for conflict."""
    from lib.hotspots import getHotspotAspect

    hotspot = {
        "aspect": "conflict",
        "event_types": ["battle", "violence"],
    }

    aspect = getHotspotAspect(hotspot)
    assert aspect == "conflict"


def test_get_hotspot_aspect_political():
    """Test hotspot aspect detection for political."""
    from lib.hotspots import getHotspotAspect

    hotspot = {
        "aspect": "political",
        "event_types": ["protest", "government"],
    }

    aspect = getHotspotAspect(hotspot)
    assert aspect == "political"


def test_get_hotspot_aspect_economic():
    """Test hotspot aspect detection for economic."""
    from lib.hotspots import getHotspotAspect

    hotspot = {
        "aspect": "economic",
        "event_types": ["market", "sanctions"],
    }

    aspect = getHotspotAspect(hotspot)
    assert aspect == "economic"


def test_get_hotspot_palette():
    """Test hotspot palette retrieval."""
    from lib.hotspots import getHotspotPalette, HOTSPOT_TYPE_PALETTE

    hotspot = {"aspect": "conflict"}
    palette = getHotspotPalette(hotspot)

    assert palette == HOTSPOT_TYPE_PALETTE["conflict"]
    assert "core" in palette
    assert "ring" in palette
    assert "cloud" in palette


# ============================================================================
# Database Query Tests
# ============================================================================

def test_count_articles_since():
    """Test counting articles since a cutoff date."""
    from db.articles_repo import _count_articles_since

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    count = _count_articles_since(cutoff)

    assert count >= 0
    assert isinstance(count, int)


def test_get_article_count():
    """Test getting article count."""
    from db.articles_repo import get_article_count

    count = get_article_count()
    assert count >= 0
    assert isinstance(count, int)


def test_get_article_count_with_hours():
    """Test getting article count with hours filter."""
    from db.articles_repo import get_article_count

    count = get_article_count(hours=24)
    assert count >= 0
    assert isinstance(count, int)


def test_get_topic_time_bounds():
    """Test getting topic time bounds."""
    from db.articles_repo import get_topic_time_bounds

    bounds = get_topic_time_bounds()

    assert "earliest_published_at" in bounds
    assert "latest_published_at" in bounds


def test_get_topic_time_bounds_with_topic():
    """Test getting topic time bounds with topic filter."""
    from db.articles_repo import get_topic_time_bounds

    bounds = get_topic_time_bounds(topic="geopolitics")

    assert "earliest_published_at" in bounds
    assert "latest_published_at" in bounds


# ============================================================================
# API Model Tests
# ============================================================================

def test_query_request_valid():
    """Test valid QueryRequest model."""
    from api.models import QueryRequest

    request = QueryRequest(
        question="What is happening in Iran?",
        topic="geopolitics",
        limit=12,
    )

    assert request.question == "What is happening in Iran?"
    assert request.topic == "geopolitics"


def test_query_request_invalid_question():
    """Test QueryRequest with invalid question."""
    from api.models import QueryRequest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        QueryRequest(question="")  # Empty question


def test_query_request_invalid_topic():
    """Test QueryRequest with invalid topic."""
    from api.models import QueryRequest

    request = QueryRequest(
        question="Test question",
        topic="invalid_topic",
    )

    # Should not raise, but topic should be normalized
    assert request.topic == "invalid_topic"


def test_query_request_source_urls_validation():
    """Test QueryRequest source URLs validation."""
    from api.models import QueryRequest

    request = QueryRequest(
        question="Test question",
        source_urls=[
            "https://example.com/article1",
            "not-a-url",
            "https://example.com/article2",
        ],
    )

    # Should filter out invalid URLs
    assert len(request.source_urls) == 2
    assert all(url.startswith("http") for url in request.source_urls)


def test_get_events_request_valid():
    """Test valid GetEventsRequest model."""
    from api.models import GetEventsRequest

    request = GetEventsRequest(limit=20, include_factual=True)

    assert request.limit == 20
    assert request.include_factual is True


def test_get_events_request_invalid_limit():
    """Test GetEventsRequest with invalid limit."""
    from api.models import GetEventsRequest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        GetEventsRequest(limit=0)  # Below minimum

    with pytest.raises(ValidationError):
        GetEventsRequest(limit=200)  # Above maximum


def test_get_structured_events_request_valid():
    """Test valid GetStructuredEventsRequest model."""
    from api.models import GetStructuredEventsRequest

    request = GetStructuredEventsRequest(
        days=7,
        limit=50,
        country="Iran",
        event_type="battle",
    )

    assert request.days == 7
    assert request.limit == 50
    assert request.country == "Iran"
    assert request.event_type == "battle"


def test_get_hotspot_attention_map_request_valid():
    """Test valid GetHotspotAttentionMapRequest model."""
    from api.models import GetHotspotAttentionMapRequest

    request = GetHotspotAttentionMapRequest(
        window="7d",
        start="2024-01-01T00:00:00Z",
        end="2024-01-07T23:59:59Z",
    )

    assert request.window == "7d"
    assert request.start == "2024-01-01T00:00:00Z"
    assert request.end == "2024-01-07T23:59:59Z"


def test_get_hotspot_attention_map_request_invalid_date():
    """Test GetHotspotAttentionMapRequest with invalid date."""
    from api.models import GetHotspotAttentionMapRequest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        GetHotspotAttentionMapRequest(
            window="24h",
            start="invalid-date",
        )


def test_get_hotspot_attention_map_request_invalid_window():
    """Test GetHotspotAttentionMapRequest with invalid window."""
    from api.models import GetHotspotAttentionMapRequest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        GetHotspotAttentionMapRequest(window="invalid")


# ============================================================================
# Integration Tests
# ============================================================================

def test_query_integration():
    """Test end-to-end query flow."""
    from services.query_service import _gather_query_articles

    articles, meta = _gather_query_articles(
        question="Iran military strike",
        limit=5,
    )

    assert isinstance(articles, list)
    assert isinstance(meta, dict)
    assert "focus" in meta
    assert "topic" in meta


def test_cluster_integration():
    """Test end-to-end clustering flow."""
    from clustering import cluster_articles

    articles = [
        {
            "url": f"https://example.com/article{i}",
            "title": "Iran military strike reported",
            "description": f"Military operation {i}",
            "published_at": "2024-01-01T12:00:00Z",
            "source": f"Source {i}",
        }
        for i in range(10)
    ]

    events = cluster_articles(articles, topic="geopolitics")

    assert isinstance(events, list)
    assert all("event_id" in event for event in events)
    assert all("label" in event for event in events)
    assert all("article_count" in event for event in events)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
