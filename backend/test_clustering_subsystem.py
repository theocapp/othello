"""Unit tests for clustering subsystem extraction and compatibility wrappers."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

_BACKEND = Path(__file__).resolve().parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import clustering as clustering_module  # noqa: E402
import contradictions as contradictions_module  # noqa: E402


def test_contradictions_cluster_articles_delegates_to_clustering_module():
    articles = [{"title": "A"}]
    expected = [{"label": "Delegated"}]
    with patch.object(
        contradictions_module,
        "_cluster_articles_impl",
        return_value=expected,
    ) as delegated:
        result = contradictions_module.cluster_articles(articles, topic="geopolitics")

    assert result == expected
    delegated.assert_called_once_with(articles, topic="geopolitics")


def test_contradictions_event_cluster_key_delegates_to_clustering_module():
    event = {"topic": "geopolitics", "label": "X", "articles": []}
    with patch.object(
        contradictions_module,
        "_event_cluster_key_impl",
        return_value="obs_key_1",
    ) as delegated:
        result = contradictions_module.event_cluster_key(event)

    assert result == "obs_key_1"
    delegated.assert_called_once_with(event)


def test_clustering_event_cluster_key_is_stable_across_article_order():
    base = {
        "topic": "geopolitics",
        "label": "Event label",
        "latest_update": "2026-04-07T12:00:00Z",
    }
    event_a = {
        **base,
        "articles": [
            {"url": "https://example.com/a"},
            {"url": "https://example.com/b"},
        ],
    }
    event_b = {
        **base,
        "articles": [
            {"url": "https://example.com/b"},
            {"url": "https://example.com/a"},
        ],
    }

    assert clustering_module.event_cluster_key(event_a) == clustering_module.event_cluster_key(event_b)


def test_cluster_articles_event_shape_remains_compatible():
    articles = [
        {
            "url": "https://example.com/a",
            "title": "Alpha",
            "description": "Alpha summary",
            "source": "Source One",
            "source_domain": "one.example",
            "published_at": "2026-04-07T12:00:00Z",
        },
        {
            "url": "https://example.com/b",
            "title": "Beta",
            "description": "Beta summary",
            "source": "Source Two",
            "source_domain": "two.example",
            "published_at": "2026-04-07T13:00:00Z",
        },
    ]
    signatures = [
        {
            "entities": {"Iran", "Israel"},
            "keywords": {"strike", "missile"},
            "anchors": {"strike"},
            "published_at": "2026-04-07T12:00:00Z",
            "published_dt": clustering_module._parse_published_at("2026-04-07T12:00:00Z"),
        },
        {
            "entities": {"Iran", "Israel"},
            "keywords": {"strike", "missile", "response"},
            "anchors": {"strike"},
            "published_at": "2026-04-07T13:00:00Z",
            "published_dt": clustering_module._parse_published_at("2026-04-07T13:00:00Z"),
        },
    ]

    with patch.object(
        clustering_module,
        "build_article_signatures",
        return_value=signatures,
    ), patch.object(
        clustering_module,
        "_source_profile",
        return_value={
            "source_type": "article",
            "trust_tier": "tier_1",
            "region": "global",
            "quality_weight": 1.3,
        },
    ), patch.object(
        clustering_module,
        "_select_consensus_title",
        return_value="Consensus title",
    ), patch.object(
        clustering_module,
        "_select_consensus_summary",
        return_value="Consensus summary",
    ):
        events = clustering_module.cluster_articles(articles, topic="geopolitics")

    assert len(events) == 1
    event = events[0]
    expected_keys = {
        "event_id",
        "topic",
        "label",
        "summary",
        "entity_focus",
        "source_count",
        "article_count",
        "latest_update",
        "earliest_update",
        "story_anchor_focus",
        "source_quality_score",
        "official_source_count",
        "structured_source_count",
        "monitored_channel_count",
        "tier_1_source_count",
        "region_counts",
        "dominant_region",
        "articles",
    }
    assert expected_keys.issubset(set(event.keys()))
    assert event["event_id"].startswith("geopolitics-")
    assert event["topic"] == "geopolitics"
    assert event["label"] == "Consensus title"
    assert event["summary"] == "Consensus summary"
