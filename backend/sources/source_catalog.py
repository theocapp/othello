import hashlib


def source_id_for(name: str) -> str:
    return hashlib.sha256(name.strip().lower().encode("utf-8")).hexdigest()[:16]


def _seed(
    source_name: str,
    source_domain: str | None,
    source_type: str,
    trust_tier: str,
    region: str,
    language: str,
    metadata: dict | None = None,
) -> dict:
    return {
        "source_id": source_id_for(source_name),
        "source_name": source_name,
        "source_domain": source_domain,
        "source_type": source_type,
        "trust_tier": trust_tier,
        "region": region,
        "language": language,
        "active": True,
        "metadata": metadata or {},
    }


SOURCE_PACKS = {
    "global_wires": {
        "label": "Global Wires",
        "description": "High-trust international headline feeds used for broad baseline coverage.",
        "source_types": ["article"],
        "default_limit_per_source": 14,
        "default_max_age_hours": 72,
    },
    "regional_flagships": {
        "label": "Regional Flagships",
        "description": "Major regional outlets that add geography-specific context and narrative diversity.",
        "source_types": ["article"],
        "default_limit_per_source": 12,
        "default_max_age_hours": 120,
    },
    "conflict_region_outlets": {
        "label": "Conflict-Region Outlets",
        "description": "Conflict-adjacent publications whose local framing often differs from global coverage.",
        "source_types": ["article"],
        "default_limit_per_source": 14,
        "default_max_age_hours": 120,
    },
    "official_un_judicial_feeds": {
        "label": "Official / UN / Judicial",
        "description": "Official institutions, UN bodies, sanctions services, courts, and gazettes.",
        "source_types": ["official_update", "structured_event"],
        "default_limit_per_source": 16,
        "default_max_age_hours": 168,
    },
    "economic_institutions": {
        "label": "Economic Institutions",
        "description": "Multilateral macro and policy institutions that shape economic narratives.",
        "source_types": ["official_update"],
        "default_limit_per_source": 16,
        "default_max_age_hours": 168,
    },
}


def source_pack_for(source: dict) -> str | None:
    metadata = source.get("metadata") or {}
    pack = metadata.get("pack")
    if pack:
        return pack
    source_type = source.get("source_type")
    collection = metadata.get("collection")
    if source_type in {"official_update", "structured_event"}:
        if collection == "macro":
            return "economic_institutions"
        return "official_un_judicial_feeds"
    return None


def source_in_pack(
    source: dict, packs: list[str] | tuple[str, ...] | set[str] | None
) -> bool:
    if not packs:
        return True
    return source_pack_for(source) in set(packs)


# Temporary quarantine list for feeds that currently return persistent auth blocks.
# Keep entries in the catalog for easy reactivation when upstream access recovers.
QUARANTINED_SOURCE_DOMAINS = {
    "reuters.com",
    "politico.com",
}


def source_is_blocked(source: dict) -> bool:
    metadata = source.get("metadata") or {}
    if metadata.get("blocked") is True:
        return True
    domain = (source.get("source_domain") or "").strip().lower()
    return domain in QUARANTINED_SOURCE_DOMAINS


# Minimal seed list kept for backward compatibility with top-level imports
# and bootstrap scripts. The original file contained a much larger catalog;
# during migration we preserve a small but representative set so tests
# and bootstrap logic can still operate.
SOURCE_SEEDS = [
    _seed(
        "BBC News",
        "bbc.co.uk",
        "article",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "rss",
            "pack": "global_wires",
            "feeds": [
                {"url": "https://feeds.bbci.co.uk/news/world/rss.xml", "topic_hints": ["geopolitics"]},
                {"url": "https://feeds.bbci.co.uk/news/business/rss.xml", "topic_hints": ["economics"]},
            ],
        },
    ),
    _seed(
        "Reuters",
        "reuters.com",
        "article",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "rss",
            "pack": "global_wires",
            # Reversible quarantine: Reuters RSS is returning 401 in production cycles.
            "blocked": True,
            "blocked_reason": "quarantined_rss_401",
            "feeds": [
                {
                    "url": "https://www.reuters.com/rssFeed/worldNews",
                    "topic_hints": ["geopolitics"],
                }
            ],
        },
    ),
    _seed(
        "Financial Times",
        "ft.com",
        "article",
        "tier_1",
        "global",
        "en",
        {"adapter": "gdelt_enriched", "pack": "global_wires"},
    ),
    _seed(
        "ACLED",
        "acleddata.com",
        "structured_event",
        "tier_1",
        "global",
        "en",
        {"adapter": "acled_oauth", "dataset": "acled", "requires_env": ["ACLED_EMAIL", "ACLED_PASSWORD"]},
    ),
    _seed(
        "UNHCR",
        "unhcr.org/us",
        "official_update",
        "tier_1",
        "global",
        "en",
        {"adapter": "official_page_listing", "pack": "official_un_judicial_feeds", "collection": "humanitarian", "pages": ["https://www.unhcr.org/us", "https://www.unhcr.org/us/news/press-releases"], "allowed_domains": ["www.unhcr.org", "unhcr.org"], "mirror_to_articles": True},
    ),
    _seed(
        "Politico",
        "politico.com",
        "article",
        "tier_2",
        "north-america",
        "en",
        {
            "adapter": "rss",
            "pack": "regional_flagships",
            # Reversible quarantine: Politico RSS is returning 403 in production cycles.
            "blocked": True,
            "blocked_reason": "quarantined_rss_403",
            "feeds": [
                {
                    "url": "https://www.politico.com/rss/politics08.xml",
                    "topic_hints": ["geopolitics"],
                }
            ],
        },
    ),
]

