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


SOURCE_SEEDS = [
    _seed("Reuters", "reuters.com", "article", "tier_1", "global", "en", {"adapter": "gdelt_enriched", "pack": "global_wires"}),
    _seed("Associated Press", "apnews.com", "article", "tier_1", "global", "en", {"adapter": "gdelt_enriched", "pack": "global_wires"}),
    _seed("AFP", "afp.com", "article", "tier_1", "global", "fr", {"adapter": "gdelt_enriched", "pack": "global_wires"}),
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
    _seed("Financial Times", "ft.com", "article", "tier_1", "global", "en", {"adapter": "gdelt_enriched", "pack": "global_wires"}),
    _seed(
        "Le Monde",
        "lemonde.fr",
        "article",
        "tier_1",
        "europe",
        "fr",
        {
            "adapter": "rss",
            "pack": "regional_flagships",
            "feeds": [
                {"url": "https://www.lemonde.fr/en/rss/une.xml", "topic_hints": ["geopolitics", "economics"]},
            ],
        },
    ),
    _seed("Al-Monitor", "al-monitor.com", "article", "tier_2", "middle-east", "en", {"adapter": "gdelt_enriched", "pack": "conflict_region_outlets"}),
    _seed(
        "The Diplomat",
        "thediplomat.com",
        "article",
        "tier_2",
        "asia-pacific",
        "en",
        {
            "adapter": "rss",
            "pack": "regional_flagships",
            "feeds": [
                {"url": "https://thediplomat.com/feed/", "topic_hints": ["geopolitics"]},
            ],
        },
    ),
    _seed(
        "African Arguments",
        "africanarguments.org",
        "article",
        "tier_2",
        "africa",
        "en",
        {
            "adapter": "rss",
            "pack": "regional_flagships",
            "feeds": [
                {"url": "https://africanarguments.org/feed/", "topic_hints": ["geopolitics"]},
            ],
        },
    ),
    _seed("Eurasia Daily Monitor", "jamestown.org", "article", "tier_2", "eurasia", "en", {"adapter": "gdelt_enriched", "pack": "conflict_region_outlets"}),
    _seed(
        "Middle East Eye",
        "middleeasteye.net",
        "article",
        "tier_2",
        "middle-east",
        "en",
        {
            "adapter": "rss",
            "pack": "conflict_region_outlets",
            "feeds": [
                {"url": "https://www.middleeasteye.net/rss", "topic_hints": ["geopolitics"]},
            ],
        },
    ),
    _seed("Agence Afrique", None, "article", "tier_2", "africa", "fr", {"adapter": "registry_only"}),
    _seed(
        "Dawn",
        "dawn.com",
        "article",
        "tier_2",
        "south-asia",
        "en",
        {
            "adapter": "rss",
            "pack": "regional_flagships",
            "feeds": [
                {"url": "https://www.dawn.com/feeds/home", "topic_hints": ["geopolitics", "economics"]},
            ],
        },
    ),
    _seed("Haaretz", "haaretz.com", "article", "tier_2", "middle-east", "en", {"adapter": "gdelt_enriched", "pack": "conflict_region_outlets"}),
    _seed("Novaya Gazeta Europe", "novayagazeta.eu", "article", "tier_2", "europe", "ru", {"adapter": "gdelt_enriched", "pack": "regional_flagships"}),
    _seed(
        "Al Jazeera English",
        "aljazeera.com",
        "article",
        "tier_1",
        "middle-east",
        "en",
        {
            "adapter": "rss",
            "pack": "conflict_region_outlets",
            "feeds": [
                {"url": "https://www.aljazeera.com/xml/rss/all.xml", "topic_hints": ["geopolitics", "economics"]},
            ],
        },
    ),
    _seed(
        "ACLED",
        "acleddata.com",
        "structured_event",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "acled_oauth",
            "dataset": "acled",
            "requires_env": ["ACLED_EMAIL", "ACLED_PASSWORD"],
        },
    ),
    _seed(
        "UNHCR",
        "unhcr.org/us",
        "official_update",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "official_page_listing",
            "pack": "official_un_judicial_feeds",
            "collection": "humanitarian",
            "pages": [
                "https://www.unhcr.org/us",
                "https://www.unhcr.org/us/news/press-releases",
            ],
            "allowed_domains": ["www.unhcr.org", "unhcr.org"],
            "allowed_href_parts": ["/us/news/", "/us/news/press-releases", "/us/news/briefing-notes"],
            "title_keywords": ["unhcr", "displaced", "refugee", "asylum", "briefing", "protection", "returns", "emergency"],
            "mirror_to_articles": True,
        },
    ),
    _seed(
        "UN OCHA",
        "unocha.org",
        "official_update",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "official_page_listing",
            "pack": "official_un_judicial_feeds",
            "collection": "humanitarian",
            "pages": [
                "https://reports.unocha.org/en/",
                "https://www.unocha.org/publications",
            ],
            "allowed_domains": ["www.unocha.org", "unocha.org", "reports.unocha.org"],
            "allowed_href_parts": ["/publications/", "reports.unocha.org/en/", "/situation-report/"],
            "title_keywords": ["ocha", "humanitarian", "flash update", "situation report", "appeal", "funding"],
            "mirror_to_articles": True,
        },
    ),
    _seed(
        "World Food Programme",
        "wfp.org",
        "official_update",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "official_page_listing",
            "pack": "official_un_judicial_feeds",
            "collection": "humanitarian",
            "pages": [
                "https://www.wfp.org/news",
                "https://www.wfp.org/stories",
            ],
            "allowed_domains": ["www.wfp.org", "wfp.org"],
            "allowed_href_parts": ["/news/", "/stories/"],
            "title_keywords": ["wfp", "food", "hunger", "nutrition", "aid", "emergency"],
            "mirror_to_articles": True,
        },
    ),
    _seed(
        "OFAC",
        "ofac.treasury.gov",
        "official_update",
        "tier_1",
        "united-states",
        "en",
        {
            "adapter": "ofac_recent_actions",
            "pack": "official_un_judicial_feeds",
            "collection": "sanctions",
            "recent_actions_url": "https://ofac.treasury.gov/recent-actions",
            "mirror_to_articles": True,
        },
    ),
    _seed(
        "OFAC Sanctions List Updates",
        "ofac.treasury.gov",
        "official_update",
        "tier_1",
        "united-states",
        "en",
        {
            "adapter": "ofac_sanctions_updates",
            "pack": "official_un_judicial_feeds",
            "collection": "sanctions",
            "recent_actions_url": "https://ofac.treasury.gov/recent-actions",
            "match_keywords": ["designation", "sanctions list", "sdn", "removal", "delisting", "non-proliferation", "counter terrorism", "counter narcotics", "magnitsky"],
            "mirror_to_articles": True,
        },
    ),
    _seed(
        "IMF",
        "imf.org",
        "official_update",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "imf_rss",
            "pack": "economic_institutions",
            "collection": "macro",
            "rss_directory_url": "https://www.imf.org/en/News/RSS",
            "mirror_to_articles": True,
        },
    ),
    _seed(
        "World Bank",
        "worldbank.org",
        "official_update",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "world_bank_news",
            "pack": "economic_institutions",
            "collection": "macro",
            "news_url": "https://www.worldbank.org/en/news",
            "mirror_to_articles": True,
        },
    ),
    _seed(
        "UN Security Council",
        "press.un.org",
        "official_update",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "unsc_press_pages",
            "pack": "official_un_judicial_feeds",
            "collection": "multilateral",
            "pages": [
                "https://press.un.org/en/security-council",
                "https://press.un.org/en/content/security-council/press-release",
                "https://press.un.org/en/content/security-council/press-conference",
            ],
            "mirror_to_articles": True,
        },
    ),
    _seed(
        "ICC Press Releases",
        "icc-cpi.int",
        "official_update",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "icc_press_pages",
            "pack": "official_un_judicial_feeds",
            "collection": "judicial",
            "pages": [
                "https://www.icc-cpi.int/news",
                "https://asp.icc-cpi.int/press-releases",
            ],
            "mirror_to_articles": True,
        },
    ),
    _seed(
        "ICC Filings",
        "icc-cpi.int",
        "official_update",
        "tier_1",
        "global",
        "en",
        {
            "adapter": "icc_filing_pages",
            "pack": "official_un_judicial_feeds",
            "collection": "judicial",
            "pages": [
                "https://www.icc-cpi.int/case-records?page=0",
                "https://asp.icc-cpi.int/ASPDocIndex",
            ],
            "mirror_to_articles": False,
        },
    ),
    _seed(
        "The Gazette",
        "thegazette.co.uk",
        "official_update",
        "tier_2",
        "united-kingdom",
        "en",
        {
            "adapter": "gazette_notices",
            "pack": "official_un_judicial_feeds",
            "collection": "gazette",
            "pages": [
                "https://www.thegazette.co.uk/all-notices",
            ],
            "mirror_to_articles": False,
        },
    ),
    _seed(
        "Conflict Telegram Monitor",
        None,
        "monitored_channel",
        "tier_3",
        "selective",
        "multi",
        {"adapter": "manual_channel", "review_required": True},
    ),
]


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


def source_in_pack(source: dict, packs: list[str] | tuple[str, ...] | set[str] | None) -> bool:
    if not packs:
        return True
    return source_pack_for(source) in set(packs)
