import os
import re
from pathlib import Path
from threading import Lock

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)


def split_csv_env(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
DEFAULT_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:5175",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5174",
    "http://127.0.0.1:5175",
    "http://127.0.0.1:5176",
]
CORS_ORIGINS = split_csv_env(os.getenv("OTHELLO_CORS_ORIGINS")) or DEFAULT_CORS_ORIGINS

# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------
TOPICS = ["geopolitics", "economics"]
BRIEFING_TOPICS = ["geopolitics", "economics", "conflict"]

# ---------------------------------------------------------------------------
# TTLs and windows
# ---------------------------------------------------------------------------
BRIEFING_TTL = 3600
HEADLINES_TTL = 1200
CORPUS_WINDOW_HOURS = 96
MIN_TOPIC_ARTICLES = 12
ATTENTION_WINDOW_HOURS = {
    "24h": 24,
    "7d": 24 * 7,
    "30d": 24 * 30,
    "90d": 24 * 90,
    "180d": 24 * 180,
    "365d": 24 * 365,
    "1825d": 24 * 365 * 5,
}
MAP_CACHE_TTL_SECONDS = 600
MAX_MAP_STRUCTURED_DAYS = 60
MAX_MAP_STRUCTURED_LIMIT = 3000
MAX_MAP_STORY_HOURS = 24 * 90

# ---------------------------------------------------------------------------
# Text-classification patterns
# ---------------------------------------------------------------------------
CONFLICT_TEXT_PATTERNS = (
    "airstrike", "missile", "drone", "shelling", "artillery", "troops",
    "battle", "fighting", "attack", "offensive", "raid", "explosion",
    "bomb", "clash", "frontline", "insurgent", "rocket fire", "drone strike",
    "missile strike", "military operation", "killed in", "struck",
)
POLITICAL_TEXT_PATTERNS = (
    "election", "government", "parliament", "diplom", "minister", "president",
    "policy", "sanction", "coalition", "cabinet", "vote", "protest",
)
ECONOMIC_TEXT_PATTERNS = (
    "market", "econom", "trade", "tariff", "inflation", "gdp", "oil", "gas",
    "supply chain", "rate", "bank", "currency", "debt", "investment",
)

# ---------------------------------------------------------------------------
# Regex
# ---------------------------------------------------------------------------
DATELINE_RE = re.compile(
    r"^\s*(?:[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){0,4}|[A-Z]{2,}(?:\s+[A-Z]{2,}){0,4})(?:,\s*[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){0,3})?\s*\((?:AP|Reuters|AFP)\)\s*[—-]\s*"
)

# ---------------------------------------------------------------------------
# Hotspot weights
# ---------------------------------------------------------------------------
HOTSPOT_EVENT_TYPE_WEIGHTS = {
    "Battles": 4.8,
    "Violence against civilians": 4.4,
    "Explosions/Remote violence": 4.1,
    "Riots": 2.8,
    "Strategic developments": 2.4,
    "Protests": 1.9,
}

# ---------------------------------------------------------------------------
# GDELT backfill
# ---------------------------------------------------------------------------
GDELT_BACKFILL_WINDOW_HOURS = int(os.getenv("OTHELLO_GDELT_BACKFILL_WINDOW_HOURS", "3"))
GDELT_BACKFILL_LAG_MINUTES = int(os.getenv("OTHELLO_GDELT_BACKFILL_LAG_MINUTES", "90"))
GDELT_BACKFILL_PAGE_SIZE = int(os.getenv("OTHELLO_GDELT_BACKFILL_PAGE_SIZE", "12"))
GDELT_BACKFILL_MIN_WINDOW_HOURS = int(os.getenv("OTHELLO_GDELT_BACKFILL_MIN_WINDOW_HOURS", "1"))
GDELT_BACKFILL_CHROMA = os.getenv("OTHELLO_GDELT_BACKFILL_CHROMA", "false").lower() == "true"
GDELT_BACKFILL_START = os.getenv("OTHELLO_GDELT_BACKFILL_START")
GDELT_BACKFILL_RETRY_MINUTES = int(os.getenv("OTHELLO_GDELT_BACKFILL_RETRY_MINUTES", "45"))
GDELT_BACKFILL_RATE_LIMIT_RETRY_MINUTES = int(os.getenv("OTHELLO_GDELT_BACKFILL_RATE_LIMIT_RETRY_MINUTES", "180"))

# ---------------------------------------------------------------------------
# Refresh intervals
# ---------------------------------------------------------------------------
SOURCE_REGISTRY_REFRESH_MINUTES = int(os.getenv("OTHELLO_SOURCE_REGISTRY_REFRESH_MINUTES", "60"))
DIRECT_FEED_REFRESH_MINUTES = int(os.getenv("OTHELLO_DIRECT_FEED_REFRESH_MINUTES", "20"))
SOURCE_REGISTRY_MIRROR_HOURS = int(os.getenv("OTHELLO_SOURCE_REGISTRY_MIRROR_HOURS", "336"))
OFFICIAL_UPDATE_REFRESH_MINUTES = int(os.getenv("OTHELLO_OFFICIAL_UPDATE_REFRESH_MINUTES", "180"))
ACLED_REFRESH_MINUTES = int(os.getenv("OTHELLO_ACLED_REFRESH_MINUTES", "240"))
GDELT_GKG_REFRESH_MINUTES = int(os.getenv("OTHELLO_GDELT_GKG_REFRESH_MINUTES", "60"))
GDELT_GKG_REFRESH_HOURS = int(os.getenv("OTHELLO_GDELT_GKG_REFRESH_HOURS", "24"))
ARTICLE_FALLBACK_REFRESH_MINUTES = int(os.getenv("OTHELLO_ARTICLE_FALLBACK_REFRESH_MINUTES", "45"))
SOURCE_RELIABILITY_REFRESH_MINUTES = int(os.getenv("OTHELLO_SOURCE_RELIABILITY_REFRESH_MINUTES", "120"))
FORESIGHT_REFRESH_MINUTES = int(os.getenv("OTHELLO_FORESIGHT_REFRESH_MINUTES", "180"))
NARRATIVE_DRIFT_REFRESH_MINUTES = int(os.getenv("OTHELLO_NARRATIVE_DRIFT_REFRESH_MINUTES", "180"))
NARRATIVE_DRIFT_TOP_SUBJECTS = int(os.getenv("OTHELLO_NARRATIVE_DRIFT_TOP_SUBJECTS", "3"))
ANALYTICS_WARM_DELAY_SECONDS = int(os.getenv("OTHELLO_ANALYTICS_WARM_DELAY_SECONDS", "5"))
HISTORICAL_FETCH_REFRESH_MINUTES = int(os.getenv("OTHELLO_HISTORICAL_FETCH_REFRESH_MINUTES", "5"))
HISTORICAL_FETCH_BATCH_LIMIT = int(os.getenv("OTHELLO_HISTORICAL_FETCH_BATCH_LIMIT", "30"))
HISTORICAL_FETCH_WRITE_BATCH_SIZE = int(os.getenv("OTHELLO_HISTORICAL_FETCH_WRITE_BATCH_SIZE", "15"))
HISTORICAL_FETCH_DOMAIN_INTERVAL_SECONDS = float(os.getenv("OTHELLO_HISTORICAL_FETCH_DOMAIN_INTERVAL_SECONDS", "2.0"))
HISTORICAL_FETCH_MAX_ATTEMPTS = int(os.getenv("OTHELLO_HISTORICAL_FETCH_MAX_ATTEMPTS", "3"))
STORY_MATERIALIZATION_REFRESH_MINUTES = int(os.getenv("OTHELLO_STORY_MATERIALIZATION_REFRESH_MINUTES", "45"))

# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------
REQUEST_ENABLE_VECTOR_SEARCH = os.getenv("OTHELLO_ENABLE_VECTOR_SEARCH", "false").lower() == "true"
REQUEST_ENABLE_CHROMA_INGEST = os.getenv("OTHELLO_ENABLE_CHROMA_INGEST", "false").lower() == "true"
REQUEST_ENABLE_LIVE_FETCH = os.getenv("OTHELLO_ENABLE_LIVE_FETCH", "false").lower() == "true"
REQUEST_ENABLE_LLM_RESPONSES = os.getenv("OTHELLO_ENABLE_LLM_RESPONSES", "false").lower() == "true"
REQUEST_ENABLE_TRANSLATION = os.getenv("OTHELLO_ENABLE_TRANSLATION", "true").lower() == "true"
TRANSLATION_MIN_SCORE = int(os.getenv("OTHELLO_TRANSLATION_MIN_SCORE", "7"))
TRANSLATION_REMOTE_FALLBACK_SCORE = int(os.getenv("OTHELLO_TRANSLATION_REMOTE_FALLBACK_SCORE", "9"))

# ---------------------------------------------------------------------------
# Scheduler / worker flags
# ---------------------------------------------------------------------------
INTERNAL_SCHEDULER_ENABLED = os.getenv("OTHELLO_INTERNAL_SCHEDULER", "false").lower() == "true"
WORKER_BOOTSTRAP_MODE = os.getenv("OTHELLO_WORKER_BOOTSTRAP_MODE", "ingest").strip().lower()
WORKER_ENABLE_INGESTION = os.getenv("OTHELLO_WORKER_ENABLE_INGESTION", "true").lower() == "true"
WORKER_ENABLE_TRANSLATIONS = os.getenv("OTHELLO_WORKER_ENABLE_TRANSLATIONS", "false").lower() == "true"
WORKER_ENABLE_ANALYTICS = os.getenv("OTHELLO_WORKER_ENABLE_ANALYTICS", "false").lower() == "true"

# ---------------------------------------------------------------------------
# API key / admin
# ---------------------------------------------------------------------------
ADMIN_API_KEY = os.getenv("OTHELLO_ADMIN_API_KEY", "").strip()

# ---------------------------------------------------------------------------
# Locks (shared across services)
# ---------------------------------------------------------------------------
INGEST_JOB_LOCK = Lock()
BACKFILL_JOB_LOCK = Lock()
HISTORICAL_FETCH_JOB_LOCK = Lock()
STORY_MATERIALIZATION_JOB_LOCK = Lock()

# ---------------------------------------------------------------------------
# Query stopwords
# ---------------------------------------------------------------------------
QUERY_STOPWORDS = {
    "about", "across", "actually", "analysis", "analytical", "analyst", "and", "are", "article",
    "articles", "background", "beyond", "be", "brief", "briefing", "coming", "comprehensive",
    "conflict", "cover", "current", "currently", "deep", "dive", "direct", "events", "expect",
    "from", "give", "happening", "historical", "implications", "intelligence", "into", "key",
    "mainstream", "media", "missing", "motivations", "news", "of", "on", "or", "playing",
    "probability", "probabilitys", "precise", "role", "surface", "specific", "story", "talks",
    "that", "the", "their", "them", "they", "this", "underreporting", "what", "weeks", "who",
    "why", "with", "world", "would",
}


def corpus_topic_for_briefing(briefing_topic: str) -> str:
    """Conflict briefings use the geopolitics article corpus (no separate conflict topic lane)."""
    if briefing_topic == "conflict":
        return "geopolitics"
    return briefing_topic
