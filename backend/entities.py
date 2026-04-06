import importlib.util
import os
import spacy
import sqlite3
from datetime import datetime, timedelta

# ─── Model ────────────────────────────────────────────────────────────────────
# Install: python3 -m spacy download en_core_web_sm
_NLP_CACHE = {}
_MODEL_NAME_CACHE = {}

LANGUAGE_MODEL_CANDIDATES = {
    "en": ["en_core_web_sm", "en_core_web_md", "en_core_web_lg"],
    "fr": ["fr_core_news_sm", "fr_core_news_md", "fr_core_news_lg"],
    "es": ["es_core_news_lg", "es_core_news_md", "es_core_news_sm"],
    "de": ["de_core_news_lg", "de_core_news_md", "de_core_news_sm"],
    "pt": ["pt_core_news_lg", "pt_core_news_md", "pt_core_news_sm"],
    "it": ["it_core_news_lg", "it_core_news_md", "it_core_news_sm"],
    "nl": ["nl_core_news_lg", "nl_core_news_md", "nl_core_news_sm"],
    "ca": ["ca_core_news_lg", "ca_core_news_md", "ca_core_news_sm"],
    "zh": ["zh_core_web_lg", "zh_core_web_md", "zh_core_web_sm"],
    "ja": ["ja_core_news_lg", "ja_core_news_md", "ja_core_news_sm"],
}
MULTILINGUAL_MODEL_CANDIDATES = ["xx_ent_wiki_sm"]


def _language_key(language: str | None) -> str:
    value = (language or "").strip().lower()
    if not value:
        return "en"
    aliases = {
        "english": "en",
        "en-us": "en",
        "en-gb": "en",
        "french": "fr",
        "spanish": "es",
        "german": "de",
        "portuguese": "pt",
        "italian": "it",
        "dutch": "nl",
        "catalan": "ca",
        "chinese": "zh",
        "japanese": "ja",
    }
    if value in aliases:
        return aliases[value]
    for code in LANGUAGE_MODEL_CANDIDATES:
        if value == code or value.startswith(f"{code}-") or value.startswith(f"{code}_"):
            return code
    return value.split("-")[0].split("_")[0]


def _candidate_models(language: str | None, include_english_fallback: bool = True) -> list[str]:
    key = _language_key(language)
    candidates = list(LANGUAGE_MODEL_CANDIDATES.get(key, []))
    if key != "en":
        candidates.extend(MULTILINGUAL_MODEL_CANDIDATES)
    if include_english_fallback and not any(c.startswith("en_core_web") for c in candidates):
        candidates.extend(["en_core_web_sm", "en_core_web_md", "en_core_web_lg"])
    return candidates


def _resolve_model_name(language: str | None, include_english_fallback: bool = True) -> str:
    key = _language_key(language)
    cached = _MODEL_NAME_CACHE.get(key)
    if cached:
        if include_english_fallback:
            return cached
        if not cached.startswith("en_core_web"):
            return cached

    last_error = None
    for model_name in _candidate_models(language, include_english_fallback=include_english_fallback):
        try:
            model = spacy.load(model_name)
            _NLP_CACHE[key] = model
            _MODEL_NAME_CACHE[key] = model_name
            return model_name
        except OSError as exc:
            last_error = exc

    if include_english_fallback:
        raise RuntimeError(
            "No compatible spaCy model is available for entity extraction. "
            "Install at minimum: python3 -m spacy download en_core_web_sm"
        ) from last_error
    raise RuntimeError(f"No non-English spaCy model is available for language '{key}'.") from last_error


def get_nlp(language: str | None = None):
    key = _language_key(language)
    if key in _NLP_CACHE:
        return _NLP_CACHE[key]

    model_name = _resolve_model_name(language)
    return _NLP_CACHE[key]


def has_native_language_model(language: str | None) -> bool:
    key = _language_key(language)
    if key == "en":
        return True
    model_name = _MODEL_NAME_CACHE.get(key)
    if model_name:
        return not model_name.startswith("en_core_web")
    try:
        _resolve_model_name(language, include_english_fallback=False)
        return True
    except RuntimeError:
        return False


def _classify_model_path(model_name: str | None) -> str:
    if not model_name or model_name.startswith("en_core_web"):
        return "english_default"
    if model_name in MULTILINGUAL_MODEL_CANDIDATES:
        return "multilingual_fallback"
    return "native_specific"


def _find_installed_model(language: str | None, include_english_fallback: bool = True) -> str | None:
    key = _language_key(language)
    cached = _MODEL_NAME_CACHE.get(key)
    if cached:
        return cached

    for model_name in _candidate_models(language, include_english_fallback=include_english_fallback):
        if importlib.util.find_spec(model_name):
            return model_name
    return None


def describe_entity_extraction(article: dict) -> dict:
    article_language = article.get("translation_source_language") or article.get("language") or "en"
    normalized_language = _language_key(article_language)
    has_translation = bool(article.get("translated_title") or article.get("translated_description"))

    if has_native_language_model(article_language):
        model_name = _resolve_model_name(article_language, include_english_fallback=False)
        extraction_language = article_language
        path = _classify_model_path(model_name)
        text_source = "original"
    elif has_translation:
        model_name = _resolve_model_name("en")
        extraction_language = "en"
        path = "translated_english"
        text_source = "translated"
    else:
        model_name = _resolve_model_name(article_language)
        extraction_language = article_language
        path = _classify_model_path(model_name)
        text_source = "original"

    return {
        "article_language": normalized_language,
        "extraction_language": _language_key(extraction_language),
        "model_name": model_name,
        "path": path,
        "text_source": text_source,
    }


def get_entity_model_capabilities() -> dict:
    tracked_languages = ["en", "fr", "es", "de", "pt", "it", "zh", "ar", "uk", "tr", "he"]
    language_support = {}
    path_totals = {
        "native_specific": 0,
        "multilingual_fallback": 0,
        "english_default": 0,
    }

    for language in tracked_languages:
        if language == "en":
            model_name = _find_installed_model("en")
            path = _classify_model_path(model_name)
            supported = bool(model_name)
        else:
            model_name = _find_installed_model(language, include_english_fallback=False)
            path = _classify_model_path(model_name) if model_name else "unavailable"
            supported = bool(model_name)

        language_support[language] = {
            "supported": supported,
            "model_name": model_name,
            "path": path,
        }
        if path in path_totals and supported:
            path_totals[path] += 1

    return {
        "tracked_languages": tracked_languages,
        "language_support": language_support,
        "path_totals": path_totals,
    }

# ─── Config ───────────────────────────────────────────────────────────────────
RELEVANT_TYPES = {"PERSON", "GPE", "ORG", "NORP"}

# Hard blocklist — pure noise, never meaningful signal under any circumstances
BLOCKLIST = {
    "government", "administration", "officials", "authorities", "spokesperson",
    "minister", "president", "prime minister", "secretary", "department",
    "committee", "parliament", "analysts", "experts", "sources", "investors",
    "traders", "police", "military", "army", "navy", "court", "media", "press",
    "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
    "january", "february", "march", "april", "may", "june", "july", "august",
    "september", "october", "november", "december",
    "reuters", "ap", "associated press", "bloomberg", "bbc", "cnn", "fox news",
    "the new york times", "the washington post", "wall street journal",
    "western", "eastern", "northern", "southern",
}

# High-frequency entities — real actors but appear constantly
# Only surface if spike ratio exceeds HIGH_FREQUENCY_THRESHOLD
HIGH_FREQUENCY = {
    "united states", "the united states", "american", "americans",
    "congress", "senate", "white house", "pentagon", "kremlin",
    "european union", "united nations", "nato", "imf", "world bank",
    "supreme court", "the supreme court", "justice department", "capitol hill",
    "federal government", "u.s. government", "state department",
    "republican party", "democratic party", "gop",
    "russia", "china", "iran", "israel", "ukraine",
    "federal reserve", "wall street",
}

HIGH_FREQUENCY_THRESHOLD = 3.0  # must spike 3x to surface
DEFAULT_THRESHOLD = 1.5          # normal entities need 1.5x

# Alias map — normalize variants to canonical names
# None = discard entirely
ALIASES = {
    # US — keep but normalize
    "u.s.": "United States",
    "usa": "United States",
    "america": "United States",
    "the united states": "United States",
    # UK
    "u.k.": "United Kingdom",
    "uk": "United Kingdom",
    "britain": "United Kingdom",
    "great britain": "United Kingdom",
    # People
    "donald trump": "Trump",
    "trump": "Trump",
    "president trump": "Trump",
    "joe biden": "Biden",
    "biden": "Biden",
    "president biden": "Biden",
    "vladimir putin": "Putin",
    "putin": "Putin",
    "xi jinping": "Xi Jinping",
    "xi": "Xi Jinping",
    "benjamin netanyahu": "Netanyahu",
    "netanyahu": "Netanyahu",
    "bibi": "Netanyahu",
    "volodymyr zelensky": "Zelensky",
    "zelensky": "Zelensky",
    "zelenskyy": "Zelensky",
    "elon musk": "Elon Musk",
    "musk": "Elon Musk",
    "emmanuel macron": "Macron",
    "macron": "Macron",
    "olaf scholz": "Scholz",
    "keir starmer": "Starmer",
    "jerome powell": "Powell",
    "janet yellen": "Yellen",
    # Countries
    "russia": "Russia",
    "ukraine": "Ukraine",
    "china": "China",
    "iran": "Iran",
    "israel": "Israel",
    "gaza": "Gaza",
    "taiwan": "Taiwan",
    # Orgs
    "fed": "Federal Reserve",
    "the fed": "Federal Reserve",
    "federal reserve": "Federal Reserve",
    "gop": "Republican Party",
    "republican party": "Republican Party",
    "democratic party": "Democratic Party",
    "hamas": "Hamas",
    "hezbollah": "Hezbollah",
    "idf": "IDF",
    "israel defense forces": "IDF",
    "the supreme court": "Supreme Court",
    "supreme court": "Supreme Court",
    # Discard
    "fda": None,
    "fsp": None,
    "epa": None,
    "fbi": None,
    "cia": None,
    "nsa": None,
    "doj": None,
    "dod": None,
    "us": None,  # too ambiguous — "us" vs "US"
}

DB_PATH = "./entities.db"
SQLITE_TIMEOUT_SECONDS = float(os.getenv("OTHELLO_SQLITE_TIMEOUT_SECONDS", "30"))


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc).lower():
            raise
    return conn

# ─── DB init ──────────────────────────────────────────────────────────────────
def init_db():
    conn = _connect()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS entity_mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            topic TEXT NOT NULL,
            article_url TEXT NOT NULL,
            mentioned_at TIMESTAMP NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS entity_cooccurrences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_a TEXT NOT NULL,
            entity_b TEXT NOT NULL,
            topic TEXT NOT NULL,
            article_url TEXT NOT NULL,
            mentioned_at TIMESTAMP NOT NULL
        )
    """)

    c.execute("CREATE INDEX IF NOT EXISTS idx_entity ON entity_mentions(entity)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_mentioned_at ON entity_mentions(mentioned_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cooc_a ON entity_cooccurrences(entity_a)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cooc_b ON entity_cooccurrences(entity_b)")
    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_entity_mention
        ON entity_mentions(topic, article_url, entity)
    """)
    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_entity_cooccurrence
        ON entity_cooccurrences(topic, article_url, entity_a, entity_b)
    """)

    conn.commit()
    conn.close()
    print("[entities] Database initialized")

# ─── Normalization ────────────────────────────────────────────────────────────
def normalize_entity(text: str) -> str | None:
    """
    Normalize entity text:
    1. Check alias map — return canonical name or None to discard
    2. Check blocklist — discard if found
    3. Apply noise filters
    4. Title case fallback
    """
    cleaned = text.strip()
    lower = cleaned.lower()

    # Alias map check — handles normalization and discarding
    if lower in ALIASES:
        return ALIASES[lower]

    # Blocklist check
    if lower in BLOCKLIST:
        return None

    # Filter very short or very long
    if len(cleaned) < 3 or len(cleaned) > 60:
        return None

    # Filter purely numeric
    if cleaned.replace(",", "").replace(".", "").isdigit():
        return None

    # Filter punctuation artifacts from headlines
    if any(char in cleaned for char in ["'S", "|", "/"]):
        return None

    # Filter suspiciously long entity names
    if len(cleaned.split()) > 5:
        return None

    # Filter all-caps abbreviations under 4 characters
    if len(cleaned) <= 3 and cleaned.isupper():
        return None

    return cleaned.title()


# ─── Extraction ───────────────────────────────────────────────────────────────
def extract_entities(text: str, language: str | None = None) -> list[dict]:
    """Extract and normalize named entities from text using the best available spaCy model."""
    doc = get_nlp(language)(text)
    entities = []
    seen = set()

    for ent in doc.ents:
        if ent.label_ not in RELEVANT_TYPES:
            continue

        normalized = normalize_entity(ent.text)
        if normalized is None:
            continue

        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)

        entities.append({
            "entity": normalized,
            "type": ent.label_,
        })

    return entities

# ─── Storage ──────────────────────────────────────────────────────────────────
def store_entity_mentions(articles: list[dict], topic: str):
    """Extract entities, store mentions and co-occurrences."""
    conn = _connect()
    c = conn.cursor()
    now = datetime.now().isoformat()
    total_mentions = 0
    total_cooc = 0
    path_counts = {}
    model_counts = {}
    language_counts = {}
    language_paths = {}

    for article in articles:
        extraction = describe_entity_extraction(article)
        article_language = extraction["article_language"]
        path = extraction["path"]
        model_name = extraction["model_name"]

        if extraction["text_source"] == "original":
            title = article.get("original_title") or article.get("title") or ""
            description = article.get("original_description") or article.get("description") or ""
        else:
            title = article.get("translated_title") or article.get("title") or article.get("original_title") or ""
            description = article.get("translated_description") or article.get("description") or article.get("original_description") or ""
        extraction_language = extraction["extraction_language"]
        text = f"{title}. {description}"
        entities = extract_entities(text, language=extraction_language)

        language_counts[article_language] = language_counts.get(article_language, 0) + 1
        path_counts[path] = path_counts.get(path, 0) + 1
        if model_name:
            model_counts[model_name] = model_counts.get(model_name, 0) + 1
        language_entry = language_paths.setdefault(
            article_language,
            {
                "articles": 0,
                "path_counts": {},
                "model_counts": {},
                "text_sources": {},
            },
        )
        language_entry["articles"] += 1
        language_entry["path_counts"][path] = language_entry["path_counts"].get(path, 0) + 1
        if model_name:
            language_entry["model_counts"][model_name] = language_entry["model_counts"].get(model_name, 0) + 1
        text_source = extraction["text_source"]
        language_entry["text_sources"][text_source] = language_entry["text_sources"].get(text_source, 0) + 1

        for entity in entities:
            c.execute("""
                INSERT INTO entity_mentions (entity, entity_type, topic, article_url, mentioned_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(topic, article_url, entity) DO NOTHING
            """, (entity["entity"], entity["type"], topic, article["url"], now))
            total_mentions += c.rowcount

        for i in range(len(entities)):
            for j in range(i + 1, len(entities)):
                a = entities[i]["entity"]
                b = entities[j]["entity"]
                if a > b:
                    a, b = b, a
                c.execute("""
                    INSERT INTO entity_cooccurrences (entity_a, entity_b, topic, article_url, mentioned_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(topic, article_url, entity_a, entity_b) DO NOTHING
                """, (a, b, topic, article["url"], now))
                total_cooc += c.rowcount

    conn.commit()
    conn.close()
    print(f"[entities] Stored {total_mentions} mentions, {total_cooc} co-occurrences for '{topic}'")
    return {
        "topic": topic,
        "articles_processed": len(articles),
        "mentions_written": total_mentions,
        "cooccurrences_written": total_cooc,
        "path_counts": path_counts,
        "model_counts": model_counts,
        "language_counts": language_counts,
        "language_paths": language_paths,
    }

# ─── Frequency / spike detection ─────────────────────────────────────────────
def get_entity_frequencies(days_recent: int = 2, days_baseline: int = 7, topic: str = None) -> list[dict]:
    """
    Compare recent vs baseline mentions to detect spikes.
    Applies tier-based thresholds — high-frequency entities need a bigger
    spike to be considered signal.
    """
    conn = _connect()
    c = conn.cursor()

    now = datetime.now()
    recent_cutoff = (now - timedelta(days=days_recent)).isoformat()
    baseline_cutoff = (now - timedelta(days=days_baseline)).isoformat()

    topic_filter = "AND topic = ?" if topic else ""
    params_recent = [recent_cutoff] + ([topic] if topic else [])
    params_baseline = [baseline_cutoff, recent_cutoff] + ([topic] if topic else [])

    # Deduplicate by entity name only (ignore spaCy type inconsistencies)
    c.execute(f"""
        SELECT entity, entity_type, COUNT(*) as count
        FROM entity_mentions
        WHERE mentioned_at > ?
        {topic_filter}
        GROUP BY entity
        ORDER BY count DESC
    """, params_recent)
    recent = {row[0]: {"type": row[1], "recent": row[2]} for row in c.fetchall()}

    c.execute(f"""
        SELECT entity, COUNT(*) as count
        FROM entity_mentions
        WHERE mentioned_at > ? AND mentioned_at <= ?
        {topic_filter}
        GROUP BY entity
    """, params_baseline)
    baseline = {row[0]: row[1] for row in c.fetchall()}

    conn.close()

    results = []
    for entity, data in recent.items():
        recent_count = data["recent"]
        baseline_count = baseline.get(entity, 0)

        if baseline_count == 0:
            spike_ratio = recent_count * 2
            trend = "NEW"
        else:
            spike_ratio = recent_count / (baseline_count / (days_baseline / days_recent))
            if spike_ratio > 1.5:
                trend = "RISING"
            elif spike_ratio < 0.5:
                trend = "FALLING"
            else:
                trend = "STABLE"

        # Tier-based threshold — high-frequency entities need a bigger spike
        threshold = HIGH_FREQUENCY_THRESHOLD if entity.lower() in HIGH_FREQUENCY else DEFAULT_THRESHOLD

        if spike_ratio >= threshold:
            results.append({
                "entity": entity,
                "type": data["type"],
                "recent_mentions": recent_count,
                "baseline_mentions": baseline_count,
                "spike_ratio": round(spike_ratio, 2),
                "trend": trend,
            })

    results.sort(key=lambda x: x["spike_ratio"], reverse=True)
    return results[:20]

def get_top_entities(topic: str = None, days: int = 7, limit: int = 10) -> list[dict]:
    """Get most mentioned entities over a time period, deduplicated by name."""
    conn = _connect()
    c = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    topic_filter = "AND topic = ?" if topic else ""
    params = [cutoff] + ([topic] if topic else [])

    c.execute(f"""
        SELECT entity, entity_type, COUNT(*) as count
        FROM entity_mentions
        WHERE mentioned_at > ?
        {topic_filter}
        GROUP BY entity
        ORDER BY count DESC
        LIMIT ?
    """, params + [limit])

    results = [{"entity": row[0], "type": row[1], "mentions": row[2]} for row in c.fetchall()]
    conn.close()
    return results

# ─── Co-occurrence / relationships ───────────────────────────────────────────
def get_entity_relationships(entity: str, days: int = 7, limit: int = 10) -> list[dict]:
    """Get entities most frequently co-mentioned with a given entity."""
    conn = _connect()
    c = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    c.execute("""
        SELECT
            CASE WHEN entity_a = ? THEN entity_b ELSE entity_a END as related_entity,
            COUNT(*) as co_mentions
        FROM entity_cooccurrences
        WHERE (entity_a = ? OR entity_b = ?)
        AND mentioned_at > ?
        GROUP BY related_entity
        ORDER BY co_mentions DESC
        LIMIT ?
    """, (entity, entity, entity, cutoff, limit))

    results = [{"entity": row[0], "co_mentions": row[1]} for row in c.fetchall()]
    conn.close()
    return results

def get_relationship_graph(days: int = 7, min_cooccurrences: int = 2, topic: str = None) -> dict:
    """Return full entity relationship graph for visualization."""
    conn = _connect()
    c = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    topic_filter = "AND topic = ?" if topic else ""
    params = [cutoff] + ([topic] if topic else []) + [min_cooccurrences]

    c.execute(f"""
        SELECT entity_a, entity_b, COUNT(*) as weight
        FROM entity_cooccurrences
        WHERE mentioned_at > ?
        {topic_filter}
        GROUP BY entity_a, entity_b
        HAVING weight >= ?
        ORDER BY weight DESC
    """, params)

    edges = [{"source": row[0], "target": row[1], "weight": row[2]} for row in c.fetchall()]

    nodes = set()
    for edge in edges:
        nodes.add(edge["source"])
        nodes.add(edge["target"])

    conn.close()
    return {
        "nodes": [{"id": n} for n in nodes],
        "edges": edges,
    }

# ─── Briefing signal formatter ────────────────────────────────────────────────
def format_signals_for_briefing(topic: str) -> str:
    """Generate signal summary to inject into briefing prompts."""
    spikes = get_entity_frequencies(topic=topic)
    top = get_top_entities(topic=topic)

    if not spikes and not top:
        return ""

    lines = ["ENTITY TRACKING SIGNALS (based on historical article analysis):"]

    rising = [e for e in spikes if e["trend"] in ("RISING", "NEW") and e["spike_ratio"] > 1.5][:5]
    if rising:
        lines.append("\nSurging mentions (potential emerging stories):")
        for e in rising:
            if e["trend"] == "NEW":
                lines.append(f"  - {e['entity']} ({e['type']}): NEW — {e['recent_mentions']} mentions, no prior history")
            else:
                lines.append(f"  - {e['entity']} ({e['type']}): {e['spike_ratio']}x spike — {e['recent_mentions']} recent vs {e['baseline_mentions']} baseline")

    if top:
        lines.append("\nMost discussed entities this week:")
        for e in top[:5]:
            lines.append(f"  - {e['entity']} ({e['type']}): {e['mentions']} mentions")

    return "\n".join(lines)
