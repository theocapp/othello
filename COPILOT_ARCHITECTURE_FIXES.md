# Architecture Fix Instructions

Six structural changes to the Othello V2 backend. Each section is self-contained.
Implement in the order listed. Do not refactor code outside the described scope.
Do not add docstrings or comments to unchanged code.

---

## Fix 1 — Migrate entity storage from SQLite to PostgreSQL

**Files changed:**
- `backend/entities.py`
- `backend/db/schema.py`

**Problem:** Entity mentions, co-occurrences, and knowledge-base links are stored in a
local `entities.db` SQLite file. SQLite serialises all writes, causing lock contention
under concurrent scheduler jobs. The retry logic in `_store_entity_mentions_with_translation()`
exists specifically to work around this. Entity data also cannot be joined with the main
PostgreSQL corpus in SQL queries.

---

### Step 1A — Add entity tables to the PostgreSQL schema

In `backend/db/schema.py`, inside the `initialize_schema()` function, add the following
three table definitions after the existing table blocks. Use the same `conn.execute()`
pattern already used throughout that function:

```python
conn.execute("""
    CREATE TABLE IF NOT EXISTS entity_mentions (
        id          BIGSERIAL PRIMARY KEY,
        entity      TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        topic       TEXT NOT NULL,
        article_url TEXT NOT NULL,
        mentioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (topic, article_url, entity)
    )
""")
conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_entity_mentions_entity "
    "ON entity_mentions (entity)"
)
conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_entity_mentions_mentioned_at "
    "ON entity_mentions (mentioned_at)"
)

conn.execute("""
    CREATE TABLE IF NOT EXISTS entity_cooccurrences (
        id          BIGSERIAL PRIMARY KEY,
        entity_a    TEXT NOT NULL,
        entity_b    TEXT NOT NULL,
        topic       TEXT NOT NULL,
        article_url TEXT NOT NULL,
        mentioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (topic, article_url, entity_a, entity_b)
    )
""")
conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_entity_cooc_a "
    "ON entity_cooccurrences (entity_a)"
)
conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_entity_cooc_b "
    "ON entity_cooccurrences (entity_b)"
)

conn.execute("""
    CREATE TABLE IF NOT EXISTS entity_links (
        entity       TEXT NOT NULL,
        qid          TEXT NOT NULL,
        label        TEXT,
        description  TEXT,
        source       TEXT,
        retrieved_at TIMESTAMPTZ,
        PRIMARY KEY (entity, qid)
    )
""")
conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_entity_links_entity "
    "ON entity_links (entity)"
)
```

---

### Step 1B — Rewrite the database layer in `entities.py`

Replace the SQLite connection and init block. Remove these lines entirely:

```python
import sqlite3
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
```

Replace `_connect()` with a call to the shared PostgreSQL connector:

```python
from db.common import _connect
```

This `_connect` is already used throughout the rest of the backend and returns a
PostgreSQL connection context manager.

---

### Step 1C — Rewrite `init_db()` in `entities.py`

The tables now live in `initialize_schema()`. Replace the full `init_db()` body:

```python
def init_db():
    from db.schema import initialize_schema
    initialize_schema()
    print("[entities] Database initialized (PostgreSQL)")
```

---

### Step 1D — Rewrite `store_entity_mentions()` in `entities.py`

The function body runs from line 617 to ~724. Replace all SQLite-style queries with
PostgreSQL equivalents. The key changes are:

1. Replace `?` parameter placeholders with `%s` throughout.
2. Change the connection usage from `conn = _connect(); c = conn.cursor(); conn.commit();
   conn.close()` to `with _connect() as conn:` (the PostgreSQL `_connect` is a context
   manager that auto-commits and auto-closes).
3. Replace SQLite `ON CONFLICT(cols) DO NOTHING` with PostgreSQL
   `ON CONFLICT (cols) DO NOTHING`.

Replace the entire function body with:

```python
def store_entity_mentions(articles: list[dict], topic: str):
    """Extract entities, store mentions and co-occurrences."""
    now = datetime.now(timezone.utc).isoformat()
    total_mentions = 0
    total_cooc = 0
    path_counts = {}
    model_counts = {}
    language_counts = {}
    language_paths = {}

    with _connect() as conn:
        for article in articles:
            extraction = describe_entity_extraction(article)
            article_language = extraction["article_language"]
            path = extraction["path"]
            model_name = extraction["model_name"]

            if extraction["text_source"] == "original":
                title = article.get("original_title") or article.get("title") or ""
                description = (
                    article.get("original_description") or article.get("description") or ""
                )
            else:
                title = (
                    article.get("translated_title")
                    or article.get("title")
                    or article.get("original_title")
                    or ""
                )
                description = (
                    article.get("translated_description")
                    or article.get("description")
                    or article.get("original_description")
                    or ""
                )
            extraction_language = extraction["extraction_language"]
            text = f"{title}. {description}"
            entities = extract_entities(text, language=extraction_language)

            language_counts[article_language] = language_counts.get(article_language, 0) + 1
            path_counts[path] = path_counts.get(path, 0) + 1
            if model_name:
                model_counts[model_name] = model_counts.get(model_name, 0) + 1
            language_entry = language_paths.setdefault(
                article_language,
                {"articles": 0, "path_counts": {}, "model_counts": {}, "text_sources": {}},
            )
            language_entry["articles"] += 1
            language_entry["path_counts"][path] = language_entry["path_counts"].get(path, 0) + 1
            if model_name:
                language_entry["model_counts"][model_name] = (
                    language_entry["model_counts"].get(model_name, 0) + 1
                )
            text_source = extraction["text_source"]
            language_entry["text_sources"][text_source] = (
                language_entry["text_sources"].get(text_source, 0) + 1
            )

            for entity in entities:
                result = conn.execute(
                    """
                    INSERT INTO entity_mentions (entity, entity_type, topic, article_url, mentioned_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (topic, article_url, entity) DO NOTHING
                    """,
                    (entity["entity"], entity["type"], topic, article["url"], now),
                )
                total_mentions += result.rowcount if result.rowcount > 0 else 0

            for i in range(len(entities)):
                for j in range(i + 1, len(entities)):
                    a = entities[i]["entity"]
                    b = entities[j]["entity"]
                    if a > b:
                        a, b = b, a
                    result = conn.execute(
                        """
                        INSERT INTO entity_cooccurrences
                            (entity_a, entity_b, topic, article_url, mentioned_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (topic, article_url, entity_a, entity_b) DO NOTHING
                        """,
                        (a, b, topic, article["url"], now),
                    )
                    total_cooc += result.rowcount if result.rowcount > 0 else 0

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
```

Also add `from datetime import timezone` to the existing datetime import line if not present.

---

### Step 1E — Update all read functions in `entities.py`

For every function that calls `_connect()` and uses SQLite cursor-style access
(`conn.cursor()`, `c.execute()`, `c.fetchall()`, `conn.close()`), convert to the
PostgreSQL `with _connect() as conn:` pattern and replace `?` with `%s`.

The functions to update are:
- `get_entity_frequencies()` (line ~728)
- `get_top_entities()` (line ~817)
- `get_entity_relationships()` (line ~847)
- `get_relationship_graph()` (line ~874)
- `lookup_entity_links()` (line ~949)
- `batch_lookup_entity_links()` (line ~1053)
- `get_best_entity_link()` (line ~1076)

For each: replace `conn = _connect(); c = conn.cursor()` with `with _connect() as conn:`,
replace `c.execute(...)` with `conn.execute(...)`, replace `c.fetchall()` with
`conn.execute(...).fetchall()`, remove `conn.close()`, replace `?` with `%s`.

Row access changes: SQLite rows are accessed by index (`row[0]`). PostgreSQL rows from
`_connect()` are dict-like — access by column name (`row["entity"]`). Update all row
access in these functions to use column name keys matching the SELECT column aliases.

---

### Step 1F — Remove the SQLite retry wrapper

In `backend/services/ingest_service.py`, the function
`_store_entity_mentions_with_translation()` (~line 101) has retry logic that exists
solely because of SQLite lock contention:

```python
def _store_entity_mentions_with_translation(articles: list[dict], topic: str) -> dict:
    for attempt in range(3):
        try:
            return store_entity_mentions(articles, topic)
        except ...:   # sqlite OperationalError retry
            ...
```

Remove the retry loop. Replace the entire function body with a direct call:

```python
def _store_entity_mentions_with_translation(articles: list[dict], topic: str) -> dict:
    return store_entity_mentions(articles, topic)
```

The retry was protecting against SQLite locking. PostgreSQL handles concurrency
natively — no retry needed.

---

## Fix 2 — Enable Chroma by default and fix O(n²) embedding in clustering

**Files changed:**
- `backend/core/config.py`
- `backend/clustering.py`

**Problem A:** Chroma is disabled by default (`REQUEST_ENABLE_CHROMA_INGEST = false`,
`REQUEST_ENABLE_VECTOR_SEARCH = false`). The vector database is built and integrated but
never used.

**Problem B:** `relatedness_score()` in `clustering.py` calls
`model.encode([left_text, right_text])` for every article pair. When clustering 500
articles, this results in up to 124,750 separate encode calls, each encoding 2 texts.
The sentence transformer can encode a full batch in one call.

---

### Step 2A — Enable Chroma by default

In `backend/core/config.py`, change the defaults for both Chroma flags:

```python
# Before:
REQUEST_ENABLE_CHROMA_INGEST = (
    os.getenv("OTHELLO_ENABLE_CHROMA_INGEST", "false").lower() == "true"
)
REQUEST_ENABLE_VECTOR_SEARCH = (
    os.getenv("OTHELLO_ENABLE_VECTOR_SEARCH", "false").lower() == "true"
)

# After:
REQUEST_ENABLE_CHROMA_INGEST = (
    os.getenv("OTHELLO_ENABLE_CHROMA_INGEST", "true").lower() == "true"
)
REQUEST_ENABLE_VECTOR_SEARCH = (
    os.getenv("OTHELLO_ENABLE_VECTOR_SEARCH", "true").lower() == "true"
)
```

---

### Step 2B — Pre-compute all embeddings in a single batch call

In `backend/clustering.py`, find the function `build_article_signatures()`. This
function builds a list of signature dicts for each article. After it builds the list,
add a single batch embedding step that pre-computes and attaches embeddings to each
signature — so `relatedness_score()` can use cached embeddings instead of re-encoding.

Add this block at the end of `build_article_signatures()`, before the return statement:

```python
# Batch-encode all article texts in one model call to avoid O(n²) encoding.
model = get_semantic_model()
texts = [sig.get("text") or "" for sig in signatures]
if texts:
    all_embeddings = model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
    for sig, embedding in zip(signatures, all_embeddings):
        sig["_embedding"] = embedding
```

Then in `relatedness_score()`, check for cached embeddings before encoding:

```python
def relatedness_score(left: dict, right: dict) -> float:
    left_embedding = left.get("_embedding")
    right_embedding = right.get("_embedding")

    if left_embedding is None or right_embedding is None:
        # Fallback: encode on demand if pre-computation was skipped
        model = get_semantic_model()
        left_text = left.get("text", "")
        right_text = right.get("text", "")
        if not left_text or not right_text:
            return 0.0
        embeddings = model.encode([left_text, right_text], convert_to_numpy=True)
        left_embedding = embeddings[0]
        right_embedding = embeddings[1]

    semantic_score = float(cosine_similarity(
        left_embedding.reshape(1, -1),
        right_embedding.reshape(1, -1)
    )[0][0])

    # Keep the rest of the existing function body unchanged from here:
    # temporal weight, geo penalty, entity overlap, etc.
```

Do not change any other logic in `relatedness_score()`.

---

## Fix 3 — Topic classification via embedding centroids as fallback

**File:** `backend/news.py`

**Problem:** `infer_article_topics()` returns empty when no keyword matches. With the
expanded keyword set this is less common, but still fails for paraphrase-heavy headlines
and out-of-domain conflicts. The sentence transformer model already loaded in
`clustering.py` can classify topics semantically — articles returning no keyword match
should get a second chance through embedding similarity.

---

### Step 3A — Define topic centroids

Add the following constant near the top of `backend/news.py`, after `TOPIC_KEYWORDS`:

```python
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
```

---

### Step 3B — Add the embedding classifier function

Add the following function immediately before `infer_article_topics()` in `backend/news.py`:

```python
_topic_centroid_embeddings: dict[str, object] | None = None


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

    text = " ".join(filter(None, [
        article.get("title", ""),
        article.get("description", ""),
    ])).strip()
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
            score = float(_cosine_similarity(
                article_embedding.reshape(1, -1),
                centroid_embedding.reshape(1, -1)
            )[0][0])
            if score >= 0.30:
                results.append((topic, score))

        results.sort(key=lambda x: x[1], reverse=True)
        return [topic for topic, _ in results]

    except Exception:
        return []
```

---

### Step 3C — Call the embedding classifier as fallback in `infer_article_topics()`

At the end of `infer_article_topics()`, before the final `return []`, insert the
embedding fallback:

```python
# Before (current end of function):
    if not scored:
        return []
    ...
    return matches

# After:
    if not scored:
        # No keyword match — try embedding-based classification as fallback.
        return _classify_topic_by_embedding(article)
    ...
    return matches
```

The embedding fallback is only called when keyword matching finds nothing, so it
does not affect performance for articles that already classify correctly.

---

## Fix 4 — Separate framing divergence from factual contradiction

**Files changed:**
- `backend/contradictions.py`
- `backend/api/routes/` (whichever route serves contradictions to the frontend)
- `frontend/src/components/` (wherever "Contradictions" is labelled in the UI)

**Problem:** `detect_contradictions()` conflates two different things:
1. **Framing divergence** — different sources use different political/moral language for
   the same event (e.g., "militant" vs "terrorist"). This is reliably detectable with
   the existing lexicon approach and is genuinely useful.
2. **Factual contradiction** — sources report conflicting facts (e.g., different death
   tolls). This is not reliably detectable through heuristics and produces false positives
   that damage user trust.

The system already has a separate `detect_narrative_fractures()` function. The problem
is that `detect_contradictions()` combines both and presents them under the same label.

---

### Step 4A — Rename the output type in `detect_contradictions()`

In `backend/contradictions.py`, find `detect_contradictions()` (~line 1253). This
function currently returns a list of items mixing factual and framing contradictions.

Add a `"contradiction_class"` field to every item it returns:
- Items that came from `detect_narrative_fractures()` / framing label divergence:
  set `"contradiction_class": "framing_divergence"`
- Items that came from numeric/status discrepancy detection:
  set `"contradiction_class": "factual_claim"`

Do this by tagging the items before they are appended to the result list. Find each
`results.append({...})` call inside `detect_contradictions()` and add the appropriate
`"contradiction_class"` key to the dict being appended.

---

### Step 4B — Filter factual claims from the default API response

In the API route that returns contradictions (find it with:
`grep -rn "detect_contradictions\|contradictions" backend/api/routes/`),
change the default response to only include `framing_divergence` items:

```python
all_contradictions = detect_contradictions(event)
# Only surface framing divergences by default — factual claims require LLM validation
contradictions = [
    c for c in all_contradictions
    if c.get("contradiction_class") != "factual_claim"
]
```

If the API route accepts a query param (e.g., `?include_factual=true`), allow factual
claims to be included when explicitly requested. Otherwise exclude them.

---

### Step 4C — Rename in the frontend

In the frontend, find all UI text that says "Contradictions" in the context of this
feature (check `frontend/src/components/` for the relevant component). Change the
display label from `"Contradictions"` to `"Framing Divergences"`. Change any
description text from "conflicting reports" to "sources using different framing for
the same event." Do not change variable names or prop names — only the visible UI
strings.

---

## Fix 5 — Cross-dataset deduplication of ACLED and GDELT events

**Files changed:**
- `backend/db/events_repo.py`
- `backend/ingestion/acled_ingestion.py`
- `backend/ingestion/gdelt_gkg_ingestion.py`
- `backend/db/schema.py`

**Problem:** ACLED and GDELT both ingest into `structured_events` with `dataset = "acled"`
or `dataset = "gdelt_gkg"`. The same physical conflict event (e.g., an airstrike in
Beirut on a specific date) appears as separate rows from both datasets. The map hotspot
system double-counts these as distinct events, inflating incident counts and weights.

---

### Step 5A — Add a `superseded_by` column to `structured_events`

In `backend/db/schema.py`, add the following ALTER TABLE statement inside
`initialize_schema()`, alongside the other ALTER TABLE statements:

```python
conn.execute(
    "ALTER TABLE structured_events "
    "ADD COLUMN IF NOT EXISTS superseded_by TEXT REFERENCES structured_events(event_id)"
)
conn.execute(
    "CREATE INDEX IF NOT EXISTS idx_structured_events_superseded "
    "ON structured_events (superseded_by) WHERE superseded_by IS NOT NULL"
)
```

`superseded_by` will hold the `event_id` of the preferred canonical record when a
duplicate is found. ACLED is preferred over GDELT when both describe the same event
(ACLED is manually curated).

---

### Step 5B — Add `deduplicate_cross_dataset_events()` to `events_repo.py`

Add the following function to `backend/db/events_repo.py`. It uses the same
`_haversine_km` logic as `map_service.py` — reimplement it locally rather than
importing from map_service to avoid a circular dependency:

```python
import math


def _haversine_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    lat1, lon1 = math.radians(lat_a), math.radians(lon_a)
    lat2, lon2 = math.radians(lat_b), math.radians(lon_b)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    arc = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * radius_km * math.asin(min(1.0, math.sqrt(max(0.0, arc))))


def deduplicate_cross_dataset_events(
    days: int = 3,
    radius_km: float = 50.0,
    max_hours_apart: float = 24.0,
) -> dict:
    """Mark GDELT events as superseded when a matching ACLED event exists nearby.

    Matching criteria:
    - Both events within radius_km of each other (default 50 km)
    - Event dates within max_hours_apart (default 24 hours)
    - Same broad event category (Battles/Violence maps to ACLED equivalents)

    ACLED is preferred as the canonical source. When a match is found,
    the GDELT row's superseded_by is set to the ACLED event_id.
    Only processes events from the last `days` days.

    Returns a summary dict with counts of matches found and marked.
    """
    from datetime import datetime, timezone, timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    with _connect() as conn:
        acled_rows = conn.execute(
            """
            SELECT event_id, latitude, longitude, event_date, event_type, country
            FROM structured_events
            WHERE dataset = 'acled'
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND last_ingested_at >= %s
            """,
            (cutoff,),
        ).fetchall()

        gdelt_rows = conn.execute(
            """
            SELECT event_id, latitude, longitude, event_date, event_type, country
            FROM structured_events
            WHERE dataset = 'gdelt_gkg'
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND superseded_by IS NULL
              AND last_ingested_at >= %s
            """,
            (cutoff,),
        ).fetchall()

    matched = 0
    for gdelt in gdelt_rows:
        g_lat = float(gdelt["latitude"] or 0)
        g_lon = float(gdelt["longitude"] or 0)
        g_date = str(gdelt["event_date"] or "")
        g_country = (gdelt["country"] or "").strip().lower()

        best_acled_id = None
        best_distance = float("inf")

        for acled in acled_rows:
            a_lat = float(acled["latitude"] or 0)
            a_lon = float(acled["longitude"] or 0)
            a_date = str(acled["event_date"] or "")
            a_country = (acled["country"] or "").strip().lower()

            # Must be in the same country to be considered a match.
            if g_country and a_country and g_country != a_country:
                continue

            # Date proximity check (treat dates as strings yyyy-mm-dd).
            try:
                from datetime import date
                gd = date.fromisoformat(g_date[:10]) if g_date else None
                ad = date.fromisoformat(a_date[:10]) if a_date else None
                if gd and ad:
                    hours_apart = abs((gd - ad).total_seconds()) / 3600
                    if hours_apart > max_hours_apart:
                        continue
            except ValueError:
                continue

            dist = _haversine_km(g_lat, g_lon, a_lat, a_lon)
            if dist <= radius_km and dist < best_distance:
                best_distance = dist
                best_acled_id = acled["event_id"]

        if best_acled_id:
            with _connect() as conn:
                conn.execute(
                    """
                    UPDATE structured_events
                    SET superseded_by = %s
                    WHERE event_id = %s AND superseded_by IS NULL
                    """,
                    (best_acled_id, gdelt["event_id"]),
                )
            matched += 1

    return {
        "acled_events_checked": len(acled_rows),
        "gdelt_events_checked": len(gdelt_rows),
        "duplicates_marked": matched,
        "radius_km": radius_km,
        "days": days,
    }
```

---

### Step 5C — Call deduplication after each ingestion run

In `backend/ingestion/acled_ingestion.py`, at the end of `ingest_acled_recent()`,
after `inserted = upsert_structured_events(events)`, add:

```python
from db.events_repo import deduplicate_cross_dataset_events
dedup_result = deduplicate_cross_dataset_events(days=3)
print(f"[acled] Cross-dataset dedup: {dedup_result}")
```

In `backend/ingestion/gdelt_gkg_ingestion.py`, at the end of the function that calls
`upsert_structured_events(events)`, add the same two lines.

---

### Step 5D — Filter superseded events from map queries

In `backend/db/events_repo.py`, find `get_recent_structured_events()` (~line 87).
Add a `WHERE superseded_by IS NULL` filter to its query so superseded GDELT rows are
excluded from all downstream consumers (map hotspots, story rollups, etc.):

```python
# In the WHERE clause of get_recent_structured_events(), add:
AND superseded_by IS NULL
```

---

## Fix 6 — Briefing architecture: deterministic scaffold, LLM enriches specific fields

**File:** `backend/analyst.py`

**Problem:** `generate_briefing()` sends everything to Groq and returns a fully
LLM-authored briefing. When Groq fails or is unavailable, a degraded deterministic
fallback runs. For an intelligence product this is backwards: structured facts
(event counts, actor lists, source attributions) should always be present, and LLM
should enrich selected prose fields only — making the output better, not gating
it.

---

### Step 6A — Add `build_deterministic_briefing()` function

Add the following function to `backend/analyst.py` before `generate_briefing()`.
It builds a fully structured briefing from its inputs without any LLM call:

```python
def build_deterministic_briefing(
    articles: list[dict],
    topic: str | None = None,
    events: list[dict] | None = None,
) -> dict:
    """Build a structured briefing from raw data without any LLM dependency.

    Returns a dict with fixed keys that the frontend can render directly.
    Each field is populated from deterministic logic — sorted lists, counts,
    entity extraction — so this always returns useful output.
    """
    from news import article_quality_score, infer_article_topics, diversify_articles

    topic_articles = [
        a for a in (articles or [])
        if not topic or topic in (infer_article_topics(a) or [])
    ]
    top_articles = sorted(
        topic_articles,
        key=lambda a: -article_quality_score(a, [topic] if topic else None),
    )[:12]

    # Key developments: top 5 article titles with source attribution
    key_developments = [
        {
            "headline": a.get("title", ""),
            "source": a.get("source") or a.get("source_domain") or "Unknown",
            "url": a.get("url", ""),
            "published_at": a.get("published_at", ""),
        }
        for a in top_articles[:5]
    ]

    # Entity signals: most frequent named entities across top articles
    entity_counts: dict[str, int] = {}
    for a in top_articles:
        for entity in (a.get("entities") or []):
            name = entity if isinstance(entity, str) else entity.get("entity", "")
            if name:
                entity_counts[name] = entity_counts.get(name, 0) + 1
    critical_actors = sorted(entity_counts.items(), key=lambda x: -x[1])[:6]

    # Source diversity
    sources = sorted(
        {a.get("source") or a.get("source_domain") or "Unknown" for a in top_articles}
    )

    # Event summary if structured events are provided
    event_summary = []
    for event in (events or [])[:5]:
        event_summary.append({
            "location": event.get("location") or event.get("country") or "Unknown",
            "event_type": event.get("event_type", ""),
            "fatalities": event.get("fatalities") or 0,
            "date": str(event.get("event_date") or ""),
        })

    return {
        "topic": topic or "general",
        "article_count": len(topic_articles),
        "key_developments": key_developments,
        "critical_actors": [{"entity": name, "mentions": count} for name, count in critical_actors],
        "sources": sources,
        "event_summary": event_summary,
        # These fields are empty in the deterministic build;
        # LLM enrichment populates them if available.
        "situation_summary": "",
        "signal_vs_noise": "",
        "llm_enriched": False,
    }
```

---

### Step 6B — Refactor `generate_briefing()` to use scaffold + enrichment

Find `generate_briefing()` in `backend/analyst.py`. Restructure it so it:
1. Always builds the deterministic scaffold first.
2. Attempts LLM enrichment of only two fields: `situation_summary` and `signal_vs_noise`.
3. Returns the scaffold with or without enrichment — never returns an empty briefing.

Replace the current body of `generate_briefing()` with:

```python
def generate_briefing(
    articles: list[dict],
    topic: str | None = None,
    events: list[dict] | None = None,
    use_llm: bool = True,
) -> dict:
    # Step 1: Always build the deterministic scaffold.
    briefing = build_deterministic_briefing(articles, topic=topic, events=events)

    # Step 2: Attempt LLM enrichment of prose fields only.
    if not use_llm or not REQUEST_ENABLE_LLM_RESPONSES:
        return briefing

    try:
        top_headlines = "\n".join(
            f"- {d['headline']} ({d['source']})"
            for d in briefing["key_developments"]
        )
        actors = ", ".join(d["entity"] for d in briefing["critical_actors"])
        enrichment_prompt = (
            f"Topic: {topic or 'general intelligence'}\n\n"
            f"Top developments:\n{top_headlines}\n\n"
            f"Key actors: {actors}\n\n"
            "Write two short paragraphs:\n"
            "1. SITUATION SUMMARY (3-4 sentences): What is the core situation?\n"
            "2. SIGNAL VS NOISE (2-3 sentences): What is genuinely significant "
            "versus routine reporting?\n\n"
            "Be specific and factual. Do not introduce actors or events not listed above."
        )
        llm_response = _call_groq(enrichment_prompt)
        if llm_response:
            # Parse the two sections from the LLM response.
            lines = llm_response.strip().split("\n")
            situation_lines = []
            noise_lines = []
            current = None
            for line in lines:
                low = line.lower()
                if "situation summary" in low:
                    current = "situation"
                elif "signal vs noise" in low or "signal versus noise" in low:
                    current = "noise"
                elif current == "situation" and line.strip():
                    situation_lines.append(line.strip())
                elif current == "noise" and line.strip():
                    noise_lines.append(line.strip())
            if situation_lines:
                briefing["situation_summary"] = " ".join(situation_lines)
            if noise_lines:
                briefing["signal_vs_noise"] = " ".join(noise_lines)
            briefing["llm_enriched"] = True
    except Exception as exc:
        print(f"[briefing] LLM enrichment failed, returning scaffold: {exc}")

    return briefing
```

In this refactored version, `_call_groq` refers to the existing internal Groq wrapper
function in `analyst.py` (the function that handles retries and rate limits —
currently named around line 304). Use whatever that function is actually called.
Do not rename it.

---

## Implementation order

Apply fixes in this sequence. Each is independent enough to be reviewed before the next:

1. **Fix 5A+5B** (schema column + dedup function) — pure additions, no existing code changes
2. **Fix 5C+5D** (wire dedup into ingestion + filter query) — uses Fix 5A/5B
3. **Fix 4A+4B+4C** (framing/contradiction split) — isolated to contradictions.py and frontend label
4. **Fix 2A** (enable Chroma default) — one-line config change, verify no startup errors
5. **Fix 2B** (batch embedding) — clustering performance fix, verify cluster output unchanged
6. **Fix 3A+3B+3C** (topic centroid classifier) — uses clustering model, test on articles that previously returned no topic
7. **Fix 6A+6B** (briefing scaffold) — refactor generate_briefing, verify both LLM-on and LLM-off paths return valid output
8. **Fix 1A** (entity schema in PG) — run migration, verify tables created
9. **Fix 1B–1F** (entity.py rewrite) — largest change, do last; verify entity counts match before/after

## Files changed summary

| File | Fixes |
|------|-------|
| `backend/db/schema.py` | Fix 1A (entity tables), Fix 5A (superseded_by column) |
| `backend/entities.py` | Fix 1B–1E (full SQLite→PostgreSQL migration) |
| `backend/services/ingest_service.py` | Fix 1F (remove SQLite retry wrapper) |
| `backend/core/config.py` | Fix 2A (enable Chroma by default) |
| `backend/clustering.py` | Fix 2B (batch embedding pre-computation) |
| `backend/news.py` | Fix 3A–3C (centroid classifier) |
| `backend/contradictions.py` | Fix 4A (tag contradiction_class on all results) |
| `backend/api/routes/` (contradictions route) | Fix 4B (filter factual claims from default response) |
| `frontend/src/components/` (contradictions UI) | Fix 4C (rename label to Framing Divergences) |
| `backend/db/events_repo.py` | Fix 5B (dedup function), Fix 5D (filter superseded) |
| `backend/ingestion/acled_ingestion.py` | Fix 5C (call dedup after ingest) |
| `backend/ingestion/gdelt_gkg_ingestion.py` | Fix 5C (call dedup after ingest) |
| `backend/analyst.py` | Fix 6A–6B (scaffold + LLM enrichment refactor) |
