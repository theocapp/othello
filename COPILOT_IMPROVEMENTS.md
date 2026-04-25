# Signal Intelligence — Copilot Implementation Prompt

You are working on a geopolitical intelligence platform called **Signal Intelligence**. The stack is:
- **Backend**: Python / FastAPI (`backend/`)
- **Frontend**: React / D3 (`frontend/src/`)
- **Database**: PostgreSQL (accessed via `backend/db/`)

This prompt describes six concrete improvements to implement. Read each section fully before writing any code — later sections depend on context from earlier ones.

---

## Codebase orientation

### How the map works (end-to-end)

The main feature is a world map (`frontend/src/components/WorldHotspotMap.jsx`) showing geolocated "hotspots". Each hotspot is a dot with a label, description, and metadata.

The map data comes from `backend/services/map_service.py → _build_hotspot_attention_map()`. This function produces two layers:
1. **`source_kind: "structured"`** — events from ACLED (conflict database) and GDELT (news event signals), via `_incident_hotspots_from_semantic_clusters()`
2. **`source_kind: "story"`** — materialized news article clusters, via `_build_story_hotspots()`

Both layers are combined and returned as a `hotspots` array to the frontend.

### How hotspot labels are built

For structured events, `_acled_hotspot_event_copy(event)` in `map_service.py` builds the `title` and `summary` for each dot. It:
- Uses the ACLED `notes` field as the summary if populated
- Synthesizes a description from metadata (actors, location, event_type) if notes is empty
- Uses `_incident_action_label()` to turn raw CAMEO/ACLED event types into human-readable labels

### Key data sources

**ACLED** (`backend/ingestion/acled_ingestion.py`): Conflict event database. Has `actor_primary`, `actor_secondary`, `fatalities`, precise `latitude`/`longitude`, and optionally a `notes` field with narrative text. High quality when notes are populated.

**GDELT** (`backend/ingestion/gdelt_gkg_ingestion.py`): 15-minute global event signals from news media. Has precise coordinates, CAMEO event codes, source URLs, article mention counts. Has **no actor names**. Stores a `sub_event_type` mapped from CAMEO codes (e.g. "Employ aerial weapons", "Conduct armed attack", "Fight with artillery and tanks").

**Articles**: RSS/API articles stored in the `articles` table. Ingested via `backend/providers/` and `backend/sources/source_catalog.py`.

**Canonical events**: `backend/services/article_event_pipeline.py` clusters articles into events and writes them to a `canonical_events` table. These have real article headlines as `resolved_title` and article descriptions as `resolved_summary`. They are **not currently shown on the map**.

---

## Improvement 1: Expand the article source catalog

**File:** `backend/sources/source_catalog.py`

**Problem:** Only two active article sources exist (BBC News RSS, FT via GDELT). Reuters and Politico RSS feeds are quarantined. This starves the story hotspot layer with insufficient coverage.

**What to do:** Add the following sources to `SOURCE_SEEDS` using the existing `_seed()` helper. Follow the exact same pattern as the BBC News entry (which uses `"adapter": "rss"` and a `"feeds"` list with `url` and `topic_hints`).

Add these sources:

```
AP News
  domain: apnews.com
  trust_tier: tier_1
  region: global
  pack: global_wires
  feeds:
    - url: https://rsshub.app/apnews/topics/ap-top-news   topic_hints: [geopolitics, economics]
    - url: https://rsshub.app/apnews/topics/world-news    topic_hints: [geopolitics]

The Guardian (World)
  domain: theguardian.com
  trust_tier: tier_1
  region: global
  pack: global_wires
  feeds:
    - url: https://www.theguardian.com/world/rss          topic_hints: [geopolitics]
    - url: https://www.theguardian.com/business/rss       topic_hints: [economics]

Al Jazeera English
  domain: aljazeera.com
  trust_tier: tier_1
  region: middle-east
  pack: conflict_region_outlets
  feeds:
    - url: https://www.aljazeera.com/xml/rss/all.xml      topic_hints: [geopolitics]

Foreign Policy
  domain: foreignpolicy.com
  trust_tier: tier_2
  region: global
  pack: regional_flagships
  feeds:
    - url: https://foreignpolicy.com/feed/               topic_hints: [geopolitics]

Defense One
  domain: defenseone.com
  trust_tier: tier_2
  region: global
  pack: conflict_region_outlets
  feeds:
    - url: https://www.defenseone.com/rss/all/            topic_hints: [geopolitics]

Radio Free Europe / Radio Liberty
  domain: rferl.org
  trust_tier: tier_2
  region: eurasia
  pack: conflict_region_outlets
  feeds:
    - url: https://www.rferl.org/api/epiqq                topic_hints: [geopolitics]

Middle East Eye
  domain: middleeasteye.net
  trust_tier: tier_2
  region: middle-east
  pack: conflict_region_outlets
  feeds:
    - url: https://www.middleeasteye.net/rss               topic_hints: [geopolitics]

Dawn (Pakistan)
  domain: dawn.com
  trust_tier: tier_2
  region: south-asia
  pack: conflict_region_outlets
  feeds:
    - url: https://www.dawn.com/feed                      topic_hints: [geopolitics]
```

Each entry should follow this pattern (copy from the BBC entry):
```python
_seed(
    "Source Name",
    "domain.com",
    "article",
    "tier_1",          # or tier_2
    "region-slug",
    "en",
    {
        "adapter": "rss",
        "pack": "pack_name",
        "feeds": [
            {"url": "https://...", "topic_hints": ["geopolitics"]},
        ],
    },
),
```

---

## Improvement 2: Fix GDELT summaries to use specific sub_event_type

**File:** `backend/ingestion/gdelt_gkg_ingestion.py`

**Problem:** Line 607 builds the GDELT event summary using the broad `event_type` variable ("Battles", "Protests", etc.) rather than the more specific `sub_event_type` which contains human-readable CAMEO labels like "Employ aerial weapons", "Conduct armed attack", "Fight with artillery and tanks".

**Current code (line ~607):**
```python
summary_line = f"{event_type} reported in {place_detail} on {event_date}.{goldstein_desc}{mention_desc}"
```

**Change to:**
```python
display_action = sub_event_type if sub_event_type and sub_event_type != event_type else event_type
summary_line = f"{display_action} reported in {place_detail} on {event_date}.{goldstein_desc}{mention_desc}"
```

This makes GDELT summaries read "Employ aerial weapons reported in Kharkiv, Ukraine on 2025-04-20. (high-intensity conflict event) Reported by 8 sources." instead of "Battles reported in Kharkiv, Ukraine on 2025-04-20."

No other changes needed in this file.

---

## Improvement 3: Relax the GDELT no-article map filter for specific events

**File:** `backend/services/map_service.py`

**Problem:** Lines 981–985 drop ALL GDELT events that don't have a linked article (`resolved_title` empty). This means any GDELT event whose source URL hasn't been fetched yet is silently discarded, even when the event has a specific and informative sub_event_type.

**Current code (around line 980–985):**
```python
_ev_dataset = (ev.get("dataset") or "").strip().lower()
if _ev_dataset == "gdelt_gkg" and not resolved_title:
    # GDELT events with no linked article contain only a CAMEO code
    # and a location — there is no specific event to describe.
    # Without an article title there is nothing factual to show the user.
    continue
```

**Change to:**
```python
_ev_dataset = (ev.get("dataset") or "").strip().lower()
if _ev_dataset == "gdelt_gkg" and not resolved_title:
    # Allow through only if the sub_event_type is specific enough to be meaningful.
    # Generic CAMEO roots (Battles, Fight, Protests) have no value without an article.
    _gdelt_generic = {
        "battles", "fight", "fights", "protests", "explosions/remote violence",
        "violence against civilians", "strategic developments", "coerce",
        "assault", "reported development",
    }
    _sub_lower = (ev.get("sub_event_type") or "").strip().lower()
    if not _sub_lower or _sub_lower in _gdelt_generic or _sub_lower == (ev.get("event_type") or "").strip().lower():
        continue
    # Specific sub_event_type (e.g. "Employ aerial weapons", "Conduct armed attack") —
    # informative enough to show without a linked article.
```

This allows events like "Employ aerial weapons in Kyiv, Ukraine" to appear on the map even before the source article is fetched.

---

## Improvement 4: Wire canonical_events into the map as a third hotspot layer

**File:** `backend/services/map_service.py`

**Context:** `backend/services/article_event_pipeline.py` writes article-derived events to a `canonical_events` table. Each row has `resolved_title`, `resolved_summary`, `latitude`, `longitude`, `geo_country`, `geo_admin1`, `geo_location`, `event_date_best`, `source_count`, `article_count`, `importance_score`, `article_urls`, and `event_type` ("Conflict" or "Political"). These are never shown on the map.

**What to add:**

### Step A — Add a DB query function

In `backend/corpus.py` (or whichever module contains `get_recent_structured_events`), add:

```python
def get_recent_canonical_events(days: int = 7, limit: int = 300) -> list[dict]:
    """Fetch recent canonical events for map display."""
    from db.common import _connect
    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT event_id, label, resolved_title, resolved_summary,
                   event_type, geo_country, geo_admin1, geo_location,
                   latitude, longitude, event_date_best,
                   source_count, article_count, importance_score,
                   article_urls, fatality_total
            FROM canonical_events
            WHERE event_date_best >= %s
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND resolved_title IS NOT NULL
              AND resolved_title != ''
            ORDER BY importance_score DESC, event_date_best DESC
            LIMIT %s
            """,
            (cutoff, limit),
        ).fetchall()
    return [dict(row) for row in rows]
```

### Step B — Add `_build_canonical_event_hotspots()` to map_service.py

Add this function to `backend/services/map_service.py` after `_build_story_hotspots`:

```python
def _build_canonical_event_hotspots(window: str, now: datetime) -> list[dict]:
    """Article-derived canonical events as map hotspots (source_kind='article')."""
    from corpus import get_recent_canonical_events

    hours = _attention_window_hours((window or "24h").strip().lower())
    days = _window_days((window or "24h").strip().lower())
    cutoff = now - timedelta(hours=hours)

    rows = get_recent_canonical_events(days=min(days, 30), limit=200)
    hotspots: list[dict] = []

    for row in rows:
        lat_v, lon_v = row.get("latitude"), row.get("longitude")
        if lat_v is None or lon_v is None:
            continue
        try:
            lat_f = float(lat_v)
            lon_f = float(lon_v)
        except (TypeError, ValueError):
            continue

        event_dt = _event_datetime_for_hotspot(
            str(row.get("event_date_best") or "")
        )
        if event_dt is None or event_dt < cutoff:
            continue

        country = " ".join(str(row.get("geo_country") or "").split()).strip() or "Unknown country"
        admin1 = " ".join(str(row.get("geo_admin1") or "").split()).strip() or None
        location = " ".join(str(row.get("geo_location") or "").split()).strip() or country
        title = " ".join(str(row.get("resolved_title") or "").split()).strip()
        summary = " ".join(str(row.get("resolved_summary") or "").split()).strip()
        event_type = (row.get("event_type") or "Political").strip()
        source_count = int(row.get("source_count") or 1)
        article_count = int(row.get("article_count") or 1)
        fatalities = int(row.get("fatality_total") or 0)
        importance = float(row.get("importance_score") or 1.0)
        article_urls = row.get("article_urls") or []

        recency = _hotspot_recency_factor(event_dt, now, hours)
        attention_score = round(
            (importance * 0.4) + (source_count * 0.8) + (article_count * 0.3) + (recency * 4.0),
            2,
        )

        aspect = "conflict" if event_type.lower() == "conflict" else "political"

        import hashlib as _hashlib
        material = f"ce|{row.get('event_id')}|{lat_f:.4f}|{lon_f:.4f}"
        hotspot_id = _hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

        sample_event = {
            "event_id": row.get("event_id"),
            "event_date": str(row.get("event_date_best") or ""),
            "country": country,
            "admin1": admin1,
            "location": location,
            "event_type": event_type,
            "fatalities": fatalities,
            "source_count": source_count,
            "source_urls": article_urls[:4] if isinstance(article_urls, list) else [],
            "title": title,
            "summary": summary or title,
        }

        hotspots.append({
            "hotspot_id": hotspot_id,
            "label": title[:220],
            "headline": title[:220],
            "country": country,
            "admin1": admin1,
            "location": location,
            "latitude": round(lat_f, 4),
            "longitude": round(lon_f, 4),
            "event_count": article_count,
            "fatality_total": fatalities,
            "source_count": source_count,
            "attention_score": attention_score,
            "attention_share": 0.0,
            "intensity": 0.0,
            "event_density": 0.0,
            "fatality_density": 0.0,
            "cloud_radius": 0.0,
            "cloud_density": 0.0,
            "latest_event_date": event_dt.isoformat().replace("+00:00", "Z") if event_dt else None,
            "event_types": [event_type],
            "aspect": aspect,
            "sample_locations": [location],
            "story_region": _world_region_for_coordinates(lat_f, lon_f),
            "sample_events": [sample_event],
            "source_kind": "article",
            "topic": "economics" if aspect == "economic" else "geopolitics",
        })

    return hotspots
```

### Step C — Call it from `_build_hotspot_attention_map`

In `_build_hotspot_attention_map()`, after the existing `story_hotspots, total_story_candidates = _build_story_hotspots(...)` call, add:

```python
canonical_hotspots = _build_canonical_event_hotspots(normalized_window, now)
combined = hotspots + story_hotspots + canonical_hotspots
```

Replace the existing `combined = hotspots + story_hotspots` line.

---

## Improvement 5: Surface sub_event_type in the map tooltip

**File:** `frontend/src/components/WorldHotspotMap.jsx`

**Context:** When a user hovers over or clicks a map dot, a tooltip/panel shows the event details. Currently the tooltip shows the broad `event_type` (e.g. "Battles") but not the specific `sub_event_type` (e.g. "Armed clash").

Find the tooltip rendering section in `WorldHotspotMap.jsx` — it reads from `hotspot.sample_events[0]` and displays event metadata.

**Change:** Wherever `event_type` is displayed in the tooltip or event panel, also show `sub_event_type` when it is present and different from `event_type`. The pattern should be: if `sub_event_type` exists and is not the same string as `event_type`, show `sub_event_type` as the primary label and `event_type` as a secondary/smaller label. If only `event_type` exists, show it alone.

Also update `frontend/src/lib/hotspots.js` — in `hotspotEventTitle(ev)` and `hotspotEventDescription(ev)`:
- Both functions already prefer `sub_event_type` when it differs from `event_type` via the `action` variable — this is correct, no change needed there.
- In `hotspotEventDescription`, ensure the fallback description uses `sub_event_type` preferentially when available.

---

## Improvement 6: Add a data freshness indicator to the map

**Files:** `frontend/src/components/WorldHotspotMap.jsx`, `backend/api/routes/analytics.py`

**Problem:** Users can't tell when the data was last updated. If ingestion hasn't run, the map looks empty and there's no explanation.

### Backend change

In `backend/services/map_service.py`, the `_build_hotspot_attention_map()` function already returns `generated_at` in its payload. Also add a `data_freshness` object:

```python
# After building combined hotspots, before returning payload:
latest_structured = max(
    (h.get("latest_event_date") or "" for h in hotspots),
    default=None
)
latest_story = max(
    (h.get("latest_event_date") or "" for h in story_hotspots),
    default=None
)

payload = {
    ...existing fields...,
    "data_freshness": {
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "latest_structured_event": latest_structured,
        "latest_story_event": latest_story,
        "structured_count": len(hotspots),
        "story_count": len(story_hotspots),
        "article_count": len(canonical_hotspots),  # after improvement 4
    },
}
```

### Frontend change

In `WorldHotspotMap.jsx`, read `data?.data_freshness` and render a small status line below the window selector. Show:
- "Updated X minutes ago" (compute from `generated_at` vs current time)
- If `structured_count === 0`: show a subtle warning: "Conflict data pending — run ingestion to refresh"
- Use a muted text style, nothing alarming

---

## Implementation notes

- Do **not** remove ACLED or GDELT ingestion — they remain the primary structured data sources
- Do **not** change the existing `source_kind: "structured"` or `source_kind: "story"` logic — only add the new `"article"` layer alongside them
- The `canonical_events` table may not exist in all environments — wrap the `get_recent_canonical_events` DB call in a try/except that returns `[]` on any error (table not found, connection issue, etc.)
- All new RSS feed URLs should be added with `"blocked": False` (no blocked key needed — absence means active). Do not mark any new source as blocked unless you have confirmed it returns errors.
- For Improvement 3, the filter relaxation only applies to GDELT (`_ev_dataset == "gdelt_gkg"`). The ACLED filter on line ~987 (`if not resolved_title and not generated_has_narrative and fatality_total == 0`) should remain unchanged.
- Test that the map still loads correctly after each improvement — the combined list is sorted by `attention_score` so new hotspots will naturally rank against existing ones.
