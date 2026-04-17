"""Map / hotspot building logic extracted from main.py.

All constants come from core.config; parse_timestamp from core.runtime.
No imports from main.
"""

from __future__ import annotations

import hashlib
import math
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException

from core.config import (
    ATTENTION_WINDOW_HOURS,
    CONFLICT_TEXT_PATTERNS,
    DATELINE_RE,
    ECONOMIC_TEXT_PATTERNS,
    HOTSPOT_EVENT_TYPE_WEIGHTS,
    MAP_CACHE_TTL_SECONDS,
    MAX_MAP_STRUCTURED_DAYS,
    POLITICAL_TEXT_PATTERNS,
)
from core.map_state import MAP_ATTENTION_CACHE, STORY_LOCATION_INDEX_CACHE
from core.runtime import parse_timestamp as _parse_timestamp

from corpus import (
    get_articles_by_urls,
    get_articles_with_regions,
    get_recent_structured_events,
    get_structured_event_coordinates_by_ids,
    load_materialized_story_clusters,
)
from entities import extract_entities
from geo_constants import COUNTRY_CENTROIDS
from news import normalize_article_description, normalize_article_title
from structured_story_rollups import build_map_structured_story_clusters

# ---------------------------------------------------------------------------
# Backwards-compatible aliases for legacy imports and tests
# ---------------------------------------------------------------------------
_MAP_ATTENTION_CACHE = MAP_ATTENTION_CACHE
_STORY_LOCATION_INDEX_CACHE = STORY_LOCATION_INDEX_CACHE
_MAP_HOTSPOT_CACHE_SCHEMA_VERSION = "v2"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _humanize_region(region: str | None) -> str:
    text = (region or "").strip().lower()
    if not text:
        return "Global"
    return " ".join(part.capitalize() for part in text.split("-"))


def _attention_window_hours(window: str) -> int:
    normalized = (window or "24h").strip().lower()
    if normalized not in ATTENTION_WINDOW_HOURS:
        raise HTTPException(
            status_code=400,
            detail=f"window must be one of {sorted(ATTENTION_WINDOW_HOURS)}",
        )
    return ATTENTION_WINDOW_HOURS[normalized]


def _build_region_attention_map(window: str = "24h") -> dict:
    normalized_window = (window or "24h").strip().lower()
    hours = _attention_window_hours(normalized_window)
    now = datetime.now(timezone.utc)
    rows = get_articles_with_regions(hours=hours)

    region_stats: dict[str, dict] = {}
    total_attention = 0.0
    global_article_count = 0
    global_attention_score = 0.0

    for row in rows:
        region = (row.get("region") or "global").strip().lower() or "global"
        published_at = _parse_timestamp(row.get("published_at"))
        source_key = (
            (row.get("source_domain") or row.get("source") or "unknown").strip().lower()
        )
        age_hours = (
            max(0.0, (now - published_at).total_seconds() / 3600)
            if published_at
            else float(hours)
        )
        recency_ratio = max(0.0, 1.0 - min(age_hours / max(hours, 1), 1.0))
        attention_increment = 1.0 + (recency_ratio * 1.75)

        if region == "global":
            global_article_count += 1
            global_attention_score += attention_increment
            continue

        entry = region_stats.setdefault(
            region,
            {
                "region": region,
                "label": _humanize_region(region),
                "article_count": 0,
                "source_keys": set(),
                "latest_published_at": None,
                "attention_score": 0.0,
            },
        )
        entry["article_count"] += 1
        entry["attention_score"] += attention_increment
        if source_key:
            entry["source_keys"].add(source_key)
        latest = entry["latest_published_at"]
        if published_at and (latest is None or published_at > latest):
            entry["latest_published_at"] = published_at
        total_attention += attention_increment

    ranked = sorted(
        region_stats.values(),
        key=lambda item: (
            -(item["attention_score"]),
            -(item["article_count"]),
            item["region"],
        ),
    )
    max_attention = max((item["attention_score"] for item in ranked), default=0.0)

    regions = []
    for item in ranked:
        latest = item["latest_published_at"]
        latest_text = (
            latest.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            if latest
            else None
        )
        source_count = len(item["source_keys"])
        attention_score = round(float(item["attention_score"]), 2)
        attention_share = (
            round((item["attention_score"] / total_attention), 4)
            if total_attention
            else 0.0
        )
        cloud_size = (
            round(0.32 + ((item["attention_score"] / max_attention) * 0.68), 3)
            if max_attention
            else 0.32
        )
        regions.append(
            {
                "region": item["region"],
                "label": item["label"],
                "article_count": int(item["article_count"]),
                "source_count": source_count,
                "latest_published_at": latest_text,
                "attention_score": attention_score,
                "attention_share": attention_share,
                "cloud_size": cloud_size,
            }
        )

    return {
        "window": normalized_window,
        "hours": hours,
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "total_articles": len(rows),
        "global_article_count": global_article_count,
        "global_attention_score": round(global_attention_score, 2),
        "regions": regions,
        "available_windows": list(ATTENTION_WINDOW_HOURS.keys()),
    }


def _window_days(window: str) -> int:
    return max(1, math.ceil(_attention_window_hours(window) / 24))


def _event_datetime_for_hotspot(value: str | None) -> datetime | None:
    parsed = _parse_timestamp(value)
    if parsed is not None:
        return parsed.astimezone(timezone.utc)
    if value:
        try:
            return datetime.fromisoformat(str(value)).replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _haversine_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    lat1 = math.radians(lat_a)
    lon1 = math.radians(lon_a)
    lat2 = math.radians(lat_b)
    lon2 = math.radians(lon_b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    arc = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * radius_km * math.asin(min(1.0, math.sqrt(max(0.0, arc))))


def _world_region_for_coordinates(lat: float, lon: float) -> str:
    if lon < -30:
        if lat >= 50:
            return "united-states"
        return "united-states"
    if -30 <= lon < 20:
        if lat >= 50:
            return "united-kingdom" if lon < -2 and lat >= 50 else "europe"
        return "africa"
    if 20 <= lon < 42 and 12 <= lat < 45:
        return "middle-east"
    if 20 <= lon < 65:
        return "eurasia" if lat >= 45 else "middle-east"
    if 65 <= lon < 95:
        return "south-asia"
    if 95 <= lon < 180:
        return "asia-pacific"
    return "global"


def _hotspot_event_weight(
    event: dict, now: datetime, window_hours: int
) -> tuple[float, datetime | None]:
    event_dt = _event_datetime_for_hotspot(event.get("event_date"))
    age_hours = (
        max(0.0, (now - event_dt).total_seconds() / 3600)
        if event_dt
        else float(window_hours)
    )
    recency = max(0.0, 1.0 - min(age_hours / max(window_hours, 1), 1.0))
    fatalities = int(event.get("fatalities") or 0)
    source_count = int(event.get("source_count") or 0)
    event_type = (event.get("event_type") or "").strip()
    weight = 1.0
    weight += HOTSPOT_EVENT_TYPE_WEIGHTS.get(event_type, 2.1)
    weight += min(fatalities, 40) * 0.18
    weight += min(source_count, 20) * 0.16
    weight += recency * 2.8
    return round(weight, 4), event_dt


def _dedup_location_string(text: str) -> str:
    """Remove redundant segments and resolve raw codes from GDELT-style location strings.
    e.g. 'Tehran, Tehran, Iran' -> 'Tehran, Iran'
         'California, United States, United States' -> 'California, United States'
         'IR, Iran' -> 'Iran'
    """
    from ingestion.gdelt_gkg_ingestion import COUNTRY_CODE_TO_NAME as _CC

    parts = [p.strip() for p in text.split(",") if p.strip()]
    # Resolve 2-letter country codes in individual segments
    resolved = []
    for p in parts:
        if len(p) == 2 and p.isupper() and p in _CC:
            resolved.append(_CC[p])
        else:
            resolved.append(p)
    deduped = []
    seen_lower: set[str] = set()
    for part in resolved:
        key = part.lower()
        if key not in seen_lower:
            deduped.append(part)
            seen_lower.add(key)
    return ", ".join(deduped)


_CONTESTED_TERRITORY_RE = re.compile(r"\bgaza\b|\bwest\s+bank\b", re.IGNORECASE)
_COUNTRY_SUFFIX_RE = re.compile(
    r",?\s*(israel|palestine|palestinian\s+terri\w*)$", re.IGNORECASE
)
# Metadata/boilerplate patterns that appear in DB summary fields but have no
# narrative value (e.g. "(high-intensity conflict event) Reported by 6 sources.")
_METADATA_SUMMARY_RE = re.compile(
    r"^\s*[\(\[]?(high|medium|low)[- ]intensity|"
    r"^\s*reported by \d+\s+sources?\.?\s*$|"
    r"^\s*\(?(high|medium|low)[- ]intensity\s+(conflict|event|incident)",
    re.IGNORECASE,
)


def _normalize_contested_territory(
    location: str, admin1: str | None, country: str
) -> tuple[str, str | None, str]:
    """Replace politically charged country labels with neutral geographic territory names.

    ACLED and GDELT conventionally record events in the Gaza Strip and West Bank
    under country='Israel'. Displaying 'Gaza, Israel' attributes sovereignty in a
    way that is contested and inappropriate for a nonpartisan platform.  We use
    the internationally recognized geographic territory names instead.

    Returns (location, admin1, country) with normalized values.
    """
    loc = (location or "").strip()
    adm = (admin1 or "").strip()

    # Gaza Strip — any location/admin1 that contains "Gaza"
    if re.search(r"\bgaza\b", loc, re.IGNORECASE) or re.search(
        r"\bgaza\b", adm, re.IGNORECASE
    ):
        # Strip any appended country name (", Israel" / ", Palestine")
        clean_loc = _COUNTRY_SUFFIX_RE.sub("", loc).strip() or "Gaza Strip"
        return clean_loc, None, "Gaza Strip"

    # West Bank — location/admin1 contains "West Bank"
    if re.search(r"\bwest\s+bank\b", loc, re.IGNORECASE) or re.search(
        r"\bwest\s+bank\b", adm, re.IGNORECASE
    ):
        clean_loc = _COUNTRY_SUFFIX_RE.sub("", loc).strip() or "West Bank"
        return clean_loc, None, "West Bank"

    return location, admin1, country


def _is_precise_structured_point(
    *,
    location: str,
    country: str,
    admin1: str,
) -> bool:
    """Require at least sub-country granularity for map incident points.

    Country-only rows (e.g., "United States") are too vague and typically map to
    country centroids, which looks like a false precise location.
    """
    loc = " ".join(str(location or "").split()).strip()
    ctry = " ".join(str(country or "").split()).strip()
    adm = " ".join(str(admin1 or "").split()).strip()
    if adm and re.match(r"^[A-Z]{2}[A-Z0-9]{0,4}$", adm):
        adm = ""

    if not loc:
        return False

    loc_key = re.sub(r"[^a-z0-9]+", " ", loc.lower()).strip()
    ctry_key = re.sub(r"[^a-z0-9]+", " ", ctry.lower()).strip()
    if loc_key and ctry_key and loc_key == ctry_key:
        return False

    # Require either a hierarchical place string (comma-separated) or a non-empty
    # sub-country admin hint. This excludes pure country-only points.
    if "," in loc:
        return True
    if adm and adm.lower() != ctry.lower():
        return True
    return False


def _normalize_source_urls(values: list | tuple | None) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        url = str(raw or "").strip()
        if not url or not url.startswith("http"):
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _collect_semantic_cluster_source_urls(semantic_clusters: list[dict]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for cluster in semantic_clusters:
        for raw in cluster.get("events") or []:
            for url in _normalize_source_urls(raw.get("source_urls") or []):
                if url in seen:
                    continue
                seen.add(url)
                urls.append(url)
    return urls


def _get_articles_by_urls_batched(
    urls: list[str], *, chunk_size: int = 100
) -> dict[str, dict]:
    cleaned = [str(url).strip() for url in urls if str(url or "").strip()]
    if not cleaned:
        return {}

    # Keep each repo call below get_articles_by_urls hard cap (120).
    cap = max(1, min(int(chunk_size), 120))
    out: dict[str, dict] = {}
    for idx in range(0, len(cleaned), cap):
        chunk = cleaned[idx : idx + cap]
        out.update(get_articles_by_urls(chunk, limit=len(chunk)))
    return out


def _select_structured_headline_summary(
    source_urls: list[str],
    article_lookup_by_url: dict[str, dict],
) -> tuple[str, str]:
    fallback_title = ""
    fallback_summary = ""

    for url in source_urls:
        article = article_lookup_by_url.get(url)
        if not article:
            continue

        title = normalize_article_title(
            article.get("translated_title")
            or article.get("title")
            or article.get("original_title")
        )
        summary = normalize_article_description(
            article.get("translated_description")
            or article.get("description")
            or article.get("original_description"),
            title,
            limit=240,
        )
        if title and not fallback_title:
            fallback_title = title
        if summary and not fallback_summary:
            fallback_summary = summary
        if title:
            return title, (summary or fallback_summary)

    return fallback_title, fallback_summary


def _acled_hotspot_event_copy(event: dict) -> dict:
    """Human-readable summary/title for map tooltips when DB summary is empty."""
    raw_summary = (event.get("summary") or "").strip()
    et = (event.get("event_type") or "Incident").strip()
    sub_raw = (event.get("sub_event_type") or "").strip()
    # Ignore raw CAMEO numeric codes (e.g. "190", "172") and GDELT admin codes as sub-types
    sub = (
        sub_raw
        if sub_raw
        and not sub_raw.isdigit()
        and not re.match(r"^[A-Z]{2}[A-Z0-9]{0,4}$", sub_raw)
        else ""
    )
    loc = _dedup_location_string(
        re.sub(r"\s*\(general\)", "", (event.get("location") or "").strip())
    )
    admin1 = (event.get("admin1") or "").strip()
    # Skip raw GDELT admin/country codes (e.g. "IS00", "USCA", "IS", "UK", "AS")
    if admin1 and re.match(r"^[A-Z]{2}[A-Z0-9]{0,4}$", admin1):
        admin1 = ""
    country = (event.get("country") or "").strip()
    # Resolve 2-letter GDELT/FIPS country codes to full names
    if country and len(country) <= 2 and country.isupper():
        from ingestion.gdelt_gkg_ingestion import COUNTRY_CODE_TO_NAME

        country = COUNTRY_CODE_TO_NAME.get(country, country)
    a1 = (event.get("actor_primary") or "").strip()
    a2 = (event.get("actor_secondary") or "").strip()
    fatal = int(event.get("fatalities") or 0)
    event_date = (event.get("event_date") or "").strip()

    # Normalize contested territories before building any user-facing strings
    loc, admin1, country = _normalize_contested_territory(loc, admin1, country)
    # Suppress admin1 when it's already embedded in the location string to avoid
    # redundant display like "Beirut, Beyrouth, Lebanon" + "Beyrouth · Lebanon".
    if admin1 and loc and admin1.lower() in loc.lower():
        admin1 = None

    def _incident_action_label() -> str:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        event_code = str(payload.get("event_code") or "").strip()
        summary_signal = " ".join(str(raw_summary or "").split()).lower()
        signal = " ".join([et, sub, event_code, summary_signal]).lower()

        generic_labels = {
            "strategic developments",
            "battles",
            "protests",
            "explosions/remote violence",
            "violence against civilians",
            # Too vague to use as-is — fall through to keyword-based mapping
            "fight",
            "fighting",
            "violence",
            "conflict",
            "incident",
            "armed conflict",
            # Raw ACLED taxonomy labels — must not leak to UI as-is
            "arrest or detain",
            "reported development",
            "non-violent action",
            "mob violence",
            "looting/property destruction",
            "peaceful protest",
            "violent demonstration",
            "government regains territory",
            "non-state actor overtakes territory",
            "headquarters or base established",
            "change to group/activity",
        }

        # Prefer explicit sub-event semantics when available.
        sub_norm = " ".join(sub.split()).strip()
        if sub_norm:
            sub_key = sub_norm.lower()
            if sub_key not in generic_labels:
                return sub_norm

        if any(token in signal for token in ("air", "aerial", "missile", "drone", "strike", "bombing", "car bombing")):
            return "Strike"
        if any(token in signal for token in ("armed attack", "assault", "raid")):
            return "Attack"
        if (
            et.lower() == "battles"
            and (not sub or sub.lower() == et.lower())
            and not any(token in signal for token in ("fight", "assault", "attack", "raid", "guerrilla", "artillery", "air"))
        ):
            return "Conflict incident"
        if any(token in signal for token in ("battle", "fight", "guerrilla", "clash")):
            return "Armed clash"
        if any(token in signal for token in ("explosion", "remote violence")):
            return "Explosion"
        if any(token in signal for token in ("protest", "demonstrat", "hunger strike", "boycott")):
            return "Protest"
        if "riot" in signal:
            return "Riot"
        if any(token in signal for token in ("arrest", "detain", "kidnap", "abduct", "expel", "deport")):
            if "arrest" in signal or "detain" in signal:
                return "Arrest or detention"
            if "kidnap" in signal or "abduct" in signal:
                return "Abduction"
            return "Security operation"
        if any(token in signal for token in ("sanction", "embargo")):
            return "Sanctions action"
        if any(token in signal for token in ("threat", "coerc")):
            return "Threat"
        if "occupy" in signal:
            return "Territory occupation"
        if any(token in signal for token in ("violence against civilians", "mass violence", "mass killings", "ethnic cleansing")):
            return "Civilian violence"
        if any(token in signal for token in ("election", "ballot", "vote", "referendum")):
            return "Election-related development"
        if any(token in signal for token in ("sanction", "embargo", "tariff", "trade")):
            return "Sanctions or trade action"
        if any(token in signal for token in ("diplomati", "agreement", "deal", "treaty", "ceasefire", "truce", "negotiat", "peace talk")):
            return "Diplomatic development"
        if any(token in signal for token in ("military operation", "deployment", "mobiliz", "troop movement", "military buildup")):
            return "Military movement"
        if any(token in signal for token in ("political", "government", "parliament", "legislation", "policy")):
            return "Political development"

        # Last-resort: avoid exposing opaque taxonomy as the user-facing action.
        if et.lower() in ("strategic developments", "reported development") or sub.lower() in ("strategic developments", "reported development"):
            return "Strategic development"
        return sub or et or "Incident"

    # Strip CAMEO jargon and GDELT metadata annotations from summaries.
    # GDELT summaries have the form:
    #   "{type} reported in {place} on {date}. (high-intensity conflict event) Covered by N sources."
    # The annotations are embedded mid-string, so we strip them inline rather than
    # relying on start-anchored patterns.
    if raw_summary:
        raw_summary = re.sub(
            r"\s*\(CAMEO\s+\d+,?\s*root\s+\d+\)", "", raw_summary
        ).strip()
        raw_summary = re.sub(
            r"\s*\[?\d{3}\]?\s*$", "", raw_summary
        ).strip()  # trailing CAMEO codes
        raw_summary = re.sub(
            r"\s*\(general\)", "", raw_summary
        ).strip()  # GDELT qualifier
        # Strip GDELT Goldstein intensity annotations (appear anywhere in string)
        raw_summary = re.sub(
            r"[\s.]*\(?(high|medium|low)[- ]intensity\s+(conflict|escalatory|cooperative)\s+event\)?\.?",
            "",
            raw_summary,
            flags=re.IGNORECASE,
        ).strip()
        raw_summary = re.sub(
            r"[\s.]*\(?escalatory\s+event\)?\.?",
            "",
            raw_summary,
            flags=re.IGNORECASE,
        ).strip()
        raw_summary = re.sub(
            r"[\s.]*\(?cooperative\s+development\)?\.?",
            "",
            raw_summary,
            flags=re.IGNORECASE,
        ).strip()
        # Strip GDELT source/mention count suffixes
        raw_summary = re.sub(
            r"[\s.]*[Cc]overed by \d+\s+sources? with \d+\s+mentions?\.?",
            "",
            raw_summary,
            flags=re.IGNORECASE,
        ).strip()
        raw_summary = re.sub(
            r"[\s.]*[Rr]eported by \d+\s+sources?\.?",
            "",
            raw_summary,
            flags=re.IGNORECASE,
        ).strip()
        raw_summary = raw_summary.rstrip(",. ").strip()
        raw_summary = _dedup_location_string(raw_summary)  # deduplicate location parts

    # Discard any remaining start-anchored metadata boilerplate
    if raw_summary and _METADATA_SUMMARY_RE.search(raw_summary):
        raw_summary = ""

    # Detect pure GDELT template summaries ("{action} reported in {place} on {date}")
    # These have no factual narrative value — the whole string is generated metadata.
    _dataset = (event.get("dataset") or "").strip().lower()
    _is_gdelt_template = bool(
        raw_summary
        and (
            _dataset == "gdelt_gkg"
            or re.match(
                r"^[\w/ ]+\s+reported\s+in\s+[\w,.\s-]+\s+on\s+\d{4}-\d{2}-\d{2}\.?\s*$",
                raw_summary,
            )
        )
    )
    if _is_gdelt_template:
        raw_summary = ""

    # Check if summary has actual narrative content (not just a location string)
    _summary_has_narrative = (
        raw_summary
        and len(raw_summary) > 30
        and any(
            kw in raw_summary.lower()
            for kw in (
                "reported",
                "involving",
                "attack",
                "strike",
                "killed",
                "injured",
                "protest",
                "clash",
                "fighting",
                "bomb",
                "explosion",
                "arrest",
                "ceasefire",
                "sanction",
                "election",
                "diplomat",
                "military",
                "troops",
                "forces",
                "violence",
                "offensive",
                "defensive",
                "siege",
                "raid",
                "detain",
                "fatalities",
                "incident",
                "development",
                "escalat",
                "de-escalat",
                "tension",
            )
        )
    )
    if _summary_has_narrative:
        summary = raw_summary
    else:
        # Build a full narrative sentence instead of terse fragments
        action = _incident_action_label()

        # Place detail — avoid redundant or partial overlaps in location strings.
        # e.g. loc="Gaza", country="Gaza Strip" → "in Gaza Strip" (not "in Gaza, Gaza Strip")
        loc_l = (loc or "").lower()
        ctry_l = (country or "").lower()
        adm_l = (admin1 or "").lower()
        if loc:
            if not ctry_l or loc_l == ctry_l:
                place_detail = f"in {loc}"
            elif ctry_l in loc_l:
                # country already embedded in location string
                place_detail = f"in {loc}"
            elif loc_l in ctry_l:
                # location is a substring of territory/country — use territory
                place_detail = f"in {country}"
            else:
                place_detail = f"in {loc}, {country}"
        elif admin1 and adm_l != ctry_l:
            place_detail = f"in {admin1}, {country}" if country else f"in {admin1}"
        elif country:
            place_detail = f"in {country}"
        else:
            place_detail = "at an unspecified location"

        # Actor detail
        if a1 and a2:
            actor_detail = f" involving {a1} and {a2}"
        elif a1:
            actor_detail = f" involving {a1}"
        else:
            actor_detail = ""

        # Assemble narrative
        summary = f"{action} reported {place_detail}{actor_detail}"
        if event_date:
            summary += f" on {event_date}"
        summary += "."
        if fatal:
            summary += f" {fatal} fatalities reported."
        # Append raw_summary if it exists but was too short to use alone
        if raw_summary and raw_summary not in summary:
            summary += f" {raw_summary}"

    # Derive the map pin title from the most specific source available.
    # (1) If we have real narrative text (ACLED notes), extract the first sentence —
    #     this gives users a factual description of what happened rather than a
    #     generic category label like "Armed clash in Gaza Strip".
    # (2) Otherwise build a structured label from event metadata, always including
    #     actors when available for maximum specificity.
    if _summary_has_narrative:
        first_sentence = raw_summary.split(".")[0].strip()
        if len(first_sentence) >= 20:
            title = first_sentence[:220]
        else:
            # First sentence too short — use the fuller narrative, capped
            title = raw_summary[:160].rsplit(" ", 1)[0] if len(raw_summary) > 160 else raw_summary
    else:
        action_label = _incident_action_label()
        action_label = re.sub(r"^reported\s+", "", action_label, flags=re.IGNORECASE).strip()
        # Use only the most specific location segment (city) in the title.
        # Admin1 and country are shown in the panel's geography line already.
        loc_city = (loc.split(",")[0].strip() if "," in loc else loc) or admin1 or country or "Unknown"
        if country and loc_city and loc_city.lower() != country.lower() and country.lower() not in loc_city.lower():
            place_for_title = f"{loc_city}, {country}"
        else:
            place_for_title = loc_city or country or "Unknown"
        if a1 and a2:
            title = f"Reported {action_label.lower()} between {a1} and {a2} in {place_for_title}"
        elif a1:
            title = f"Reported {action_label.lower()} involving {a1} in {place_for_title}"
        else:
            title = f"Reported {action_label.lower()} in {place_for_title}"
        if fatal and "fatal" not in title.lower():
            title = f"{title} · {fatal} fatalities"
        title = title[0].upper() + title[1:] if title else title
    return {"summary": summary, "title": title[:220], "has_narrative": _summary_has_narrative}


def _map_headline_for_structured_cluster(cluster: dict, primary_country: str) -> str:
    """Headline for map dots: narrative first, not 'EventType in Country: City' (place tail)."""
    summary = " ".join(str(cluster.get("summary") or "").split()).strip()
    if summary:
        first = summary.split(".")[0].strip()
        if len(first) < 20 and summary.count(".") >= 1:
            chunks = summary.split(".")
            first = ".".join(chunks[:2]).strip()
            if chunks[2:]:
                first += "."
        if len(first) > 168:
            first = first[:165].rsplit(" ", 1)[0] + "..."
        return first or summary[:168]

    # Fallback: build a meaningful narrative headline from cluster metadata
    pet = (cluster.get("primary_event_type") or "").strip() or "Incident"
    sub = (cluster.get("primary_sub_event_type") or "").strip()
    base = sub if sub and sub.lower() != pet.lower() else pet
    actors = [
        str(a).strip() for a in (cluster.get("entity_focus") or []) if str(a).strip()
    ]
    locations = [
        str(loc).strip()
        for loc in (cluster.get("location_focus") or [])
        if str(loc).strip()
    ]
    event_count = int(cluster.get("structured_event_count") or 0)
    fatalities = int(cluster.get("fatality_total") or 0)

    # Build place detail
    if locations:
        place = (
            locations[0]
            if locations[0].lower() != primary_country.lower()
            else primary_country
        )
        if len(locations) >= 2 and locations[0].lower() != primary_country.lower():
            place = f"{locations[0]} and nearby areas"
    else:
        place = primary_country

    # Build the headline with as much context as possible
    parts = [base]
    if actors and len(actors) >= 2:
        parts.append(f"involving {actors[0]} and {actors[1]}")
    elif actors:
        parts.append(f"involving {actors[0]}")
    elif event_count > 1:
        parts.append(f"({event_count} incidents)")
    parts.append(f"in {place}")
    if fatalities:
        parts.append(f"— {fatalities} fatalities reported")

    headline = " ".join(parts)
    if len(headline) > 168:
        headline = headline[:165].rsplit(" ", 1)[0] + "..."
    return headline


def _development_aspect_from_structured_cluster(cluster: dict) -> str:
    """Bucket incident semantics into political | conflict | economic (structured events are rarely economic)."""
    et = (cluster.get("primary_event_type") or "").strip()
    conflict_types = {
        "Battles",
        "Violence against civilians",
        "Explosions/Remote violence",
    }
    political_types = {"Protests", "Riots", "Strategic developments"}
    if et in conflict_types:
        return "conflict"
    if et in political_types:
        return "political"
    el = et.lower()
    if any(
        token in el
        for token in (
            "battle",
            "violence",
            "explosion",
            "clash",
            "airstrike",
            "armed attack",
        )
    ):
        return "conflict"
    if any(
        token in el
        for token in (
            "protest",
            "riot",
            "strategic",
            "coup",
            "election",
            "sanction",
            "diplom",
        )
    ):
        return "political"
    return "political"


def _development_aspect_from_structured_event(event: dict) -> str:
    et = (event.get("event_type") or "").strip()
    sub = (event.get("sub_event_type") or "").strip()
    cluster_like = {
        "primary_event_type": et,
        "primary_sub_event_type": sub,
    }
    return _development_aspect_from_structured_cluster(cluster_like)


def _hotspot_recency_factor(
    event_dt: datetime | None, now: datetime, window_hours: int
) -> float:
    if event_dt is None:
        return 0.45
    age_hours = max(0.0, (now - event_dt).total_seconds() / 3600)
    return max(0.0, 1.0 - min(age_hours / max(window_hours, 1), 1.0))


def _incident_hotspots_from_semantic_clusters(
    semantic_clusters: list[dict],
    now: datetime,
    hours: int,
    cutoff: datetime,
    article_lookup_by_url: dict[str, dict],
) -> tuple[list[dict], int, int]:
    """Turn semantic structured clusters into geocoded map hotspots (one dot per incident)."""
    hotspots: list[dict] = []
    candidate_events = 0
    window_fatalities = 0
    seen_event_ids: set[str] = set()

    for cluster in semantic_clusters:
        for raw in cluster.get("events") or []:
            ev = dict(raw)
            event_id = str(ev.get("event_id") or "").strip()
            if event_id and event_id in seen_event_ids:
                continue

            event_dt = _event_datetime_for_hotspot(ev.get("event_date"))
            if event_dt is None or event_dt < cutoff:
                continue

            lat_v, lon_v = ev.get("latitude"), ev.get("longitude")
            if lat_v is None or lon_v is None:
                continue
            try:
                lat_f = float(lat_v)
                lon_f = float(lon_v)
            except (TypeError, ValueError):
                continue

            if event_id:
                seen_event_ids.add(event_id)

            candidate_events += 1
            fatality_total = int(ev.get("fatalities") or 0)
            window_fatalities += fatality_total

            source_urls = _normalize_source_urls(ev.get("source_urls") or [])
            resolved_title, resolved_summary = _select_structured_headline_summary(
                source_urls,
                article_lookup_by_url,
            )

            event_copy = _acled_hotspot_event_copy(ev)
            generated_title = (event_copy.get("title") or "").strip()
            generated_summary = (event_copy.get("summary") or "").strip()
            generated_has_narrative = event_copy.get("has_narrative", False)

            title = (
                resolved_title
                or generated_title
                or resolved_summary
                or generated_summary
                or "Incident"
            )
            summary = (
                resolved_summary
                or generated_summary
                or resolved_title
                or generated_title
                or "Incident"
            )
            label = title

            ev_country = " ".join(str(ev.get("country") or "").split()).strip()
            if ev_country and len(ev_country) == 2 and ev_country.isupper():
                from ingestion.gdelt_gkg_ingestion import COUNTRY_CODE_TO_NAME

                ev_country = COUNTRY_CODE_TO_NAME.get(ev_country, ev_country)

            ev_admin1 = " ".join(str(ev.get("admin1") or "").split()).strip()
            if ev_admin1 and re.match(r"^[A-Z]{2}[A-Z0-9]{0,4}$", ev_admin1):
                ev_admin1 = ""

            ev_location = _dedup_location_string(
                re.sub(
                    r"\s*\(general\)",
                    "",
                    " ".join(str(ev.get("location") or "").split()).strip(),
                )
            )
            if not ev_location:
                ev_location = ev_admin1 or ev_country or "Unknown location"

            # Run precision check on original values BEFORE territory normalization.
            # Normalization can make location == country (e.g. "Gaza Strip" / "Gaza Strip")
            # which the precision check would incorrectly reject as a country-centroid point.
            if not _is_precise_structured_point(
                location=ev_location,
                country=ev_country,
                admin1=ev_admin1,
            ):
                continue

            # Normalize contested territories for display (after precision check passes)
            ev_location, ev_admin1, ev_country = _normalize_contested_territory(
                ev_location, ev_admin1, ev_country
            )

            # Suppress admin1 when it's already embedded in the location string.
            if ev_admin1 and ev_location and ev_admin1.lower() in ev_location.lower():
                ev_admin1 = None

            _ev_dataset = (ev.get("dataset") or "").strip().lower()
            if _ev_dataset == "gdelt_gkg" and not resolved_title:
                # GDELT events with no linked article contain only a CAMEO code
                # and a location — there is no specific event to describe.
                # Without an article title there is nothing factual to show the user.
                continue

            if not resolved_title and not generated_has_narrative and fatality_total == 0:
                # ACLED (and other) events with no linked article and no real narrative
                # in the stored notes produce pure template titles like
                # "Reported arrest or detain in Indiana, United States" — nothing factual
                # to show the user. Keep only if there are fatalities (always significant).
                continue

            attention_score, _ = _hotspot_event_weight(ev, now, hours)
            aspect = _development_aspect_from_structured_event(ev)
            event_type = " ".join(str(ev.get("event_type") or "").split()).strip()
            sub_event_type = " ".join(str(ev.get("sub_event_type") or "").split()).strip()
            event_types = [
                value
                for value in (event_type, sub_event_type)
                if value and value != event_type
            ]
            if event_type:
                event_types.insert(0, event_type)

            sample_event = {
                "event_id": ev.get("event_id"),
                "event_date": ev.get("event_date"),
                "country": ev_country,
                "admin1": ev_admin1,
                "location": ev_location,
                "event_type": ev.get("event_type"),
                "sub_event_type": ev.get("sub_event_type"),
                "actor_primary": ev.get("actor_primary"),
                "actor_secondary": ev.get("actor_secondary"),
                "fatalities": fatality_total,
                "source_count": int(ev.get("source_count") or 0),
                "source_urls": source_urls,
                "summary": summary,
                "title": title,
            }

            material = f"evt|{event_id or label}|{lat_f:.5f}|{lon_f:.5f}|{ev.get('event_date') or ''}"
            hotspot_id = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

            hotspots.append(
                {
                    "hotspot_id": hotspot_id,
                    "label": label[:220],
                    "headline": label[:220],
                    "cluster_label": (cluster.get("label") or "").strip() or label[:220],
                    "country": ev_country or "Unknown country",
                    "admin1": ev_admin1 or None,
                    "location": ev_location,
                    "latitude": round(lat_f, 4),
                    "longitude": round(lon_f, 4),
                    "event_count": 1,
                    "fatality_total": fatality_total,
                    "source_count": int(ev.get("source_count") or 0),
                    "attention_score": round(float(attention_score), 2),
                    "attention_share": 0.0,
                    "intensity": 0.0,
                    "event_density": 0.0,
                    "fatality_density": 0.0,
                    "cloud_radius": 0.0,
                    "cloud_density": 0.0,
                    "latest_event_date": (
                        event_dt.isoformat().replace("+00:00", "Z") if event_dt else None
                    ),
                    "event_types": event_types[:4],
                    "aspect": aspect,
                    "sample_locations": [ev_location],
                    "story_region": _world_region_for_coordinates(lat_f, lon_f),
                    "sample_events": [sample_event],
                    "source_kind": "structured",
                    "topic": "geopolitics",
                }
            )

    max_weight = max((float(h["attention_score"]) for h in hotspots), default=0.0)
    max_events = max((int(h["event_count"]) for h in hotspots), default=1)
    max_fatalities = max((int(h["fatality_total"]) for h in hotspots), default=0)
    total_cluster_weight = sum(float(h["attention_score"]) for h in hotspots)

    for h in hotspots:
        intensity = (
            round((float(h["attention_score"]) / max_weight), 4) if max_weight else 0.0
        )
        event_density = round((int(h["event_count"]) / max(max_events, 1)), 4)
        fatality_density = (
            round((int(h["fatality_total"]) / max(max_fatalities, 1)), 4)
            if max_fatalities
            else 0.0
        )
        cloud_radius = round(34.0 + (intensity * 42.0) + (event_density * 10.0), 2)
        cloud_density = round(
            min(1.0, 0.3 + (intensity * 0.45) + (event_density * 0.35)), 3
        )
        share = (
            round((float(h["attention_score"]) / total_cluster_weight), 4)
            if total_cluster_weight
            else 0.0
        )
        h["intensity"] = intensity
        h["event_density"] = event_density
        h["fatality_density"] = fatality_density
        h["cloud_radius"] = cloud_radius
        h["cloud_density"] = cloud_density
        h["attention_share"] = share

    hotspots.sort(
        key=lambda item: (
            float(item.get("attention_score") or 0.0),
            int(item.get("fatality_total") or 0),
            int(item.get("event_count") or 0),
        ),
        reverse=True,
    )
    hotspots = hotspots[:160]

    return hotspots, candidate_events, window_fatalities


def _pick_materialized_rows_for_map(
    rows: list[dict],
    *,
    target_hours: int,
    cutoff: datetime,
) -> list[dict]:
    """Deduplicate materialized clusters and keep rows relevant to the map window."""
    eligible: list[dict] = []
    for row in rows:
        latest = _parse_timestamp(row.get("latest_published_at") or "")
        latest_dt = latest.astimezone(timezone.utc) if latest is not None else None
        if latest_dt is None or latest_dt < cutoff:
            continue
        eligible.append(row)

    best: dict[str, tuple[int, dict]] = {}
    for row in eligible:
        key = str(row.get("cluster_key") or "")
        if not key:
            continue
        wh = int(row.get("window_hours") or 0) or target_hours
        dist = abs(wh - target_hours)
        prev = best.get(key)
        if (
            prev is None
            or dist < prev[0]
            or (
                dist == prev[0]
                and (row.get("computed_at") or 0) > (prev[1].get("computed_at") or 0)
            )
        ):
            best[key] = (dist, row)
    return [pair[1] for pair in best.values()]


def _story_latest_datetime(story: dict) -> datetime | None:
    latest = _parse_timestamp(story.get("latest_update") or "")
    if latest is not None:
        return latest.astimezone(timezone.utc)
    source_dates = [
        _parse_timestamp(article.get("published_at") or "")
        for article in (story.get("sources") or [])
        if article.get("published_at")
    ]
    source_dates = [
        value.astimezone(timezone.utc) for value in source_dates if value is not None
    ]
    return max(source_dates) if source_dates else None


def _story_hotspot_type(story: dict) -> str:
    topic = (story.get("topic") or "").strip().lower()
    text = " ".join(
        str(value or "")
        for value in (
            story.get("headline"),
            story.get("label"),
            story.get("title"),
            story.get("summary"),
            story.get("description"),
        )
    ).lower()
    event_types = " ".join(
        str(value or "") for value in (story.get("event_types") or [])
    ).lower()
    if any(token in text or token in event_types for token in CONFLICT_TEXT_PATTERNS):
        return "conflict"
    if topic == "geopolitics":
        return "political"
    if topic == "economics":
        return "economic"
    if any(token in text for token in POLITICAL_TEXT_PATTERNS):
        return "political"
    if any(token in text for token in ECONOMIC_TEXT_PATTERNS):
        return "economic"
    return "story"


def _strip_story_dateline(text: str) -> str:
    clean = str(text or "").strip()
    previous = None
    while clean and clean != previous:
        previous = clean
        clean = DATELINE_RE.sub("", clean).strip()
    return clean


def _normalize_place_key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def _register_place_candidate(
    index: dict[str, dict], label: str | None, payload: dict
) -> None:
    key = _normalize_place_key(label)
    if not key:
        return
    current = index.get(key)
    if current is None or float(payload.get("weight") or 0.0) > float(
        current.get("weight") or 0.0
    ):
        index[key] = payload


def _build_story_location_index(days: int) -> dict[str, dict]:
    scope_days = max(14, min(days, 60))
    cached = _STORY_LOCATION_INDEX_CACHE.get(scope_days)
    now_ts = time.time()
    if cached and (now_ts - cached[0]) < MAP_CACHE_TTL_SECONDS:
        return cached[1]

    index: dict[str, dict] = {}
    structured = get_recent_structured_events(
        days=scope_days, limit=max(1800, scope_days * 40)
    )
    grouped: dict[str, dict] = {}
    for event in structured:
        latitude = event.get("latitude")
        longitude = event.get("longitude")
        if latitude is None or longitude is None:
            continue
        try:
            lat = float(latitude)
            lon = float(longitude)
        except (TypeError, ValueError):
            continue
        location = " ".join(str(event.get("location") or "").split()).strip()
        country = " ".join(str(event.get("country") or "").split()).strip()
        admin1 = " ".join(str(event.get("admin1") or "").split()).strip() or None
        if not location and not country:
            continue
        canonical_key = _normalize_place_key(location or country)
        record = grouped.setdefault(
            canonical_key,
            {
                "label": location or country,
                "country": country or location or "Unknown",
                "admin1": admin1,
                "latitude_sum": 0.0,
                "longitude_sum": 0.0,
                "weight": 0.0,
            },
        )
        weight = (
            1.0
            + min(int(event.get("source_count") or 0), 8) * 0.2
            + min(int(event.get("fatalities") or 0), 25) * 0.05
        )
        record["latitude_sum"] += lat * weight
        record["longitude_sum"] += lon * weight
        record["weight"] += weight

    for record in grouped.values():
        weight = max(float(record.get("weight") or 0.0), 1e-9)
        payload = {
            "label": record["label"],
            "country": record["country"],
            "admin1": record["admin1"],
            "latitude": record["latitude_sum"] / weight,
            "longitude": record["longitude_sum"] / weight,
            "weight": weight,
        }
        _register_place_candidate(index, record["label"], payload)
        _register_place_candidate(index, record["country"], payload)
        _register_place_candidate(index, record["admin1"], payload)

    for key, payload in COUNTRY_CENTROIDS.items():
        merged = {**payload, "weight": 0.5}
        _register_place_candidate(index, key, merged)
        _register_place_candidate(index, payload.get("label"), merged)
        _register_place_candidate(index, payload.get("country"), merged)

    _STORY_LOCATION_INDEX_CACHE[scope_days] = (now_ts, index)
    return index


def _story_article_text(article: dict) -> str:
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
    clean_title = _strip_story_dateline(title)
    clean_description = _strip_story_dateline(str(description)[:320])
    return f"{clean_title}. {clean_description}".strip()


def _resolve_story_place(
    entity_name: str, article_text: str, location_index: dict[str, dict]
) -> dict | None:
    key = _normalize_place_key(entity_name)
    if not key:
        return None
    text = article_text.lower()
    if key == "washington":
        if any(
            token in text
            for token in (
                "washington dc",
                "white house",
                "state department",
                "pentagon",
                "capitol hill",
                "federal government",
            )
        ):
            return {
                "label": "Washington, DC",
                "country": "United States",
                "admin1": "District of Columbia",
                "latitude": 38.9072,
                "longitude": -77.0369,
                "weight": 3.0,
            }
        if any(
            token in text
            for token in ("washington state", "seattle", "spokane", "olympia")
        ):
            return {
                "label": "Washington State",
                "country": "United States",
                "admin1": "Washington",
                "latitude": 47.7511,
                "longitude": -120.7401,
                "weight": 1.6,
            }
        return None
    return location_index.get(key)


def _story_article_language(article: dict) -> str | None:
    return article.get("translation_source_language") or article.get("language")


def _build_story_hotspots(window: str, now: datetime) -> tuple[list[dict], int]:
    """Story-layer dots from materialized clusters, geocoded via linked structured IDs (not 180 km article merge)."""
    normalized_window = (window or "24h").strip().lower()
    hours = _attention_window_hours(normalized_window)
    days = _window_days(normalized_window)
    cutoff = now - timedelta(hours=hours)
    location_index = _build_story_location_index(days)

    raw_rows = load_materialized_story_clusters(limit=220)
    picked = _pick_materialized_rows_for_map(
        raw_rows, target_hours=hours, cutoff=cutoff
    )
    picked.sort(
        key=lambda row: (
            row.get("latest_published_at") or "",
            row.get("computed_at") or 0,
        ),
        reverse=True,
    )
    picked = picked[:26]

    story_candidates = 0
    hotspots: list[dict] = []
    for index, row in enumerate(picked, 1):
        urls = [
            str(u).strip()
            for u in (row.get("article_urls") or [])
            if u and str(u).strip()
        ]
        linked = [
            str(x).strip()
            for x in (row.get("linked_structured_event_ids") or [])
            if x and str(x).strip()
        ]
        if not linked:
            continue
        story_candidates += max(len(urls), len(linked), 1)

        articles_map = get_articles_by_urls(urls, limit=48)
        coord_by_id = get_structured_event_coordinates_by_ids(linked)

        lat_sum = 0.0
        lon_sum = 0.0
        n_geo = 0
        location_counts: dict[str, int] = defaultdict(int)
        country_counts: dict[str, int] = defaultdict(int)
        admin1_counts: dict[str, int] = defaultdict(int)
        for meta in coord_by_id.values():
            lat_v, lon_v = meta.get("latitude"), meta.get("longitude")
            if lat_v is None or lon_v is None:
                continue
            try:
                lat_f = float(lat_v)
                lon_f = float(lon_v)
            except (TypeError, ValueError):
                continue
            lat_sum += lat_f
            lon_sum += lon_f
            n_geo += 1
            for label, bucket in (
                (meta.get("location"), location_counts),
                (meta.get("country"), country_counts),
                (meta.get("admin1"), admin1_counts),
            ):
                clean = " ".join(str(label or "").split()).strip()
                if clean:
                    bucket[clean] += 1

        centroid_lat: float | None = None
        centroid_lon: float | None = None
        if n_geo:
            centroid_lat = lat_sum / n_geo
            centroid_lon = lon_sum / n_geo

        primary_country = "Unknown country"
        primary_admin1 = None
        primary_location = ""

        if n_geo:
            primary_country = (
                max(country_counts.items(), key=lambda item: (item[1], item[0]))[0]
                if country_counts
                else primary_country
            )
            primary_location = (
                max(location_counts.items(), key=lambda item: (item[1], item[0]))[0]
                if location_counts
                else (
                    max(admin1_counts.items(), key=lambda item: (item[1], item[0]))[0]
                    if admin1_counts
                    else primary_country
                )
            )
            primary_admin1 = (
                max(admin1_counts.items(), key=lambda item: (item[1], item[0]))[0]
                if admin1_counts
                else None
            )
        else:
            place_payload = None
            # Cluster label and summary are the ground truth for what the story is about.
            # Use them to score location candidates — the location most mentioned in
            # the cluster's own text is more likely to be the subject than a country
            # that just happens to be mentioned in one article's lede.
            cluster_label_lower = (
                row.get("label") or row.get("summary") or ""
            ).lower()

            # Entity blacklist: these are actors/context, not event locations
            actor_keywords = {
                "us", "united states", "usa", "america", "american",
                "nato", "europe", "european", "washington", "state department",
                "g7", "g20", "un", "united nations", "eu", "european union",
            }
            country_actors = {"united states", "usa", "america"}

            # Score each candidate location by: frequency across articles + cluster label match
            place_scores: dict[str, dict] = {}  # entity_lower → best resolved place + score

            for url in urls[:12]:
                article = articles_map.get(url)
                if not article:
                    continue
                text = _story_article_text(article)
                if not text.strip():
                    continue
                try:
                    entities = extract_entities(
                        text, language=_story_article_language(article)
                    )
                except Exception:
                    continue

                article_headline = (
                    text.split(".")[0] if "." in text else text[:100]
                ).lower()

                for entity in entities:
                    if entity.get("type") != "GPE":
                        continue
                    entity_name = str(entity.get("entity") or "")
                    entity_lower = entity_name.lower()

                    place = _resolve_story_place(entity_name, text, location_index)
                    if not place:
                        continue

                    resolved_country = str(place.get("country", "")).lower()
                    is_actor = (
                        entity_lower in actor_keywords
                        or resolved_country in country_actors
                    )
                    if is_actor:
                        continue

                    # Score: +2 if in cluster label, +1 if in this article headline, +1 per article mention
                    in_cluster_label = entity_lower in cluster_label_lower or resolved_country in cluster_label_lower
                    in_article_headline = entity_lower in article_headline or resolved_country in article_headline

                    entry = place_scores.setdefault(entity_lower, {
                        "place": place,
                        "score": 0,
                        "in_cluster_label": in_cluster_label,
                    })
                    entry["score"] += 2 if in_cluster_label else (1 if in_article_headline else 0.5)
                    entry["in_cluster_label"] = entry["in_cluster_label"] or in_cluster_label

            # Pick the highest-scoring candidate. Cluster-label matches rank above all others.
            best_key = None
            best_score = -1.0
            for key, entry in place_scores.items():
                # Boost cluster-label matches so they always beat non-label mentions
                effective_score = entry["score"] + (100 if entry["in_cluster_label"] else 0)
                if effective_score > best_score:
                    best_score = effective_score
                    best_key = key
            if best_key:
                place_payload = place_scores[best_key]["place"]

            if not place_payload:
                continue
            centroid_lat = float(place_payload["latitude"])
            centroid_lon = float(place_payload["longitude"])
            primary_country = str(place_payload.get("country") or "Unknown country")
            primary_admin1 = place_payload.get("admin1")
            primary_location = str(place_payload.get("label") or primary_country)

        if centroid_lat is None or centroid_lon is None:
            continue

        raw_label = " ".join(str(row.get("label") or "").split()).strip()
        summary_text = " ".join(str(row.get("summary") or "").split()).strip()

        primary_topic = (row.get("topic") or "").strip().lower() or None
        headline = ""

        # Priority 1: first sentence of the cluster summary (if substantial)
        if summary_text:
            first_sent = summary_text.split(".")[0].strip()
            if len(first_sent) >= 28:
                if len(first_sent) > 168:
                    first_sent = first_sent[:165].rsplit(" ", 1)[0] + "..."
                headline = first_sent

        # Priority 2: cluster label (often derived from article titles during clustering)
        if not headline:
            headline = raw_label

        # Priority 3: best article title from the cluster's linked articles.
        # This is far more informative than a generic "Political development in X".
        if not headline or headline == primary_location or headline == primary_country:
            best_article_title = ""
            arts_by_recency = sorted(
                articles_map.values(),
                key=lambda a: (a.get("published_at") or ""),
                reverse=True,
            )
            for art in arts_by_recency:
                t = normalize_article_title(
                    art.get("translated_title") or art.get("title") or art.get("original_title")
                )
                # Skip placeholder/inferred titles and very short ones
                payload = art.get("payload") or {}
                is_placeholder = payload.get("inferred") or payload.get("origin") == "structured_event_source_url"
                if t and len(t) >= 20 and not is_placeholder:
                    best_article_title = t[:168]
                    break
            headline = best_article_title or headline

        # Last resort: only if we truly have nothing meaningful
        if not headline or headline == primary_location or headline == primary_country:
            place_label = primary_location or primary_country
            headline = f"Reported developments in {place_label}" if place_label else "Developing story"
        aspect = _story_hotspot_type(
            {
                "topic": primary_topic,
                "label": headline,
                "headline": headline,
                "title": headline,
                "summary": summary_text or row.get("summary"),
                "description": summary_text or row.get("summary"),
            }
        )
        if aspect == "story":
            aspect = "economic" if primary_topic == "economics" else "political"

        latest = _parse_timestamp(row.get("latest_published_at") or "")
        latest_dt = latest.astimezone(timezone.utc) if latest is not None else None
        recency = _hotspot_recency_factor(latest_dt, now, hours)
        base = 11.0 + min(len(urls), 20) * 1.05 + len(linked) * 0.4 + n_geo * 0.55
        attention_score = round(base * (0.3 + 0.7 * recency), 2)

        arts_sorted = sorted(
            articles_map.values(),
            key=lambda a: (a.get("published_at") or ""),
            reverse=True,
        )
        sample_events = []
        for art in arts_sorted[:4]:
            title = (art.get("title") or "").strip()[:220]
            body = (
                art.get("description")
                or art.get("translated_description")
                or title
                or ""
            ).strip()
            sample_events.append(
                {
                    "event_id": art.get("url"),
                    "event_date": art.get("published_at"),
                    "country": primary_country,
                    "admin1": primary_admin1,
                    "location": primary_location,
                    "event_type": aspect,
                    "fatalities": 0,
                    "source_count": 1,
                    "source_urls": [art.get("url")] if art.get("url") else [],
                    "title": title or None,
                    "summary": body or None,
                }
            )

        if not sample_events and urls:
            fallback_summary = (row.get("summary") or headline)[:280] or headline
            for url in urls[:4]:
                sample_events.append(
                    {
                        "event_id": url,
                        "event_date": row.get("latest_published_at"),
                        "country": primary_country,
                        "admin1": primary_admin1,
                        "location": primary_location,
                        "event_type": aspect,
                        "fatalities": 0,
                        "source_count": 1,
                        "source_urls": [url] if url else [],
                        "title": headline,
                        "summary": fallback_summary,
                    }
                )

        latest_out = latest_dt.isoformat().replace("+00:00", "Z") if latest_dt else None
        material = f"mstory|{row.get('cluster_key')}|{centroid_lat:.4f}|{centroid_lon:.4f}|{index}"
        hotspot_id = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

        hotspots.append(
            {
                "hotspot_id": hotspot_id,
                "label": headline,
                "headline": headline,
                "country": primary_country,
                "admin1": primary_admin1,
                "location": primary_location,
                "latitude": round(centroid_lat, 4),
                "longitude": round(centroid_lon, 4),
                "event_count": max(len(urls), 1),
                "fatality_total": 0,
                "source_count": max(
                    len({(a.get("source") or "").strip().lower() for a in arts_sorted}),
                    1,
                ),
                "attention_score": attention_score,
                "attention_share": 0.0,
                "intensity": 0.0,
                "event_density": 0.0,
                "fatality_density": 0.0,
                "cloud_radius": 0.0,
                "cloud_density": 0.0,
                "latest_event_date": latest_out,
                "event_types": [aspect],
                "aspect": aspect,
                "sample_locations": [
                    name
                    for name, _ in sorted(
                        location_counts.items(), key=lambda item: (-item[1], item[0])
                    )[:6]
                ],
                "story_region": _world_region_for_coordinates(
                    centroid_lat, centroid_lon
                ),
                "sample_events": sample_events,
                "source_kind": "story",
                "topic": primary_topic,
            }
        )

    max_weight = max((float(h["attention_score"]) for h in hotspots), default=0.0)
    max_events = max((int(h["event_count"]) for h in hotspots), default=0)
    total_cluster_weight = sum(float(h["attention_score"]) for h in hotspots) or 1.0

    for h in hotspots:
        intensity = (
            round((float(h["attention_score"]) / (max_weight + 6.0)), 4)
            if max_weight
            else 0.0
        )
        event_density = round((int(h["event_count"]) / max(max_events, 1)), 4)
        coverage_density = round(min(1.0, int(h["source_count"]) / 6.0), 4)
        share = round(
            (
                float(h["attention_score"])
                / (total_cluster_weight + max(len(hotspots), 1) * 4.0)
            ),
            4,
        )
        cloud_radius = round(
            20.0
            + (intensity * 24.0)
            + (event_density * 18.0)
            + (coverage_density * 14.0),
            2,
        )
        cloud_density = round(
            min(
                0.92,
                0.22
                + (intensity * 0.36)
                + (event_density * 0.18)
                + (coverage_density * 0.16),
            ),
            3,
        )
        h["intensity"] = intensity
        h["event_density"] = event_density
        h["fatality_density"] = 0.0
        h["cloud_radius"] = cloud_radius
        h["cloud_density"] = cloud_density
        h["attention_share"] = share

    return hotspots, story_candidates


def _build_hotspot_attention_map(window: str = "24h", start: str | None = None, end: str | None = None, days: int | None = None) -> dict:
    # support explicit custom timeframes via start/end ISO dates or numeric days
    normalized_window = (window or "24h").strip().lower()
    custom = bool(start or end or (days is not None))

    # build a cache key that includes custom params when provided
    cache_key = f"{_MAP_HOTSPOT_CACHE_SCHEMA_VERSION}|{normalized_window}"
    if custom:
        cache_key = (
            f"{_MAP_HOTSPOT_CACHE_SCHEMA_VERSION}|custom|window={normalized_window}|"
            f"start={start or ''}|end={end or ''}|days={days or ''}"
        )

    cached = _MAP_ATTENTION_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and (now_ts - cached[0]) < MAP_CACHE_TTL_SECONDS:
        cached_payload = cached[1] if isinstance(cached[1], dict) else {}
        cached_hotspots = int(cached_payload.get("hotspot_count") or 0)
        cached_events = int(cached_payload.get("total_events") or 0)
        # Avoid returning a stale empty map payload for the full cache TTL.
        # This lets the map recover quickly after structured ingestion runs.
        if cached_hotspots > 0 or cached_events > 0:
            return cached_payload
        if (now_ts - cached[0]) < min(30.0, float(MAP_CACHE_TTL_SECONDS)):
            return cached_payload
        _MAP_ATTENTION_CACHE.pop(cache_key, None)

    # compute hours/days/cutoff based on either preset window or provided custom params
    if custom:
        # explicit days provided
        if days is not None and int(days) > 0:
            hours = int(days) * 24
            computed_days = max(1, math.ceil(hours / 24))
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(hours=hours)
        else:
            # parse start/end datetimes (accept ISO date or datetime strings)
            start_dt = _event_datetime_for_hotspot(start) if start else None
            end_dt = _event_datetime_for_hotspot(end) if end else None
            # if only end provided, treat end as end of that day; if only start provided, use now as end
            if end_dt and not start_dt:
                # use 24 hours ending at end_dt by default
                start_dt = end_dt - timedelta(days=1)
            if start_dt and not end_dt:
                end_dt = datetime.now(timezone.utc)

            if not start_dt or not end_dt:
                # fallback to default 24h window
                hours = _attention_window_hours(normalized_window)
                computed_days = _window_days(normalized_window)
                now = datetime.now(timezone.utc)
                cutoff = now - timedelta(hours=hours)
            else:
                # ensure end is inclusive of the selected end date (treat end as end of day if date-only)
                # if supplied strings were date-only, _event_datetime_for_hotspot produces midnight; extend to end of day
                # add one day to end to include the day's coverage
                if isinstance(end, str) and 'T' not in end:
                    end_dt = end_dt + timedelta(days=1)
                if isinstance(start, str) and 'T' not in start:
                    # start remains at midnight of start
                    pass

                now = end_dt
                cutoff = start_dt
                delta_hours = max(0.0, (now - cutoff).total_seconds() / 3600)
                hours = max(1, int(math.ceil(delta_hours)))
                computed_days = max(1, math.ceil(hours / 24))
    else:
        hours = _attention_window_hours(normalized_window)
        computed_days = _window_days(normalized_window)
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=hours)

    # limit structured days for heavy semantic workloads
    structured_days = min(computed_days, MAX_MAP_STRUCTURED_DAYS)
    semantic_clusters = build_map_structured_story_clusters(
        structured_days=structured_days,
        limit=40,
        dataset=None,
    )
    structured_source_urls = _collect_semantic_cluster_source_urls(semantic_clusters)
    # Upsert minimal article records from structured event URLs to enable headline enrichment
    if structured_source_urls:
        from db.articles_repo import upsert_minimal_articles_from_urls
        upsert_minimal_articles_from_urls(structured_source_urls)
    structured_article_lookup = _get_articles_by_urls_batched(structured_source_urls)
    hotspots, structured_candidate_count, window_fatalities = (
        _incident_hotspots_from_semantic_clusters(
            semantic_clusters,
            now,
            hours,
            cutoff,
            structured_article_lookup,
        )
    )

    story_hotspots, total_story_candidates = _build_story_hotspots(
        normalized_window, now
    )
    combined = hotspots + story_hotspots
    combined.sort(
        key=lambda item: (
            float(item.get("attention_score") or 0.0),
            int(item.get("source_count") or 0),
            int(item.get("event_count") or 0),
        ),
        reverse=True,
    )
    if hours <= 24:
        combined_limit = 180
    elif hours <= (24 * 7):
        combined_limit = 220
    elif hours <= (24 * 30):
        combined_limit = 260
    else:
        combined_limit = 300
    combined = combined[:combined_limit]
    total_attention = (
        sum(float(item.get("attention_score") or 0.0) for item in combined) or 1.0
    )
    max_attention = max(
        (float(item.get("attention_score") or 0.0) for item in combined), default=1.0
    )
    for item in combined:
        item["attention_share"] = round(
            float(item.get("attention_score") or 0.0) / total_attention, 4
        )
        if item.get("source_kind") == "story":
            item["intensity"] = (
                round(float(item.get("attention_score") or 0.0) / max_attention, 4)
                if max_attention
                else 0.0
            )

    payload = {
        "window": "custom" if custom else normalized_window,
        "hours": hours,
        "days": computed_days,
        "start": cutoff.isoformat().replace("+00:00", "Z") if custom else None,
        "end": now.isoformat().replace("+00:00", "Z") if custom else None,
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "total_events": structured_candidate_count + total_story_candidates,
        "hotspot_count": len(combined),
        "total_fatalities": int(window_fatalities),
        "available_windows": list(ATTENTION_WINDOW_HOURS.keys()),
        "hotspots": combined,
    }
    _MAP_ATTENTION_CACHE[cache_key] = (now_ts, payload)
    return payload


# ---------------------------------------------------------------------------
# Public API (called by route handlers)
# ---------------------------------------------------------------------------


def get_region_attention_payload(window: str = "24h") -> dict:
    return _build_region_attention_map(window=window)


def get_hotspot_attention_map_payload(
    window: str = "24h",
    start: str | None = None,
    end: str | None = None,
    days: int | None = None,
) -> dict:
    return _build_hotspot_attention_map(
        window=window,
        start=start,
        end=end,
        days=days,
    )
