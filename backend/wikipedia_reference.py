import re
from urllib.parse import quote

import requests

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_PAGE_BASE = "https://en.wikipedia.org/wiki/"


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "OthelloV2/1.0 (+entity reference; read-only)",
            "Accept": "application/json,*/*",
        }
    )
    return session


def _normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _normalize_match(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _pick_candidate(entity: str, candidates: list[dict]) -> dict | None:
    if not candidates:
        return None
    target = _normalize_match(entity)
    ranked = []
    for candidate in candidates:
        title = candidate.get("title") or ""
        normalized_title = _normalize_match(title)
        score = 0
        if normalized_title == target:
            score += 100
        elif target and target in normalized_title:
            score += 60
        elif normalized_title and normalized_title in target:
            score += 40
        score -= int(candidate.get("index", 0) or 0)
        ranked.append((score, candidate))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked[0][1]


def _search_titles(entity: str, limit: int = 5) -> list[dict]:
    session = _session()
    response = session.get(
        WIKIPEDIA_API,
        params={
            "action": "query",
            "list": "search",
            "srsearch": entity,
            "srlimit": limit,
            "srprop": "",
            "format": "json",
            "formatversion": 2,
            "utf8": 1,
        },
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("query", {}).get("search", [])


def _fetch_page(title: str) -> dict | None:
    session = _session()
    response = session.get(
        WIKIPEDIA_API,
        params={
            "action": "query",
            "prop": "extracts|pageimages|info|pageprops",
            "titles": title,
            "redirects": 1,
            "inprop": "url",
            "exintro": 1,
            "explaintext": 1,
            "piprop": "thumbnail",
            "pithumbsize": 360,
            "format": "json",
            "formatversion": 2,
            "utf8": 1,
        },
        timeout=12,
    )
    response.raise_for_status()
    pages = response.json().get("query", {}).get("pages", [])
    if not pages:
        return None
    page = pages[0]
    if page.get("missing"):
        return None
    return page


def fetch_wikipedia_reference(entity: str) -> dict:
    entity = _normalize_text(entity)
    if not entity:
        return {
            "entity": entity,
            "provider": "wikipedia",
            "status": "empty",
            "summary": None,
            "url": None,
            "thumbnail_url": None,
            "reference_only": True,
            "note": "Wikipedia background only. Not used in Othello analysis, scoring, or contradiction detection.",
        }

    candidates = _search_titles(entity)
    selected = _pick_candidate(entity, candidates)
    if not selected:
        return {
            "entity": entity,
            "provider": "wikipedia",
            "status": "empty",
            "title": entity,
            "summary": None,
            "url": None,
            "thumbnail_url": None,
            "reference_only": True,
            "note": "Wikipedia background only. Not used in Othello analysis, scoring, or contradiction detection.",
        }

    page = _fetch_page(selected.get("title") or entity)
    if not page:
        return {
            "entity": entity,
            "provider": "wikipedia",
            "status": "empty",
            "title": selected.get("title") or entity,
            "summary": None,
            "url": f"{WIKIPEDIA_PAGE_BASE}{quote((selected.get('title') or entity).replace(' ', '_'))}",
            "thumbnail_url": None,
            "reference_only": True,
            "note": "Wikipedia background only. Not used in Othello analysis, scoring, or contradiction detection.",
        }

    title = page.get("title") or selected.get("title") or entity
    summary = _normalize_text(page.get("extract"))
    fullurl = (
        page.get("fullurl") or f"{WIKIPEDIA_PAGE_BASE}{quote(title.replace(' ', '_'))}"
    )
    thumbnail = (page.get("thumbnail") or {}).get("source")
    return {
        "entity": entity,
        "provider": "wikipedia",
        "status": "ok" if summary or fullurl else "empty",
        "title": title,
        "summary": summary or None,
        "url": fullurl,
        "thumbnail_url": thumbnail,
        "page_id": page.get("pageid"),
        "language": "en",
        "match_title": selected.get("title"),
        "reference_only": True,
        "note": "Wikipedia background only. Not used in Othello analysis, scoring, or contradiction detection.",
    }
